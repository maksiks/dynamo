import os
import socket
import logging
import time

from common.interface.history import TransactionHistoryInterface
from common.interface.mysql import MySQL
from common.dataformat import HistoryRecord
import common.configuration as config

logger = logging.getLogger(__name__)

class MySQLHistory(TransactionHistoryInterface):
    """
    Transaction history interface implementation using MySQL as the backend.
    """

    def __init__(self):
        super(self.__class__, self).__init__()

        self._mysql = MySQL(**config.mysqlhistory.db_params)

    def _do_acquire_lock(self): #override
        while True:
            # Use the system table to "software-lock" the database
            self._mysql.query('LOCK TABLES `lock` WRITE')
            self._mysql.query('UPDATE `lock` SET `lock_host` = %s, `lock_process` = %s WHERE `lock_host` LIKE \'\' AND `lock_process` = 0', socket.gethostname(), os.getpid())

            # Did the update go through?
            host, pid = self._mysql.query('SELECT `lock_host`, `lock_process` FROM `lock`')[0]
            self._mysql.query('UNLOCK TABLES')

            if host == socket.gethostname() and pid == os.getpid():
                # The database is locked.
                break

            logger.warning('Failed to lock database. Waiting 30 seconds..')

            time.sleep(30)

    def _do_release_lock(self): #override
        self._mysql.query('LOCK TABLES `lock` WRITE')
        self._mysql.query('UPDATE `lock` SET `lock_host` = \'\', `lock_process` = 0 WHERE `lock_host` LIKE %s AND `lock_process` = %s', socket.gethostname(), os.getpid())

        # Did the update go through?
        host, pid = self._mysql.query('SELECT `lock_host`, `lock_process` FROM `lock`')[0]
        self._mysql.query('UNLOCK TABLES')

        if host != '' or pid != 0:
            raise LocalStoreInterface.LockError('Failed to release lock from ' + socket.gethostname() + ':' + str(os.getpid()))

    def _do_new_run(self, operation, partition, is_test): #override
        part_ids = self._mysql.query('SELECT `id` FROM `partitions` WHERE `name` LIKE %s', partition)
        if len(part_ids) == 0:
            part_id = self._mysql.query('INSERT INTO `partitions` (`name`) VALUES (%s)', partition)
        else:
            part_id = part_ids[0]

        if operation == HistoryRecord.OP_COPY:
            if is_test:
                operation_str = 'copy_test'
            else:
                operation_str = 'copy'
        else:
            if is_test:
                operation_str = 'deletion_test'
            else:
                operation_str = 'deletion'

        return self._mysql.query('INSERT INTO `runs` (`operation`, `partition_id`, `time_start`) VALUES (%s, %s, FROM_UNIXTIME(%s))', operation_str, part_id, time.time())

    def _do_make_copy_entry(self, site, operation_id, approved, do_list, size): #override
        """
        Arguments:
          do_list: [(dataset, origin)]
        1. Make sure the destination and origin sites are all in the database.
        2. Insert dataset names that were copied.
        3. Make an entry in the main history table.
        """

        self._mysql.insert_many('sites', ('name',), lambda (d, o): (o.name,), do_list)
        self._mysql.insert_many('sites', ('name',), lambda s: (s.name,), [site])

        site_ids = dict(self._mysql.query('SELECT `name`, `id` FROM `sites`'))

        dataset_names = list(set(d.name for d, o in do_list))
        
        self._mysql.insert_many('datasets', ('name',), lambda name: (name,), dataset_names)
        dataset_ids = dict(self._mysql.query('SELECT `name`, `id` FROM `datasets`'))

        self._mysql.query('INSERT INTO `copy_requests` (`id`, `timestamp`, `approved`, `site_id`, `size`) VALUES (%s, NOW(), %s, %s, %s)', operation_id, approved, site_ids[site.name], size)

        self._mysql.insert_many('copied_replicas', ('copy_id', 'dataset_id', 'origin_site_id'), lambda (d, o): (operation_id, dataset_ids[d.name], site_ids[o.name]), do_list)

    def _do_make_deletion_entry(self, run_number, site, operation_id, approved, datasets, size): #override
        """
        site and dataset are expected to be already in the database (save_deletion_decisions should be called first).
        """

        site_id = self._mysql.query('SELECT `id` FROM `sites` WHERE `name` LIKE %s', site.name)[0]

        dataset_ids = self._mysql.select_many('datasets', ('id',), ('name',), [d.name for d in datasets])

        self._mysql.query('INSERT INTO `deletion_requests` (`id`, `run_id`, `timestamp`, `approved`, `site`, `size`) VALUES (%s, %s, NOW(), %s, %s, %s)', deletion_id, run_number, approved, site_id, size)

        self._mysql.insert_many('deleted_replicas', ('deletion_id', 'dataset_id'), lambda did: (deletion_id, did), dataset_ids)

    def _do_update_copy_entry(self, copy_record): #override
        self._mysql.query('UPDATE `copy_requests` SET `approved` = %s, `size_copied` = %s, `last_update` = FROM_UNIXTIME(%s) WHERE `id` = %s', copy_record.approved, copy_record.done, copy_record.last_update, copy_record.operation_id)
        
    def _do_update_deletion_entry(self, deletion_record): #override
        self._mysql.query('UPDATE `deletion_requests` SET `approved` = %s, `size_deleted` = %s, `last_update` = FROM_UNIXTIME(%s) WHERE `id` = %s', deletion_record.approved, deletion_record.done, deletion_record.last_update, deletion_record.operation_id)

    def _do_save_deletion_decisions(self, run_number, deletions, protections, inventory): #override
        site_id_map = dict(self._mysql.query('SELECT `name`, `id` FROM `sites`'))
        sites_to_insert = []
        for site_name in inventory.sites.keys():
            if site_name not in site_id_map:
                sites_to_insert.append(site_name)

        if len(sites_to_insert) != 0:
            self._mysql.insert_many('sites', ('name',), lambda n: (n,), sites_to_insert)
            site_id_map = dict(self._mysql.query('SELECT `name`, `id` FROM `sites`'))

        sites_to_insert = None

        dataset_id_map = dict(self._mysql.query('SELECT `name`, `id` FROM `datasets`'))
        datasets_to_insert = []
        for dataset_name in inventory.datasets.keys():
            if dataset_name not in dataset_id_map:
                datasets_to_insert.append(dataset_name)

        if len(datasets_to_insert) != 0:
            self._mysql.insert_many('datasets', ('name',), lambda n: (n,), datasets_to_insert)
            dataset_id_map = dict(self._mysql.query('SELECT `name`, `id` FROM `datasets`'))

        datasets_to_insert = None

        indices_to_replicas = {}
        for dataset in inventory.datasets.values():
            dataset_id = dataset_id_map[dataset.name]
            for replica in dataset.replica:
                index = (site_id_map[replica.site.name], dataset_id)
                indices_to_replicas[index] = replica

        site_id_map = None
        dataset_id_map = None

        # replicas that are new or have sizes changed
        replicas_to_update = {}
        replicas_in_record = {}

        # find new replicas with no snapshots
        in_record = self._mysql.query('SELECT DISTINCT `site_id`, `dataset_id` FROM `replica_snapshots` ORDER BY `site_id`, `dataset_id`')
        in_memory = sorted(indices_to_replicas.keys())

        irec = 0
        imem = 0
        while irec != len(in_record) and imem != len(in_memory):
            recidx = in_record[irec]
            memidx = in_memory[imem]

            if recidx < memidx:
                # replica not in the current inventory
                irec += 1
            elif recidx > memidx:
                # new replica
                replicas_to_update[memidx] = indices_to_replicas[memidx]
                imem += 1
            else:
                replicas_in_record[memidx] = indices_to_replicas[memidx]
                irec += 1
                imem += 1

        while imem != len(in_memory):
            memidx = in_memory[imem]
            replicas_to_update[memidx] = indices_to_replicas[memidx]
            imem += 1

        # find the latest snapshots for all replicas in record
        snapshots = {} # replica -> (snapshot id, size)
        last_id = 0

        for snapshot_id, site_id, dataset_id, size in self._mysql.query('SELECT `id`, `site_id`, `dataset_id`, `size` FROM `replica_snapshots` ORDER BY `id` DESC'):
            if last_id == 0:
                last_id = snapshot_id

            try:
                replica = replicas_in_record[(site_id, dataset_id)]
            except KeyError:
                # this replica does not exist in the current inventory any more
                continue

            if replica not in snapshots:
                snapshots[replica] = (snapshot_id, size)

                if len(snapshots) == len(replicas_in_record):
                    # found latest snapshots for all existing replicas
                    break

        keeps = []

        # update replica snapshots for those with size changed
        for index, replica in replicas_in_record.items():
            if replica.size() != snapshots[replica][1]:
                replicas_to_update[index] = replica
            else:
                keeps.append(replica)

        replicas_in_record = None

        fields = ('site_id', 'dataset_id', 'size')
        mapping = lambda (index, replica): (index[0], index[1], replica.size())

        self._mysql.insert_many('replica_snapshots', fields, mapping, replicas_to_update.items())

        for snapshot_id, site_id, dataset_id, size in self._mysql.query('SELECT `id`, `site_id`, `dataset_id`, `size` FROM `replica_snapshots` WHERE `id` > %s ORDER BY `id` DESC', last_id):
            index = (site_id, dataset_id)
            replica = replicas_to_update[index]
            snapshots[replica] = (snapshot_id, size)

        replicas_to_update = None

        # save decisions
        PROTECT, DELETE, KEEP = range(1, 4)
        decision_entries = []

        for replica in protections:
            decision_entries.append((run_number, snapshots[replica][0], PROTECT))

        for replica in deletions:
            decision_entries.append((run_number, snapshots[replica][0], DELETE))

        for replica in keeps:
            decision_entries.append((run_number, snapshots[replica][0], KEEP))

        fields = ('run_id', 'snapshot_id', 'decision')
        mapping = lambda t: t

        self._mysql.insert_many('deletion_decisions', fields, mapping, decision_entries)

    def _do_get_incomplete_copies(self): #override
        history_entries = self._mysql.query('SELECT h.`id`, UNIX_TIMESTAMP(h.`timestamp`), h.`approved`, s.`name`, h.`size`, h.`size_copied`, UNIX_TIMESTAMP(h.`last_update`) FROM `copy_requests` AS h INNER JOIN `sites` AS s ON s.`id` = h.`site_id` WHERE h.`size` != h.`size_copied`')
        
        id_to_record = {}
        for eid, timestamp, approved, site_name, size, size_copied, last_update in history_entries:
            id_to_record[eid] = HistoryRecord(HistoryRecord.OP_COPY, eid, site_name, timestamp = timestamp, approved = approved, size = size, done = size_copied, last_update = last_update)

        id_to_dataset = dict(self._mysql.query('SELECT `id`, `name` FROM `datasets`'))
        id_to_site = dict(self._mysql.query('SELECT `id`, `name` FROM `sites`'))

        replicas = self._mysql.select_many('copied_replicas', ('copy_id', 'dataset_id', 'origin_site_id'), 'copy_id', ['%d' % i for i in id_to_record.keys()])

        current_copy_id = 0
        for copy_id, dataset_id, origin_site_id in replicas:
            if copy_id != current_copy_id:
                record = id_to_record[copy_id]
                current_copy_id = copy_id

            record.replicas.append(HistoryRecord.CopiedReplica(dataset_name = id_to_dataset[dataset_id], origin_site_name = id_to_site[origin_site_id]))

        return id_to_record.values()

    def _do_get_incomplete_deletions(self): #override
        history_entries = self._mysql.query('SELECT h.`id`, UNIX_TIMESTAMP(h.`timestamp`), h.`approved`, s.`name`, h.`size`, h.`size_deleted`, UNIX_TIMESTAMP(h.`last_update`) FROM `deletion_requests` AS h INNER JOIN `sites` AS s ON s.`id` = h.`site_id` WHERE h.`size` != h.`size_deleted`')
        
        id_to_record = {}
        for eid, timestamp, approved, site_name, size, size_deleted, last_update in history_entries:
            id_to_record[eid] = HistoryRecord(HistoryRecord.OP_DELETE, eid, site_name, timestamp = timestamp, approved = approved, size = size, done = size_deleted, last_update = last_update)

        id_to_dataset = dict(self._mysql.query('SELECT `id`, `name` FROM `datasets`'))
        id_to_site = dict(self._mysql.query('SELECT `id`, `name` FROM `sites`'))

        replicas = self._mysql.select_many('deleted_replicas', ('deletion_id', 'dataset_id'), 'deletion_id', ['%d' % i for i in id_to_record.keys()])

        current_deletion_id = 0
        for deletion_id, dataset_id in replicas:
            if deletion_id != current_deletion_id:
                record = id_to_record[deletion_id]
                current_deletion_id = deletion_id

            record.replicas.append(HistoryRecord.DeletedReplica(dataset_name = id_to_dataset[dataset_id]))

        return id_to_record.values()

    def _do_get_site_name(self, operation_id): #override
        result = self._mysql.query('SELECT s.name FROM `sites` AS s INNER JOIN `copy_requests` AS h ON h.`site_id` = s.`id` WHERE h.`id` = %s', operation_id)
        if len(result) != 0:
            return result[0]

        result = self._mysql.query('SELECT s.name FROM `sites` AS s INNER JOIN `deletion_requests` AS h ON h.`site_id` = s.`id` WHERE h.`id` = %s', operation_id)
        if len(result) != 0:
            return result[0]

        return ''
