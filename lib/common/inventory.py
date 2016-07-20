import logging
import fnmatch
import re

from common.interface.classes import default_interface
from common.interface.store import LocalStoreInterface
from common.interface.sitequota import SiteQuotaRetriever
from common.dataformat import IntegrityError, Dataset, DatasetReplica, BlockReplica
import common.configuration as config

logger = logging.getLogger(__name__)

class ConsistencyError(Exception):
    """Exception to be raised in case of data consistency problem."""
    
    pass


class InventoryManager(object):
    """Bookkeeping class to bridge the communication between remote and local data sources."""

    def __init__(self, load_data = True, store_cls = None, site_source_cls = None, dataset_source_cls = None, replica_source_cls = None):
        if store_cls:
            self.store = store_cls()
        else:
            self.store = default_interface['store']()

        if site_source_cls:
            self.site_source = site_source_cls()
        else:
            self.site_source = default_interface['site_source']()

        if dataset_source_cls:
            self.dataset_source = dataset_source_cls()
        else:
            self.dataset_source = default_interface['dataset_source']()

        if replica_source_cls:
            self.replica_source = replica_source_cls()
        else:
            self.replica_source = default_interface['replica_source']()

        self.sites = {}
        self.groups = {}
        self.datasets = {}

        if load_data:
            self.load()

    def load(self, load_replicas = True):
        """
        Load all information from local persistent storage to memory. The flag load_replicas
        can be used to determine whether dataset/block-site links should also be loaded;
        it is set to false when loading for an inventory update (link information is volatile).
        """

        logger.info('Loading data from local persistent storage.')

        self.sites = {}
        self.groups = {}
        self.datasets = {}
        
        self.store.acquire_lock()

        # temporary
        quota_manager = SiteQuotaRetriever()

        try:
            site_names = self.store.get_site_list(site_filt = config.inventory.included_sites)
            for name in config.inventory.excluded_sites:
                try:
                    site_names.remove(name)
                except ValueError:
                    continue

            sites, groups, datasets = self.store.load_data(site_filt = site_names, load_replicas = load_replicas)

            for site in sites:
                for group in groups:
                    site.set_group_quota(group, quota_manager.get_quota(site, group))

                site.active = quota_manager.get_status(site)

            self.sites = dict((s.name, s) for s in sites)
            self.groups = dict((g.name, g) for g in groups)
            self.datasets = dict((d.name, d) for d in datasets)

        finally:
            self.store.release_lock()

        logger.info('Data is loaded to memory.')

    def update(self, dataset_filter = '/*/*/*', load_first = True, make_snapshot = True):
        """Query the dataSource and get updated information."""

        logger.info('Locking inventory.')

        # Lock the inventory
        self.store.acquire_lock()

        try:
            if make_snapshot:
                logger.info('Making a snapshot of inventory.')
                # Make a snapshot (older snapshots cleaned by an independent daemon)
                self.store.make_snapshot()

            if load_first and len(self.sites) == 0:
                logger.info('Loading data from local storage.')
                self.load(load_replicas = False)

            else:
                logger.info('Unlinking replicas.')
                self.unlink_all_replicas()

            self.site_source.get_site_list(self.sites, filt = config.inventory.included_sites)

            for site_name in config.inventory.excluded_sites:
                try:
                    site = self.sites.pop(site_name)
                except KeyError:
                    continue

                site.unlink()
                del site
                self.store.clear_cache()

            self.site_source.set_site_status(self.sites)

            self.site_source.get_group_list(self.groups, filt = config.inventory.included_groups)

            # First get information on all replicas in the system, possibly creating datasets / blocks along the way.
            if dataset_filter == '/*/*/*':
                self.replica_source.make_replica_links(self.sites, self.groups, self.datasets)
            else:
                self.replica_source.make_replica_links(self.sites, self.groups, self.datasets, dataset_filt = dataset_filter)

            # Take out datasets with no replicas
            datasets = self.datasets.values()
            for dataset in datasets:
                if len(dataset.replicas) == 0:
                    self.datasets.pop(dataset.name)
                    self.store.clear_cache()
                    dataset.unlink()

            del datasets

            self.dataset_source.set_dataset_details(self.datasets, skip_valid = True)

            self.replica_source.find_tape_copies(self.datasets)

            logger.info('Saving data.')

            # Save inventory data to persistent storage
            # Datasets and groups with no replicas are removed
            self.store.save_data(self.sites.values(), self.groups.values(), self.datasets.values())

        finally:
            # Lock is released even in case of unexpected errors
            self.store.release_lock(force = True)

    def unlink_datasetreplica(self, replica):
        """
        Remove link from datasets and sites to the replica. Don't remove the replica-to-dataset/site link;
        replica objects may be still being used in the program.
        """

        dataset = replica.dataset
        site = replica.site

        # Remove block replicas from the site
        for block_replica in replica.block_replicas:
            site.remove_block_replica(block_replica)

        site.dataset_replicas.remove(replica)
        dataset.replicas.remove(replica)

    def unlink_all_replicas(self):
        for dataset in self.datasets.values():
            dataset.replicas = []

        for site in self.sites.values():
            site.dataset_replicas = []
            site.clear_block_replicas()

    def add_dataset_to_site(self, dataset, site, group = None):
        """
        Create a new DatasetReplica object and return.
        """

        new_replica = DatasetReplica(dataset, site, group = group)

        dataset.replicas.append(new_replica)
        site.dataset_replicas.append(new_replica)

        for block in dataset.blocks:
            block_replica = BlockReplica(block, site, group, is_complete = False, is_custodial = False, size = 0)
            new_replica.block_replicas.append(block_replica)
            site.add_block_replica(block_replica)

        return new_replica

    def scan_datasets(self, dataset_filter = '/*/*/*'):
        """
        Checks the information of existing datasets and save changes. Intended for an independent daemon process.
        """

        if len(self.datasets) == 0:
            self.load()

        if dataset_filter == '/*/*/*':
            datasets = [d for d in self.datasets.values() if d.status != Dataset.STAT_IGNORED]
        else:
            regex = re.compile(fnmatch.translate(dataset_filter))
            datasets = [d for d in self.datasets.values() if regex.match(d.name) and d.status != Dataset.STAT_IGNORED]

        self.dataset_source.set_dataset_details(datasets)

        self.store.save_datasets(datasets)


if __name__ == '__main__':

    from argparse import ArgumentParser
    import common.interface.classes as classes

    parser = ArgumentParser(description = 'Inventory manager')

    parser.add_argument('command', metavar = 'COMMAND', nargs = '+', help = '(update|scan|list (datasets|sites)) [commands]')
    parser.add_argument('--store', '-i', metavar = 'CLASS', dest = 'store_cls', default = '', help = 'Store class to be used.')
    parser.add_argument('--site-source', '-s', metavar = 'CLASS', dest = 'site_source_cls', default = '', help = 'SiteInfoSourceInterface class to be used.')
    parser.add_argument('--dataset-source', '-t', metavar = 'CLASS', dest = 'dataset_source_cls', default = '', help = 'DatasetInfoSourceInterface class to be used.')
    parser.add_argument('--replica-source', '-r', metavar = 'CLASS', dest = 'replica_source_cls', default = '', help = 'ReplicaInfoSourceInterface class to be used.')
    parser.add_argument('--dataset', '-d', metavar = 'EXPR', dest = 'dataset', default = '/*/*/*', help = 'Limit operation to datasets matching the expression.')
    parser.add_argument('--no-load', '-L', action = 'store_true', dest = 'no_load',  help = 'Do not load the existing inventory when updating.')
    parser.add_argument('--no-snapshot', '-S', action = 'store_true', dest = 'no_snapshot',  help = 'Do not make a snapshot of existing inventory when updating.')
    parser.add_argument('--log-level', '-l', metavar = 'LEVEL', dest = 'log_level', default = '', help = 'Logging level.')

    args = parser.parse_args()

    if args.log_level:
        try:
            level = getattr(logging, args.log_level.upper())
            logging.getLogger().setLevel(level)
        except AttributeError:
            logging.warning('Log level ' + args.log_level + ' not defined')

    kwd = {'load_data': False} # not loading data by default to speed up update process

    for cls in ['store', 'site_source', 'dataset_source', 'replica_source']:
        clsname = getattr(args, cls + '_cls')
        if clsname == '':
            kwd[cls + '_cls'] = classes.default_interface[cls]
        else:
            kwd[cls + '_cls'] = getattr(classes, clsname)

    manager = InventoryManager(**kwd)

    icmd = 0
    while icmd != len(args.command):
        command = args.command[icmd]
        icmd += 1
    
        if command == 'update':
            manager.update(dataset_filter = args.dataset, load_first = not args.no_load, make_snapshot = not args.no_snapshot)
    
        elif command == 'scan':
            manager.scan_datasets(dataset_filter = args.dataset)
    
        elif command == 'list':
            if len(manager.datasets) == 0:
                manager.load()
    
            target = args.command[icmd]
            icmd += 1
    
            if target == 'datasets':
                print manager.datasets.keys()
    
            elif target == 'sites':
                print manager.sites.keys()
