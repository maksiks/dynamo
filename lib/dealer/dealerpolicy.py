import logging
import fnmatch
import random

from dynamo.dataformat import Site, Dataset, Block

LOG = logging.getLogger(__name__)

class ReplicaPlacementRule(object):
    """
    Defining the interface for replica placement rules.
    """

    def __init__(self):
        pass

    def dataset_allowed(self, dataset, site):
        return True

    def block_allowed(self, block, site):
        return True


# "already_exists" functions
# Return values are
# 0 -> item does not exist at the site
# 1 -> item exists but is owned by a different group
# 2 -> item exists and is owned by the group

def dataset_already_exists(dataset, site, group):
    level = 0
    replica = site.find_dataset_replica(dataset)
    if replica is not None and replica.is_full():
        level = 1

        owners = set(brep.group for brep in replica.block_replicas)
        if len(owners) == 1 and list(owners)[0] == group:
            level = 2

    return level

def block_already_exists(block, site, group):
    level = 0
    replica = site.find_block_replica(block)
    if replica is not None and replica.is_complete:
        level = 1
        if replica.group == group:
            level = 2

    return level

def blocks_already_exist(blocks, site, group):
    complete_at_site = True
    owned_at_site = True

    for block in blocks:
        replica = site.find_block_replica(block)
        if replica is None or not replica.is_complete:
            complete_at_site = False
        elif replica.group != group:
            owned_at_site = False

    if complete_at_site:
        if owned_at_site:
            return 2
        else:
            return 1
    else:
        return 0

# "group finding" functions
# If the item is owned by a single group at the site, return the group object
# Otherwise return None

def dataset_owned_by(dataset, site):
    replica = site.find_dataset_replica(dataset)
    if replica is not None:
        owners = set(brep.group for brep in replica.block_replicas)
        if len(owners) == 1:
            return list(owners)[0]

    return None

def block_owned_by(block, site):
    replica = site.find_block_replica(block)
    if replica is not None:
        return replica.group

    return None

def blocks_owned_by(blocks, site):
    group = None

    for block in blocks:
        replica = site.find_block_replica(block)
        if replica is None:
            if group is None:
                group = replica.group
            else:
                return None

    return group


class DealerPolicy(object):
    """
    Defined for each partition and implements the concrete conditions for copies.
    """

    def __init__(self, config, version = ''):
        self.partition_name = config.partition_name
        self.group_name = config.group_name

        self.target_site_names = list(config.target_sites)
        # Do not copy data to sites beyond target occupancy fraction (0-1)
        self.target_site_occupancy = config.target_site_occupancy
        # Maximum fraction of the quota that can be pending at a single site.
        self.max_site_pending_fraction = config.max_site_pending_fraction
        # Maximum overall volume that can be queued in this cycle for transfer.
        # The value is given in TB in the configuration file.
        self.max_total_cycle_volume = config.max_total_cycle_volume * 1.e+12

        self.version = version
        self.placement_rules = []

        # To be set at runtime
        self.target_sites = set()

    def set_target_sites(self, sites, partition):
        """
        @param sites   List of Site objects
        """

        for site in sites:
            if self.is_target_site(site.partitions[partition]):
                self.target_sites.add(site)

    def is_target_site(self, site_partition, additional_volume = 0.):
        site = site_partition.site
        quota = site_partition.quota

        if site.status != Site.STAT_READY:
            LOG.debug('%s is not ready', site.name)
            return False

        matches = False
        for pattern in self.target_site_names:
            if pattern.startswith('!'):
                if fnmatch.fnmatch(site.name, pattern[1:]):
                    matches = False
            else:
                if fnmatch.fnmatch(site.name, pattern):
                    matches = True

        if not matches:
            LOG.debug('%s does not match target site def', site.name)
            return False

        if self.target_site_occupancy < 1.:
            if quota == 0.:
                LOG.debug('%s has no quota', site.name)
                return False
            elif quota > 0.:
                occupancy_fraction = site_partition.occupancy_fraction(physical = False)
                occupancy_fraction += float(additional_volume) / quota
        
                if occupancy_fraction > self.target_site_occupancy:
                    LOG.debug('%s occupancy fraction %f > %f', site.name, occupancy_fraction, self.target_site_occupancy)
                    return False

        if self.max_site_pending_fraction < 1.:
            if quota == 0.:
                LOG.debug('%s has no quota', site.name)
                return False
            elif quota > 0.:
                occupancy_fraction = site_partition.occupancy_fraction(physical = False)
                occupancy_fraction += float(additional_volume) / quota

                # Difference between projected and physical volumes
                pending_fraction = occupancy_fraction
                pending_fraction -= site_partition.occupancy_fraction(physical = True)
        
                if pending_fraction > self.max_site_pending_fraction:
                    LOG.debug('%s pending fraction %f > %f', site.name, pending_fraction, self.max_site_pending_fraction)
                    return False

        return True

    def is_allowed_destination(self, item, site):
        """
        Check if the item (= Dataset, Block, or [Block]) is allowed to be at site, according to the set of rules.
        """

        for rule in self.placement_rules:
            if item is Dataset:
                if not rule.dataset_allowed(item, site):
                    return False

            elif item is Block:
                if not rule.block_allowed(item, site):
                    return False

            elif type(item) is list:
                for block in item:
                    if not rule.block_allowed(block, site):
                        return False

        return True

    def item_info(self, item):
        if type(item) is Dataset:
            item_name = item.name
            item_size = item.size
            already_exists = dataset_already_exists
            owned_by = dataset_owned_by

        elif type(item) is Block:
            item_name = item.full_name()
            item_size = item.size
            already_exists = block_already_exists
            owned_by = block_owned_by

        elif type(item) is list:
            # list of blocks (must belong to the same dataset)
            if len(item) == 0:
                return None, None, None, None

            dataset = item[0].dataset
            item_name = dataset.name
            item_size = sum(b.size for b in item)
            already_exists = blocks_already_exist
            owned_by = blocks_owned_by

        else:
            return None, None, None, None

        return item_name, item_size, already_exists, owned_by

    def find_destination_for(self, item, group, partition, candidates = None):
        item_name, item_size, already_exists, owned_by = self.item_info(item)

        if item_name is None:
            LOG.warning('Invalid request found. Skipping.')
            return None, None, None, 'Invalid request'

        if candidates is None:
            candidates = self.target_sites

        site_array = []
        for site in candidates:
            site_partition = site.partitions[partition]

            if site_partition.quota > 0.:
                projected_occupancy = site_partition.occupancy_fraction(physical = False)
                projected_occupancy += float(item_size) / site_partition.quota
    
                # total projected volume must not exceed the quota
                if projected_occupancy > 1.:
                    continue

            # replica must not be at the site already
            if already_exists(item, site, group):
                continue

            # placement must be allowed by the policy
            if not self.is_allowed_destination(item, site):
                continue

            p = 1. - projected_occupancy
            if len(site_array) != 0:
                p += site_array[-1][1]

            site_array.append((site, p))

        if len(site_array) == 0:
            LOG.warning('%s has no copy destination.', item_name)
            return None, item_name, item_size, 'No destination available'

        x = random.uniform(0., site_array[-1][1])

        isite = next(k for k in range(len(site_array)) if x < site_array[k][1])

        return site_array[isite][0], item_name, item_size, None

    def check_destination(self, item, destination, group, partition):
        item_name, item_size, already_exists, owned_by = self.item_info(item)

        if item_name is None:
            LOG.warning('Invalid request found. Skipping.')
            return None, None, 'Invalid request'

        if destination not in self.target_sites:
            LOG.debug('Destination %s for %s is not a target site.', destination.name, item_name)
            return item_name, item_size, 'Not a target site'

        if not self.is_allowed_destination(item, destination):
            LOG.debug('Placement of %s to %s not allowed by policy.', item_name, destination.name)
            return item_name, item_size, 'Not allowed'

        exists_level = already_exists(item, destination, group)

        if exists_level == 2: # exists and owned by the same group
            LOG.debug('%s is already at %s.', item_name, destination.name)
            return item_name, item_size, 'Replica exists'

        elif exists_level == 0: # does not exist
            site_partition = destination.partitions[partition]
            if site_partition.quota > 0:
                occupancy_fraction = site_partition.occupancy_fraction(physical = False)
                occupancy_fraction += float(item_size) / site_partition.quota
            else:
                occupancy_fraction = 1.
    
            if occupancy_fraction >= 1.:
                LOG.debug('Cannot copy %s to %s because destination is full.', item_name, destination.name)
                return item_name, item_size, 'Destination is full'

        return item_name, item_size, None
