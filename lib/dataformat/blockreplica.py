from dataformat.exceptions import ObjectError

class BlockReplica(object):
    """Block placement at a site. Holds an attribute 'group' which can be None.
    BlockReplica size can be different from that of the Block."""

    __slots__ = ['block', 'site', 'group', 'is_complete', 'is_custodial', 'size', 'last_update', 'files']

    def __init__(self, block, site, group = None, is_complete = False, is_custodial = False, size = -1, last_update = 0):
        self.block = block
        self.site = site
        self.group = group
        self.is_complete = is_complete
        self.is_custodial = is_custodial
        if size < 0:
            self.size = block.size
        else:
            self.size = size
        self.last_update = last_update

        # set of File objects for incomplete replicas
        self.files = None

    def __str__(self):
        return 'BlockReplica %s/%s#%s (group=%s, is_complete=%s, size=%d, last_update=%d)' % \
            (self.site.name, self.block.dataset.name, self.block.real_name(),
                'None' if self.group is None else self.group.name, self.is_complete, self.size, self.last_update)

    def __repr__(self):
        return 'BlockReplica(block=%s, site=%s, group=%s)' % (repr(self.block), repr(self.site), repr(self.group))

    def copy(self, other):
        self.group = other.group
        self.is_complete = other.is_complete
        self.is_custodial = other.is_custodial
        self.size = other.size
        self.last_update = other.last_update

    def unlinked_clone(self):
        block = self.block.unlinked_clone()
        site = self.site.unlinked_clone()
        if self.group is None:
            group = None
        else:
            group = self.group.unlinked_clone()

        return BlockReplica(block, site, group, self.is_complete, self.is_custodial, self.size, self.last_update)

    def linked_clone(self, inventory):
        dataset = inventory.datasets[self.block.dataset.name]
        block = dataset.find_block(self.block.name, must_find = True)
        site = inventory.sites[self.site.name]
        if self.group is None:
            group = None
        else:
            group = inventory.groups[self.group.name]

        replica = BlockReplica(block, site, group, self.is_complete, self.is_custodial, self.size, self.last_update)

        dataset_replica = dataset.find_replica(site, must_find = True)
        dataset_replica.block_replicas.add(replica)
        block.replicas.add(replica)
        site.add_block_replica(replica)

        return replica
