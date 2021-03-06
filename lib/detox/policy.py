import re
import fnmatch
import logging
import collections
import subprocess

import detox.variables as variables
from detox.condition import ReplicaCondition, SiteCondition

logger = logging.getLogger(__name__)

class ConfigurationError(Exception):
    def __init__(self, *args):
        if len(args) != 0:
            self.str = args[0] % args[1:]
        else:
            self.str = ''

    def __str__(self):
        return repr(self.str)

class Decision(object):
    pass

class Dismiss(Decision):
    pass

class Delete(Decision):
    pass

class DeleteOwner(Decision):
    def __init__(self, groups):
        self.groups = groups

class Keep(Decision):
    pass

class Protect(Decision):
    pass

class PolicyLine(object):
    """
    Call this Policy when fixing the terminology.
    AND-chained list of predicates.
    """

    def __init__(self, decision, text):
        self.condition = ReplicaCondition(text)
        self.decision = decision
        self.has_match = False
        if self.condition.static:
            self.cached_result = {}

        # filled by history interface
        self.condition_id = 0

    def __str__(self):
        return self.condition.text

    def __call__(self, replica):
        if self.condition.static:
            try:
                return self.cached_result[replica]
            except KeyError:
                pass

        if self.condition.match(replica):
            self.has_match = True
            result = (replica, self.decision, self.condition_id)
            if self.condition.static:
                self.cached_result[replica] = result

            return result
        else:
            if self.condition.static:
                self.cached_result[replica] = None

class Policy(object):
    """
    Responsible for partitioning the replicas and activating deletion on sites, and making deletion decisions on replicas.
    The core of the object is a stack of rules (specific rules first) with a fall-back default decision.
    A rule is a callable object that takes a dataset replica as an argument and returns None or (replica, decision, reason)
    """

    def __init__(self, partition, lines, version, inventory):
        self.partition = partition

        self.untracked_replicas = {} # temporary container of block replicas that are not in the partition

        self.static_optimization = True
        self.used_demand_plugins = set()
        self.parse_rules(lines, inventory)

        self.version = version

    def parse_rules(self, lines, inventory):
        logger.info('Parsing policy stack for partition %s.', self.partition.name)

        if type(lines) is file:
            conf = lines
            lines = map(str.strip, conf.read().split('\n'))
            il = 0
            while il != len(lines):
                if lines[il] == '' or lines[il].startswith('#'):
                    lines.pop(il)
                else:
                    il += 1

        self.target_site_def = None
        self.deletion_trigger = None
        self.stop_condition = None
        self.rules = []
        self.default_decision = -1
        self.candidate_sort = None

        LINE_SITE_TARGET, LINE_DELETION_TRIGGER, LINE_STOP_CONDITION, LINE_POLICY, LINE_ORDER = range(5)

        for line in lines:
            line_type = -1

            words = line.split()
            if words[0] == 'On':
                line_type = LINE_SITE_TARGET
            elif words[0] == 'When':
                line_type = LINE_DELETION_TRIGGER
            elif words[0] == 'Until':
                line_type = LINE_STOP_CONDITION
            elif words[0] == 'Order':
                line_type = LINE_ORDER
            elif words[0] == 'Protect':
                decision = Protect()
                line_type = LINE_POLICY
            elif words[0] == 'Dismiss':
                decision = Dismiss()
                line_type = LINE_POLICY
            elif words[0] == 'Delete':
                decision = Delete()
                line_type = LINE_POLICY
            elif words[0].startswith('DeleteOwner'):
                group_names = re.match('DeleteOwner\(([^)]+)\)', words[0]).group(1).split(',')
                groups = [inventory.groups[n] for n in group_names]
                decision = DeleteOwner(groups)
                line_type = LINE_POLICY
            else:
                raise ConfigurationError(line)

            if len(words) == 1:
                if line_type == LINE_POLICY:
                    self.default_decision = decision
                    continue
                else:
                    raise ConfigurationError(line)

            if line_type == LINE_ORDER:
                if words[1] == 'increasing':
                    reverse = False
                elif words[1] == 'decreasing':
                    reverse = True
                elif words[1] == 'none':
                    self.candidate_sort = lambda replicas: replicas
                    continue
                else:
                    raise ConfigurationError(words[1])

                for plugin, exprs in variables.required_plugins.items():
                    if words[2] in exprs:
                        self.used_demand_plugins.add(plugin)

                sortkey = variables.replica_vardefs[words[2]][0]
                self.candidate_sort = lambda replicas: sorted(replicas, key = sortkey, reverse = reverse)

            else:
                cond_text = ' '.join(words[1:])

                if line_type == LINE_SITE_TARGET:
                    self.target_site_def = SiteCondition(cond_text, self.partition)

                elif line_type == LINE_DELETION_TRIGGER:
                    self.deletion_trigger = SiteCondition(cond_text, self.partition)

                elif line_type == LINE_STOP_CONDITION:
                    self.stop_condition = SiteCondition(cond_text, self.partition)

                elif line_type == LINE_POLICY:
                    self.rules.append(PolicyLine(decision, cond_text))

        if self.target_site_def is None:
            raise ConfigurationError('Target site definition missing.')
        if self.deletion_trigger is None or self.stop_condition is None:
            raise ConfigurationError('Deletion trigger and release expressions are missing.')
        if self.default_decision == -1:
            raise ConfigurationError('Default decision not given.')
        if self.candidate_sort is None:
            raise ConfiguraitonError('Deletion candidate sorting is not specified.')

        for cond in [self.target_site_def, self.deletion_trigger, self.stop_condition]:
            self.used_demand_plugins.update(cond.used_demand_plugins)

        for rule in self.rules:
            if not rule.condition.static:
                logger.info('Condition %s is dynamic. Turning off static policy evaluation for %s.', str(rule.condition), self.partition.name)
                self.static_optimization = False

            self.used_demand_plugins.update(rule.condition.used_demand_plugins)

        logger.info('Policy stack for %s: %d rules using demand plugins %s', self.partition.name, len(self.rules), str(sorted(self.used_demand_plugins)))

    def partition_replicas(self, datasets):
        """
        Take the full list of datasets and pick out block replicas that are not in the partition.
        If a dataset replica loses all block replicas, take the dataset replica itself out of inventory.
        Return the list of all dataset replicas in the partition.
        """

        all_replicas = set()

        # stacking up replicas (rather than removing them one by one) for efficiency
        site_all_dataset_replicas = collections.defaultdict(list)
        site_all_block_replicas = collections.defaultdict(list)

        for dataset in datasets:
            if dataset.replicas is None:
                continue

            ir = 0
            while ir != len(dataset.replicas):
                replica = dataset.replicas[ir]
                site = replica.site
                # site occupancy is computed at the end by set_block_replicas

                if site.partition_quota(self.partition) == 0.:
                    self.untracked_replicas[replica] = replica.block_replicas
                    replica.block_replicas = []
                    dataset.replicas.pop(ir)
                    continue

                block_replicas = []                    
                not_in_partition = []

                for block_replica in replica.block_replicas:
                    if self.partition(block_replica):
                        # this block replica is in partition
                        if len(block_replicas) == 0:
                            # first block replica
                            site_all_dataset_replicas[site].append(replica)
                            site_block_replicas = site_all_block_replicas[site]

                        site_block_replicas.append(block_replica)
                        block_replicas.append(block_replica)
                    else:
                        not_in_partition.append(block_replica)

                if len(block_replicas) == 0:
                    # no block was in the partition
                    self.untracked_replicas[replica] = replica.block_replicas
                    replica.block_replicas = []

                else:
                    replica.block_replicas = block_replicas
    
                    if len(not_in_partition) != 0:
                        # remember blocks not in partition
                        self.untracked_replicas[replica] = not_in_partition

                if len(replica.block_replicas) == 0:
                    dataset.replicas.pop(ir)
                else:
                    all_replicas.add(replica)
                    ir += 1

        for site, dataset_replicas in site_all_dataset_replicas.items():
            site.dataset_replicas = set(dataset_replicas)

        for site, block_replicas in site_all_block_replicas.items():
            site.set_block_replicas(block_replicas)

        return all_replicas

    def restore_replicas(self):
        while len(self.untracked_replicas) != 0:
            replica, block_replicas = self.untracked_replicas.popitem()

            dataset = replica.dataset
            site = replica.site

            if replica not in dataset.replicas:
                dataset.replicas.append(replica)

            if replica not in site.dataset_replicas:
                site.dataset_replicas.add(replica)

            for block_replica in block_replicas:
                replica.block_replicas.append(block_replica)
                site.add_block_replica(block_replica)

    def evaluate(self, replica):
        for rule in self.rules:
            result = rule(replica)
            if result is not None:
                break
        else:
            return replica, self.default_decision, 0

        return result
