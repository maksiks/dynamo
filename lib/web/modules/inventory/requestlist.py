import fnmatch
import re
import logging

from dynamo.web.modules._base import WebModule
from dynamo.web.exceptions import InvalidRequest
from dynamo.dataformat import Dataset, Group
from dynamo.request.copy import CopyRequestManager
from dynamo.dataformat.request import Request
from dynamo.dataformat import Dataset


LOG = logging.getLogger(__name__)

class RequestList(WebModule):
    """
    request listing
    """

    def __init__(self, config):
        WebModule.__init__(self, config)
        
        self.copy_manager = CopyRequestManager()
        self.copy_manager.set_read_only()


    def run(self, caller, request, inventory):
        if 'decision' in request:
            if request['decision'] != 'approved':
                return {'request': []}
        if 'approval' in request:
            if request['approval'] != 'approved':
                return {'request': []}
        if 'decided_by' in request:
            return {'request': []}


        # def get_requests(self, request_id = None, statuses = None, users = None, items = None, sites = None):
        #  __slots__ = ['request_id', 'user', 'user_dn', 'status', 'reject_reason', 'sites', 'items', 'actions']
        reqid = None
        requested_by = None
        site_names = None
        item_names = None
        if 'request' in request:
            reqid = int(request['request'])
        if 'requested_by' in request:
            requested_by = [request['requested_by']]
        if 'node' in request:
            site_names = []
            nodepat = re.compile(fnmatch.translate(request['node']))
            for site in inventory.sites:
                if nodepat.match(site):
                    site_names.append(site)
            if len(site_names) < 1: site_names = None

        if 'dataset' in request:
            item_names = []
            dset_name = request['dataset']
            if '*' in dset_name:
                pattern = re.compile(fnmatch.translate(dset_name))
                for thename in inventory.datasets.iterkeys():
                    if pattern.match(thename):
                        item_names.append(thename)
            elif dset_name in inventory.datasets:
                item_names.append(dset_name)
            if len(item_names) < 1: item_names = None

        
        if 'block' in request:
            item_names = []
            block_full_name = request['block']

            try:
                dset_name, _, block_name = block_full_name.partition('#')
            except:
                raise InvalidRequest('Invalid block name %s' % block_full_name)

            if Dataset.name_pattern.match(dset_name) is None:
                raise InvalidRequest('Invalid dataset name %s' % dset_name)

            datasets = []
            if '*' in dset_name:
                pattern = re.compile(fnmatch.translate(dset_name))
                for dset_obj in inventory.datasets.itervalues():
                    if pattern.match(dset_obj.name) is not None:
                        datasets.append(dset_obj)
            elif dset_name in inventory.datasets:
                datasets.append(inventory.datasets[dset_name])

            if '*' in block_name:
                pattern = re.compile(fnmatch.translate(block_name))
            else:
                pattern = None

            for dset_obj in datasets:
                for block_obj in dset_obj.blocks:
                    if pattern is None:
                        if block_obj.real_name() == block_name:
                            item_names.append(block_obj.full_name())
                    else:
                        if pattern.match(block_obj.real_name()) is not None:
                            item_names.append(block_obj.full_name())
            
            if len(item_names) < 1: 
                item_names = None

        #return {'request': item_names}
        
        #try:
        erequests = self.copy_manager.get_requests(request_id=reqid, users=requested_by, sites=site_names,
                                                       items=item_names)
        #except:
        #    return {'request':['error']}

        if len(erequests) > 0:
            return {'request':[len(erequests)]}
        else:
            return {'request':[]}



        # collect information from the inventory and registry according to the requests
        datasets = []
        pattern = re.compile(fnmatch.translate(dset_name))
        if '*' in dset_name:
            for thename in inventory.datasets.iterkeys():
                if pattern.match(thename):
                    datasets.append(inventory.datasets[thename])
        else:
            if dset_name in inventory.datasets:
                datasets.append(inventory.datasets[dset_name])
        

        
        blocks = {}
        blockreps = {}
        if 'node' in request:
            nodepat = re.compile(fnmatch.translate(request['node']))
        if '*' in block_name:
            blockpat = re.compile(fnmatch.translate(block_name))
        for dset_obj in datasets:
            blocks[dset_obj] = []
            for block_obj in dset_obj.blocks:
                if '*' in block_name:
                    if not blockpat.match(block_obj.real_name()):
                        continue
                else:
                    if block_name != '' and block_name != block_obj.real_name():
                        continue

                blocks[dset_obj].append(block_obj)
                blockreps[block_obj] = []
                for blockrep_obj in block_obj.replicas:
                    if 'node' in request:
                        site_name = blockrep_obj.site.name
                        if '*' in node_name:
                            if not nodepat.match(site_name):
                                continue
                        else:
                            if site_name != request['node']:
                                continue
                
                    if 'complete' in request:
                        if request['complete'] == 'y':
                            if not blockrep_obj.is_complete():
                                continue
                        if request['complete'] == 'n':
                            if blockrep_obj.is_complete():
                                continue

                    if 'group' in request:
                        if request['group'] != blockrep_obj.group.name:
                            continue

                    if 'update_since' in request:
                        update_since = int(request['update_since'])
                        if update_since > blockrep_obj.last_update:
                            continue

                    if 'create_since' in request:
                        update_since = int(request['create_since'])
                        if create_since > blockrep_obj.last_update:
                            continue
                    blockreps[block_obj].append(blockrep_obj)
           
        
        response = []
        
        for dset_obj in blocks:
            for block_obj in blocks[dset_obj]:
                repline = []
                for blkrep in blockreps[block_obj]:
                    if blkrep.group is Group.null_group:
                        subscribed = 'n'
                    else:
                        subscribed = 'y'

                    rephash = {'node': blkrep.site.name, 'files': blkrep.num_files, 'node_id': blkrep.site.id, 
                               'se': blkrep.site.host, 'complete': self.crt(blkrep.is_complete()), 
                               'time_create': blkrep.last_update, 'time_update': blkrep.last_update,
                               'group': blkrep.group.name, 'custodial': self.crt(blkrep.is_custodial),
                               'subscribed': subscribed}
                    repline.append(rephash)
                if len(repline) < 1 : continue

                line = {'name': block_obj.full_name(), 'files': block_obj.num_files, 'bytes': block_obj.size, 
                        'is_open': self.crt(block_obj.is_open), 'id': block_obj.id, 'replica': repline }
                response.append(line)
        
        return {'block': response}

    def crt(self,boolval):
        if boolval == True: return 'y'
        return 'n'


# exported to __init__.py
export_data = {
    'requestlist': RequestList
}
