from common.interface.phedexdbsssb import PhEDExDBSSSB
from common.interface.mysqlstore import MySQLStore
from common.interface.dbs import DBS
from common.interface.popdb import PopDB
from common.interface.globalqueue import GlobalQueue
from common.interface.mysqlhistory import MySQLHistory
from common.interface.weblock import WebReplicaLock

class Generator(object):
    """
    Generator of various objects with a storage for singleton objects.
    """

    _singletons = {}

    def __init__(self, cls):
        self._cls = cls

    def __call__(self):
        try:
            obj = Generator._singletons[self._cls]
        except KeyError:
            obj = self._cls()
            Generator._singletons[self._cls] = obj

        return obj


class DummyInterface(object):
    def __init__(self):
        pass
            

default_interface = {
    'dataset_source': Generator(PhEDExDBSSSB),
    'site_source': Generator(PhEDExDBSSSB),
    'replica_source': Generator(PhEDExDBSSSB),
    'copy': Generator(PhEDExDBSSSB),
    'deletion': Generator(PhEDExDBSSSB),
    'store': Generator(MySQLStore),
    'history': Generator(MySQLHistory)
}

demand_plugins = {
    'replica_locks': Generator(WebReplicaLock),
    'replica_access': Generator(PopDB),
    'dataset_request': Generator(GlobalQueue)
}
