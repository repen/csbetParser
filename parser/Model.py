from peewee import *
from Globals import WORK_DIR
import os, glob, json, zlib
from tools import log as _log

from ZODB import FileStorage, DB
from BTrees.OOBTree import OOBTree
from persistent import Persistent
from persistent.list import PersistentList
import transaction
from datetime import datetime


storage = FileStorage.FileStorage( os.path.join( WORK_DIR,  "data", "mydatabase.fs"), pack_keep_old=False )
zopedb = DB(storage, large_record_size=1000000000000)
connection = zopedb.open()

root = connection.root()
# breakpoint()
db = SqliteDatabase( os.path.join( WORK_DIR,  "data", "csbet.db") )
log = _log("Model")

class ITSnapshot(Persistent):

    def __init__(self):
        self.container = OOBTree()


    def insert(self, data):
        if data["m_id"] not in self.container.keys():
            self.container[data["m_id"]] = set()

        list_snapshot = self.container[data["m_id"]] 

        str_data = json.dumps( data )
        str_data = str_data.encode("ascii")

        list_snapshot.add( zlib.compress(str_data)  )
        # list_snapshot.add( str_data  )

        log.info("ID %s len snapshot: %d", data["m_id"] , len( list_snapshot ) )
        self.container._p_changed = 1



    def delete(self, m_id):
        if m_id in self.container.keys():
            self.container.pop(m_id, None)
            transaction.commit()


class ITCSGame(Persistent):
    def __init__(self):
        self.csgame = {}

    def insert(self, data):
        self.csgame[ data["m_id"] ] = data
        self._p_changed = 1


class ITMStatus(Persistent):
    def __init__(self):
        self.tmstatus = []

    def insert(self, data):
        self.tmstatus.append( data )
        self._p_changed = 1

class Finished(Persistent):

    @staticmethod
    def transaction_commit():
        transaction.commit()

    def __init__(self):
        self.tree = OOBTree()


    def get_id_list(self):
        return list(self.tree.keys())

    def add(self, data):
        self.tree[ data["m_id"] ] = data

    def get_all_id(self):
        return list( self.tree.keys() )




TSnapshot = root.setdefault("snapshot" , ITSnapshot() )
TCSGame   = root.setdefault( "csgame",   ITCSGame()   )
TMStatus  = root.setdefault( "mstatus",  ITMStatus()  )
finished  = root.setdefault("finished",  Finished()   )
# transaction.commit()

# breakpoint()
# root["mstatus"].tmstatus = []

# for x in ['269898', '269913', '269943', '269954', '269956', '269958', '269960', '269962', '269964', '269966', '269968']:
#     data = {

#      "m_id" : x,
#      "m_status" : 1,
#      "m_time" : 0,
#     }
#     TMStatus.insert(data)

# transaction.commit()    

# breakpoint()

class CSGame(Model):
    m_id     = CharField(unique=True)
    m_time   = IntegerField()
    team1    = CharField()
    team2    = CharField()

    class Meta:
        database = db


class Snapshot( Model ):
    m_id       = CharField()
    m_snapshot = BlobField()
    m_time_snapshot = IntegerField()
    m_status = IntegerField()

    @staticmethod
    def delete_snapshot(m_id):
        Snapshot.delete().where( Snapshot.m_id == m_id ).execute()
        # db.execute_sql("VACUUM")

    @staticmethod
    def insert_safe( data ):
        try:
            Snapshot.insert(data).execute()
            result = True
        except OperationalError:
            result = False
        return result

    class Meta:
        database = db

class MStatus( Model ):
    m_id       = CharField(unique=True)
    m_status   = IntegerField()
    m_time     = IntegerField()

    @staticmethod
    def insert_safe(data):
        MStatus.insert(data).execute()
        result = True
        return result

    class Meta:
        database = db



def prepare():
    for filename in glob.glob( os.path.join(WORK_DIR, "logs", "*.log") ):
        os.remove(filename)


if __name__ == '__main__':
    pass
