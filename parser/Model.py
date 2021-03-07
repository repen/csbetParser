from peewee import *
from Globals import WORK_DIR
import os, glob, json, zlib, shelve, dbm
from tools import log as _log



db = SqliteDatabase( os.path.join( WORK_DIR,  "data", "csbet.db") )
log = _log("Model")

data = {
    "sadsad" : set(),
}

class BaseInterface:

    def open(self):
        self.db = shelve.open(os.path.join(WORK_DIR, "data", self.name) )


    def dump(self):
        self.db.close()    

    def close(self):
        self.db.close()


class ITSnapshot( BaseInterface ):

    YI = 0

    def __init__(self):
        # self.container = {}
        self.db = None
        self.name = "snapshot.shv"
    

    def dump(self):
        if ITSnapshot.YI % 10 == 0:
            log.info("Reorganize db: %d", ITSnapshot.YI)

            # self.db.dict.reorganize()
            # breakpoint()
        self.db.close()
        ITSnapshot.YI += 1

    def insert(self, data):
        
        if data["m_id"] not in self.db.keys():
            self.db[data["m_id"]] = set()

        snapshot_set = self.db[data["m_id"]]

        str_data = json.dumps( data )
        str_data = str_data.encode("ascii")

        snapshot_set.add( zlib.compress(str_data)  )
        snapshot_length = len( snapshot_set )

        self.db[data["m_id"]] = snapshot_set

        log.info("ID %s len snapshot: %d", data["m_id"] , snapshot_length )

    def get_keys(self):
        self.open()
        keys = list(self.db.keys())
        self.close()
        return keys

    def get_collection(self, m_id):
        self.open()
        data = self.db.get(m_id)
        self.close()
        return data

    def get_collection_and_del(self, m_id):
        self.open()
        data = self.db.pop(m_id, None)
        self.dump()
        return data


class ITCSGame(BaseInterface):
    def __init__(self):
        self.db = None
        self.name = "csgame.shv"

    def insert(self, data):
        self.db[ data["m_id"] ] = data
        # self.csgame[ data["m_id"] ] = data

    def get_csgame(self):
        self.open()
        data = self.db.get(m_id)
        self.close()
        return data


class ITMStatus:
    def __init__(self):
        self.name = "mstatus.shv"
        db = shelve.open(os.path.join(WORK_DIR, "data", self.name))
        if "mstatus" not in db.keys():
            db["mstatus"] = list()
        db.close()

    def insert(self, data):
        db = shelve.open(os.path.join(WORK_DIR, "data", self.name))
        temp = db["mstatus"]
        temp.append( data )
        db["mstatus"] = temp
        db.close()

    def tmstatus(self):
        db = shelve.open(os.path.join(WORK_DIR, "data", self.name))
        temp = db["mstatus"]
        db.close()
        return temp


class Finished:

    def __init__(self):
        self.name = "finished.shv"
        db = shelve.open(os.path.join(WORK_DIR, "data", self.name))
        self.key_list = list(db.keys())
        db.close()

    def get_id_list(self):
        return self.key_list
        # return list(self.tree.keys())

    def add(self, data):
        self.key_list.append( data["m_id"] )
        # self.tree[ data["m_id"] ] = data
        db = shelve.open(os.path.join(WORK_DIR, "data", self.name))
        db[data["m_id"]] = data
        db.close()

    def get_all_id(self):
        return self.key_list
        # return list( self.tree.keys() )




TSnapshot = ITSnapshot()
TCSGame   = ITCSGame()
TMStatus  = ITMStatus()
finished  = Finished()
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
