
from Globals import WORK_DIR
import os, glob, zlib, json
from tools import log as _log
from peewee import *

db = SqliteDatabase(
    os.path.join( WORK_DIR,  "data", "sqlite.db"), check_same_thread=False
)

finished_db = SqliteDatabase(
    os.path.join( WORK_DIR,  "data", "csfinished.db"), check_same_thread=False
)

class Snapshot(Model):
    m_time_snapshot = IntegerField()
    m_id = CharField()
    m_snapshot  = BlobField()
    m_status  = IntegerField()

    class Meta:
        database = db


class CSgame(Model):
    m_id  = CharField(unique=True)
    team1 = CharField()
    team2 = CharField()
    m_time= IntegerField()

    class Meta:
        database = db


class Gamestatus(Model):
    m_id  = CharField()
    m_status = IntegerField()
    m_time= IntegerField()

    class Meta:
        database = db


class GameFinished(Model):
    m_id  = CharField()
    data  = BlobField()

    class Meta:
        database = finished_db

Snapshot.create_table()
CSgame.create_table()
Gamestatus.create_table()
GameFinished.create_table()


log = _log("Model")

class ITSnapshot:

    YI = 0

    def __init__(self):
        self.name = "snapshot.shv"
        self.temp = []

    def insert(self, data):
        str_data = data["m_snapshot"].encode("utf8")
        compress = zlib.compress(str_data)
        data["m_snapshot"] = compress
        self.temp.append( data )

        query = Snapshot.select().where(Snapshot.m_id == data["m_id"])
        log.info("ID %s len snapshot: %d", data["m_id"], len(query) )

        # if ITSnapshot.YI == 5:
        #     # breakpoint()
        #     query = Snapshot.select().where(Snapshot.m_id == data["m_id"])


    def insert_many(self):


        Snapshot.insert_many(self.temp).execute()

        self.temp.clear()
        ITSnapshot.YI += 1


    def check_m_id_in_db(self, m_id):
        return bool( Snapshot.select().where(Snapshot.m_id == m_id) )

    def get_collection(self, m_id):
        query = Snapshot.select().where(Snapshot.m_id == m_id).order_by(Snapshot.m_time_snapshot)
        snapshots = [x._asdict() for x in query.namedtuples()]
        for snapshot in snapshots:
            snapshot.pop("id", None)
            snapshot["m_snapshot"] = zlib.decompress( snapshot["m_snapshot"] ).decode("utf8")

        return snapshots

    def get_collection_and_del(self, m_id):
        snapshots = self.get_collection(m_id)
        Snapshot.delete().where(Snapshot.m_id == m_id).execute()
        log.info("Del snapshot for game: %s", m_id)
        return snapshots


class ITCSGame:
    def __init__(self):
        self.db = None
        self.name = "csgame.shv"

    def insert(self, data):
        try:
            CSgame.insert(**data).execute()
        except IntegrityError:
            pass

    def get_csgame(self, m_id):
        # breakpoint()
        # data = self.db.get(m_id)
        query = CSgame.select().where(CSgame.m_id == m_id)
        data = query.namedtuples()[0]._asdict()
        data.pop("id", None)
        return data


class ITMStatus:
    def __init__(self):
        self.name = "mstatus.shv"

    def insert(self, data):
        Gamestatus.insert(**data).execute()

    def tmstatus(self):
        """список матчей которые имеют метку live
        Незабудь обновлять m_status = 0 после создания объекта
        """
        query = Gamestatus.select().where(Gamestatus.m_status == 0x01)
        data = [ x._asdict() for x in query.namedtuples()]
        [x.pop("id", None) for x in data]
        return data

    def csgame_processed(self, m_id):
        Gamestatus.update(
            {"m_status" : 0}).where(Gamestatus.m_id == m_id).execute()

    def get_live_csgame(self):
        return self.tmstatus()


class Finished:

    def __init__(self):
        self.name = "finished.shv"


    def add(self, data):
        m_id = data["m_id"]

        GameFinished.insert({
            "m_id" : m_id,
            "data" : json.dumps( data )
        }).execute()

        # self.key_list.append( data["m_id"] )
        # self.tree[ data["m_id"] ] = data





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


def prepare():
    for filename in glob.glob( os.path.join(WORK_DIR, "logs", "*.log") ):
        os.remove(filename)


if __name__ == '__main__':
    pass
