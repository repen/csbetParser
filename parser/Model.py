from peewee import *
from Globals import WORK_DIR
import os, glob, json, zlib
from collections import namedtuple

from ZODB import FileStorage, DB
from BTrees.OOBTree import OOBTree
from persistent import Persistent, list, dict
# blob_dir = os.path.join( WORK_DIR,  "data", "fl")
storage = FileStorage.FileStorage( os.path.join( WORK_DIR,  "data", "mydatabase.fs"), pack_keep_old=False )
zopedb = DB(storage, large_record_size=1000000000000)
connection = zopedb.open()

root = connection.root()

db = SqliteDatabase( os.path.join( WORK_DIR,  "data", "csbet.db") )


class ITSnapshot(Persistent):
    def __init__(self):
        self.container = OOBTree()


    def insert(self, data):
        list_snapshot = self.container.setdefault(data["m_id"], set())

        str_data = json.dumps( data )
        str_data = str_data.encode("ascii")

        list_snapshot.add( zlib.compress(str_data)  )
        # list_snapshot.add( str_data  )

        print("ID", data["m_id"], " len snapshot: ", len( list_snapshot ) )
        self.container._p_changed = 1
        # self._p_changed = 1

class ITCSGame(Persistent):
    def __init__(self):
        self.csgame = {}

    def insert(self, data):
        self.csgame[ data["m_id"] ] = data
        self._p_changed = 1


class ITMStatus(Persistent):
    def __init__(self):
        self.tmstatus = set()

    def insert(self, data):
        self.tmstatus.add( data )
        self._p_changed = 1


TSnapshot = root.setdefault("snapshot" , ITSnapshot() )
TCSGame = root.setdefault( "csgame", ITCSGame() )
TMStatus = root.setdefault( "mstatus", ITMStatus() )


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



def init_db():
    for filename in glob.glob( os.path.join(WORK_DIR, "logs", "*.log") ):
        os.remove(filename)

    # CSGame.drop_table()
    # Snapshot.drop_table()
    # MStatus.drop_table()

    CSGame.create_table()
    Snapshot.create_table()
    MStatus.create_table()


def get_size():
    '''
    Returns:
        int:returning size database on KB
    '''
    cursor = db.execute_sql('SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()')
    res = cursor.fetchone()

    return int( res[0] / 1024 )

# "SELECT * FROM HtmlData ORDER BY rowid DESC LIMIT 1;"
# cursor = db.execute_sql('select count(*) from snapshot;')
# res = cursor.fetchone()
# print('Total: ', res[0])
# SELECT COUNT(*) *  -- The number of rows in the table
#      ( 24 +        -- The length of all 4 byte int columns
#        12 +        -- The length of all 8 byte int columns
#        128 )       -- The estimate of the average length of all string columns
# FROM MyTable

# cursor = db.execute_sql('SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()')
# res = cursor.fetchone()

# db.execute_sql("VACUUM")
# time = datetime.fromtimestamp( r.m_time_snapshot ).strftime("%Y.%m.%d %H:%M")


if __name__ == '__main__':
    # query = CSGame.select()
    # query = Snapshot.select().where(Snapshot.m_id == "267805")
    query = Snapshot.select()
    # breakpoint()
    print(len(query))
