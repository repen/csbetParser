from peewee import *
from Globals import WORK_DIR
import time

from datetime import datetime, timedelta
db   = SqliteDatabase(WORK_DIR + '/data/database/csgo.db') # Snapshot
db02 = SqliteDatabase(WORK_DIR + '/data/database/csgo02.db') # Fixture info
dbHtml = SqliteDatabase(WORK_DIR + '/data/database/datahtml.db') # html
# dbHtml = SqliteDatabase("/home/repente/SERVERS/Remote/srv1/database/datahtml.db") # html



class CSGame(Model):
    m_id     = CharField()
    m_time   = IntegerField()
    team1    = CharField()
    team2    = CharField()

    class Meta:
        database = db02



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
    m_id       = CharField()
    m_status   = IntegerField()
    m_time     = IntegerField()

    @staticmethod
    def insert_safe(data):
        try:
            MStatus.insert(data).execute()
            result = True
        except OperationalError:
            result = False
        return result

    class Meta:
        database = db



class HtmlData(Model):
    m_time   = IntegerField( )
    html     = BlobField()


    # @staticmethod
    # def get_last_row():
    #     cursor = dbHtml.execute_sql("SELECT * FROM HtmlData ORDER BY rowid DESC LIMIT 1;" )
    #     data = cursor.fetchone()
    #     id_ = data[0]
    #     time_snap = data[1]
    #     html = data[2]
    #     return id_, time_snap,  html.decode('unicode-escape')

    @staticmethod
    def get_last_row():
        query = HtmlData.select().order_by( HtmlData.id.desc() ).get()
        last_row = HtmlData.select().where( HtmlData.id == query )
        data = last_row.tuples()[0]
        # print(query)
        if last_row:
            id_ = data[0]
            time_snap = data[1]
            html = data[2]
            return id_, time_snap, html.decode('unicode-escape')
        raise ValueError

    @staticmethod
    def iter_while_last():
        while True:
            try:
                yield HtmlData.get_last_row()
            except OperationalError:
                time.sleep( 120 )

    @staticmethod
    def get_iter_close():
        query = HtmlData.select()
        for que in query.tuples():
            id_ = que[0]
            time_snap = que[1]
            html = que[2]
            yield id_, time_snap,  html.decode('unicode-escape')
            # yield que

    @staticmethod
    def get_row(id_num):
        query = HtmlData.select().where( HtmlData.id == id_num )
        for que in query.tuples():
            id_ = que[0]
            time_snap = que[1]
            html = que[2]

        return id_, time_snap,  html.decode('unicode-escape')

    @staticmethod
    def get_count_row():
        cursor = dbHtml.execute_sql("SELECT rowid FROM HtmlData ORDER BY rowid DESC LIMIT 1;" )
        data = cursor.fetchone()
        return data[0]



    @staticmethod
    def get_iter():
        # method not work!!!
        START = 2000
        tasks = [ x  for x in range( START, HtmlData.get_count_row() + 1 )]
        print(tasks)
        for task in tasks:
            yield HtmlData.get_row( task )

            # yield task


    class Meta:
        database = dbHtml


CSGame.create_table()
Snapshot.create_table()
HtmlData.create_table()
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
    MStatus.update({
        "m_status": 0,
    }).where(MStatus.m_id == 240744).execute()
