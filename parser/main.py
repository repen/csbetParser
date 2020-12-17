from request import main as get_fixture
import Model
from tools import sheduler, log
import json, time, threading
from datetime import datetime
from Bot import Bot
from bs4 import BeautifulSoup
from objbuild import object_building
# from multiprocessing import Process, Queue
from multiprocessing import Process, Lock
from queue import Queue



logMain  = log("proc1:main:main.py", "/logs/main.log")
logProc2 = log("proc2:everyday_job:main.py", "/logs/everyday_job.log")
logCheckFixture = log("new_fixture:main.py", "/logs/check_new_fixture.log")
log_bot_work  = log("proc1:bot_work:main.py", "/logs/bot_work.log")
log_bot_work2 = log("proc1:bot_work2:main.py", "/logs/bot_work2.log")
loglqb = log("proc1:last_query_bot:main.py", "/logs/last_query_bot.log")
logQueue = log("proc1:queue", "/logs/queue.log")
Bot.logger = log

GenR = Model.HtmlData.get_iter()
GenLast = Model.HtmlData.iter_while_last()
queue = Queue( maxsize= 5 )

ids = []
G = {
    "iter1":True,
    "ThreadStop": False, 
}

def last_iter():
    zi = 0
    G['iter1'] = False
    while True:
        dq = next( GenLast )
        id_row = dq[0]

        if id_row not in ids:
            c_time = datetime.fromtimestamp( dq[1] ).strftime("%Y.%m.%d %H:%M")
            logQueue.debug("id: {} c: {} t:{}".format( id_row, zi, c_time ))
            queue.put(dq)
            ids.append( id_row )
        else:
            logQueue.debug("Wait query 60 sec {}".format(zi))
            time.sleep( 60 )
        zi += 1

def __create_queue(*args):
    last_iter()

def create_queue():
    while True:
        __create_queue()


class BL:
    '''Bot Logic'''
    obj_bots   = []
    id_bots    = []

def __check_new_fixture(*args):
    id_, time_snapshot, html = queue.get()
    fixtures = get_fixture(  time_snapshot, html )
    if not fixtures: 
        logCheckFixture.debug("Fixtures len {}".format( len( fixtures ) ))

    for fixture in fixtures:
        query = Model.CSGame.select(  ).where( Model.CSGame.m_id == fixture['m_id'] )
        if not query:
            
            Model.CSGame.insert({"m_id" : fixture['m_id'], "m_time" : fixture['m_time'],
                "team1" : fixture['t1name'], "team2" : fixture['t2name']
            }).execute()
            logCheckFixture.debug("data write to database CSGame")

        if fixture['m_id'] not in BL.id_bots:

            logCheckFixture.debug( "Start {}  {} | {}".format( fixture['m_id'], fixture['t1name'], fixture['t2name'] ) )
            
            bot = Bot( fixture['m_id'], fixture['m_time'] )
            
            BL.obj_bots.append( bot )
            BL.id_bots.append( fixture['m_id'] )
   
    queue.task_done()
    logCheckFixture.debug("len: {} ".format( len( BL.id_bots ) ))

def __bot_work():
    # time_snapshot, html = HtmlData.get_last_row() # query on database for last record
    id_, time_snapshot, html = queue.get() # query on database for last record
    log_bot_work.debug("get html from database. Length: {}".format( len( html ) ) )

    assert 250000 < len( html )
    
    soup = BeautifulSoup( html, "html.parser" )
    del_index = []
    for e, bot in enumerate( BL.obj_bots ):
        code = bot.main( time_snapshot, soup )
        if code == 401 or code == 402:
            del_index.append(e)
            log_bot_work.debug("Error code {}. Bot: {}".format( code,bot.m_id ))
  
    temp = []
    for e, bot in enumerate( BL.obj_bots ):
        if e in del_index:
            continue
        temp.append( bot )
    BL.obj_bots = temp

    queue.task_done()

def bot_work():

    i = 10
    log_bot_work2.debug("run bot_work_2")
    while True:

        log_bot_work2.debug("run __bot_work c:{}".format(i) )
        __bot_work()

        if i >= 10:
            log_bot_work2.debug( "run __check_new_fixture c:{}".format(i) )
            __check_new_fixture()
            i = 0
        i += 1
        

def everyday_job(*args):
    lock = args[0]
    logProc2.debug("Start a function everyday_job")
    time.sleep(60 * 60 * 1)
    while True:

        logProc2.debug("Handle database")
        with lock:
            object_building()
        logProc2.debug("Processing end")

        time.sleep( 60 * 65 * 3 )



def __main(*args):
    th0 = threading.Thread( target=create_queue )
    th2 = threading.Thread( target=bot_work )
    
    th0.start()
    th2.start()

    th0.join()
    th2.join()

def main():
    lock = Lock()
    # first_conn, second_conn = Pipe()

    p1 = Process( target=__main, args=(lock,) , daemon=True)
    p2 = Process( target=everyday_job, args=(lock,), daemon=True )
    
    p1.start()
    p2.start()    

    p1.join()
    # p2.join()
    # __main()


if __name__ == '__main__':
    main()
