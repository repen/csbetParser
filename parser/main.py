from request import main as get_fixture
import requests, time, os, zlib
from tools import log as l
from Bot import Bot
from bs4 import BeautifulSoup
from objbuild import object_building
from multiprocessing import Process, Lock
from Globals import REMOTE_API, BASE_DIR
from itertools import count
from Model import prepare, CSGame, Snapshot, TCSGame, TSnapshot, zopedb, finished
import transaction
from ydisk import upload_object
from datetime import datetime


prepare()
log = l("Main")

ids = []

class BL:
    '''Bot Logic'''
    obj_bots   = []
    id_bots    = []

def _check_new_fixture(*args):
    # id_, time_snapshot, html = queue.get()
    log.info("Check new fixture")
    response = requests.get(REMOTE_API+ "/html")
    data = response.json()
    time_snapshot = data['snapshot_time']
    html = data['data']
    fixtures = get_fixture(  time_snapshot, html )

    log.info(">>> before fixtures [%d]", len(BL.obj_bots))
    for fixture in fixtures:

        csgame_data = {
            "m_id": fixture['m_id'],
            "m_time": fixture['m_time'],
            "team1": fixture['t1name'],
            "team2": fixture['t2name']
        }

        TCSGame.insert(csgame_data)

        if fixture['m_id'] not in BL.id_bots:
            bot = Bot( fixture['m_id'], fixture['m_time'] )
            BL.obj_bots.append( bot )
            BL.id_bots.append( fixture['m_id'] )

    log.info("<<< after fixtures [%d]", len(BL.obj_bots))



def _bot_work():
    _objs = set()

    def removing_garbage():
        nonlocal _objs
        obj_list = set(finished.get_id_list())

        if _objs != obj_list:
            new = obj_list.difference(_objs)
            _objs = obj_list
            for obj in new:
                m_id = obj.split("/")[-1]
                TSnapshot.delete( m_id )


    response = requests.get(REMOTE_API+ "/html")
    data = response.json()
    time_snapshot = data['snapshot_time']
    html = data['data']
    
    upload_object( zlib.compress( html.encode("utf8") ) , "date_" + str(int(datetime.now().timestamp())) )
    
    log.info("Bot Work response html %d", len(html) )

    # assert 250000 < len( html )
    if 250000 > len( html ):
        log.info("Warning. Html len: %d", len(html))
        return

    log.info("[ - start work bots = %d - ]", len(BL.obj_bots))
    soup = BeautifulSoup( html, "html.parser" )
    del_index = []
    for e, bot in enumerate( BL.obj_bots ):
        code = bot.main( time_snapshot, soup )
        if code == 401 or code == 402:
            del_index.append(e)

    temp = []
    for e, bot in enumerate( BL.obj_bots ):
        if e in del_index:
            continue
        temp.append( bot )
    BL.obj_bots = temp


    log.info("[ - end work bots = %d - ]", len(BL.obj_bots))
    log.info("Start [ removing garbage ]")
    removing_garbage()
    log.info("Stop  [ removing garbage ]")



def bot_work():
    log.info("Start bot-work")
    for c in count():

        if c % 10 == 0:
            _check_new_fixture()

        _bot_work()

        transaction.commit()
        zopedb.pack()
        log.info("-= bot work sleep 60 =-")
        time.sleep(60)


def proc2():
    wait = 60 * 60 * 3
    log = l("Proc2")
    for _ in count():
        try:
            log.info("Build!!")
            object_building()
            log.info("Wait %d sec", wait)
            time.sleep(wait)
        except Exception as e:
            log.error("Error", exc_info=True)

def proc1():
    try:
        bot_work()
    except KeyboardInterrupt:
        log.info("Pr > Ctrl + C ")
        log.error("Error", exc_info=True)
    
def main02():
    for c in count():
        z = 50
        if c % 10 == 0:
            _check_new_fixture()

        _bot_work()

        if c % 60 == 0:
            z = 0
            log.info("[ Start Build ]")
            try:
                object_building()
                log.info("[ End Build ]")
            except Exception as e:
                log.error("Error", exc_info=True)

        transaction.commit()
        zopedb.pack()
        log.info("-= bot work sleep 60 c=%d", c)
        time.sleep(z)

def main():
    # first_conn, second_conn = Pipe()
    lock = Lock()

    p1 = Process( target=proc1, daemon=True)
    p2 = Process( target=proc2, daemon=True )
    
    p1.start()
    p2.start()    

    p1.join()
    p2.join()
    # __main()


if __name__ == '__main__':
    main02()
    # main()
    # proc1()
    # proc2()
