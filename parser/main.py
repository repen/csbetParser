from request import main as get_fixture
import requests, Model, time
from tools import log as l
from Bot import Bot
from bs4 import BeautifulSoup
# from objbuild import object_building
from multiprocessing import Process
from Globals import REMOTE_API
from itertools import count

Model.init_db()
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
        query = Model.CSGame.select(  ).where( Model.CSGame.m_id == fixture['m_id'] )
        if not query:
            
            Model.CSGame.insert({"m_id" : fixture['m_id'], "m_time" : fixture['m_time'],
                "team1" : fixture['t1name'], "team2" : fixture['t2name']
            }).execute()

        if fixture['m_id'] not in BL.id_bots:
            bot = Bot( fixture['m_id'], fixture['m_time'] )
            BL.obj_bots.append( bot )
            BL.id_bots.append( fixture['m_id'] )

    log.info("<<< after fixtures [%d]", len(BL.obj_bots))

def _bot_work():
    response = requests.get(REMOTE_API+ "/html")
    data = response.json()
    time_snapshot = data['snapshot_time']
    html = data['data']
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


def bot_work():
    log.info("Start bot-work")
    for c in count():

        if c % 10 == 0:
            _check_new_fixture()

        _bot_work()

        log.info("-= bot work sleep 60 =-")
        time.sleep(60)


def proc2():
    time.sleep(60 * 60 * 3)
    for _ in count():
        # object_building()
        time.sleep( 60 * 65 * 3 )

def proc1():
    try:
        bot_work()
    except KeyboardInterrupt:
        log.info("Pr > Ctrl + C ")
        log.error("Error", exc_info=True)
    

def main():
    # first_conn, second_conn = Pipe()

    p1 = Process( target=proc1, daemon=True)
    p2 = Process( target=proc2, daemon=True )
    
    p1.start()
    p2.start()    

    p1.join()
    # p2.join()
    # __main()


if __name__ == '__main__':
    # main()
    proc1()
