from multiprocessing.connection import Listener
import traceback, time, sys
from queue import Queue
from threading import Thread
from itertools import count
import re, os, zlib, json
from datetime import datetime
from bs4 import BeautifulSoup
from Globals import WORK_DIR, REMOTE_API
from tools import log as _log, divider
from collections import namedtuple
from ydisk import upload_object
from dataclasses import dataclass
from peewee import SqliteDatabase, CharField, BlobField, Model
from multiprocessing.connection import Client

log = _log("RemoteBuild")


NAME = "/tmp/build_obj"
if os.path.exists(NAME):
    os.remove(NAME)

finished_db = SqliteDatabase(
    os.path.join( WORK_DIR,  "data", "csfinished.db"), check_same_thread=False
)

class GameFinished(Model):
    m_id  = CharField()
    data  = BlobField()

    class Meta:
        database = finished_db


def write(data):
    m_id = data["m_id"]
    data["_name_markets"] = list( data["_name_markets"] )

    GameFinished.insert({
        "m_id" : m_id,
        "data" : zlib.compress( json.dumps( data ).encode("ascii") )
    }).execute()

    log.info("Write game [%s] in database csfinished.db", m_id)

q_input = Queue()
GameFinished.create_table()

PATH_OBJECT = os.path.join( WORK_DIR, "data", "objects" )

def hand_num(text):
    if not text:
        return 0
    if "." in text and "x" in text:
        return float(".".join( re.findall(r"(\d+)\.?", text) ) )
    # print(text)
    return int("".join( re.findall(r"\d", text) ) )

def get_htime(string):
    res = datetime.fromtimestamp(int(string)).strftime("%Y-%m-%d %H:%M:%S")
    return res

def extract_last_snapshot(*args):
    win_dict = args[0]
    res = {}
    for name in win_dict:
        if not re.search(r"\||t1name|t2name", name):
            obj = Market(
                name, win_dict[name + "|sum1"], 
                win_dict[name + "|sum2"], 
                win_dict[name], 
                int( datetime.now().timestamp() ),

            )
            res[name] = obj
    return res


class Market(namedtuple('Market', ['name', 'left', 'right', 'winner', 'time_snapshot',  "koefleft", "koefright"])):

    def __new__(cls, *args, koefleft=None, koefright=None):
        return super().__new__( cls, *args, koefleft=koefleft, koefright=koefright)

def get_fields_snapshot(html, winner, t_snapshot):
    soup = html
    markets = soup.select("div.bet-events__item")

    M = {}
    names = []
    name_market = "Main"
    left  = soup.select_one(".sys-stat-abs-1").text
    right = soup.select_one(".sys-stat-abs-2").text
    koef_left  = hand_num(soup.select_one(".sys-stat-koef-1").text) if soup.select_one(".sys-stat-koef-1") else 1
    koef_right = hand_num(soup.select_one(".sys-stat-koef-2").text) if soup.select_one(".sys-stat-koef-2") else 1

    
    param = ( 
        name_market, hand_num( left ) , hand_num( right ), winner[ name_market ], t_snapshot,
    )
    
    names.append( name_market )
    M[name_market] = Market( *param, koefleft=koef_left, koefright=koef_right )

    for market in markets:
        name_market = market.select_one(".bet-event__text-inside-part").text.strip()
        left = market.select_one(".bet-currency.bet-currency_RUB.sys-stat-abs-1").text
        right = market.select_one(".bet-currency.bet-currency_RUB.sys-stat-abs-2").text
        try:
            param = (
                name_market, hand_num( left ) , hand_num( right ), winner[ name_market ], t_snapshot,
            )
        except KeyError as e:
            log.error("Error %s", str(e), exc_info=True)
            continue

        names.append( name_market )
        M[name_market] = Market( *param )
    return M, names


class Fixture:
    def __init__(self,*args, **kwargs):
        self.id     = args[0]
        self.m_id   = args[0]
        self.m_time = args[1]
        self.team01 = args[2]
        self.team02 = args[3]
        self.league = kwargs.setdefault("league", "")
        self._name_markets = set()
        self._snapshots = []

    @property
    def markets(self):
        return self._snapshots

    @markets.setter
    def markets(self, elements):
        self._snapshots.append( elements )

    @markets.getter
    def markets(self):
        return self._snapshots

    @property
    def name_markets(self):
        return self._name_markets

    @name_markets.setter
    def name_markets(self, names):
        for name in names:
            self._name_markets.add( name )

    @name_markets.getter
    def name_markets(self,):
        return self._name_markets

    def _asdict(self):
        for snapshot in self.__dict__["_snapshots"]:

            for key in snapshot:
                snapshot[key] = snapshot[key]._asdict()

        return self.__dict__


@dataclass
class IParams:
    m_id:str
    m_time:int
    team01:str
    team02:str
    league:str
    ts_snapshots:list
    winner_dict:dict

def snapshot_service(params):
    param = IParams(**params)

    log.debug( "Object create {}".format( param.m_id )   )

    fixture = Fixture( 
        param.m_id, param.m_time,
        param.team01, param.team02,
        league = param.league.strip())

    for snapshot in param.ts_snapshots:
        html = snapshot.m_snapshot
        markets, names = get_fields_snapshot( BeautifulSoup(html, "html.parser"), param.winner_dict, snapshot.m_time_snapshot )
        fixture.name_markets = names
        fixture.markets = markets

    fixture.markets = extract_last_snapshot( param.winner_dict )
    # обрезка лишних даных
    fixture.__dict__["_snapshots"] = divider( fixture.__dict__["_snapshots"] )

    log.debug( "Object [%s] write in sqlite", param.m_id  )
    log.debug( "Object done {}".format(  param.m_id )  )

    write( fixture._asdict() ) # -> в базу


def queue_service():

    for c in count():
        params = q_input.get()
        snapshot_service( params )
        q_input.task_done()



def worker(conn):
    try:
        while True:
            payload = conn.recv()

            if isinstance( payload, int ):
                raise ValueError("Force close process")
            
            q_input.put( payload )

    except EOFError:
        log.info("Connected close")


def server(address, authkey):
    serv = Listener(address, authkey=authkey)
    for c in count():
        # try:
        client = serv.accept()

        worker( client )

        # except Exception:
        #     traceback.print_exc()

def check_done():
    log.info('Queue join!')
    time.sleep(35)
    q_input.join()
    log.info('Queue Done')
    # Send 0 for close process
    build_srv = Client(NAME, authkey=b"qwerty")
    build_srv.send(0)


def main():
    Thread(target=queue_service, daemon=True).start()
    Thread(target=check_done, daemon=True).start()

    try:
        server(NAME, authkey=b'qwerty')
    except Exception:
        log.error("Error", exc_info=True)
    finally:
        log.info("End process")
        



if __name__ == '__main__':
    main()