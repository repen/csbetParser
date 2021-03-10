from multiprocessing.connection import Listener
import traceback
from queue import Queue
from itertools import count
from threading import Thread

import re, os, zlib, json
from datetime import datetime
from bs4 import BeautifulSoup
from Globals import WORK_DIR, REMOTE_API
from tools import log as _log, divider
from collections import namedtuple
from ydisk import upload_object
from dataclasses import dataclass
from peewee import *

log = _log("RemoteBuild")


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

    # Snapshot.delete().where(Snapshot.m_id == m_id).execute()
    log.info("Write game [%s] in database csfinished.db", m_id)

q_input = Queue()
GameFinished.create_table()

mstatus   = namedtuple("mstatus",   ["m_id", "m_status", "m_time"])
msnapshot = namedtuple("msnapshot", ["m_id", "m_snapshot", "m_time_snapshot", "m_status"])


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

def get_winner(html):
    def detect(array):
        result = 0
        for arr in array:
            if "betting-won-team1" == arr:
                result = 1
            if "betting-won-team2" == arr:
                result = 2

        return result

    soup = html
    # soup = BeautifulSoup(html, "html.parser")
    dict_m = {}

    main_res =  soup.select_one( ".bm-main .bm-result")
    if not main_res:
        raise ValueError( "No element .bm-main .bm-result" )
    
    dict_m['Main'] = main_res.text.strip()
    dict_m['Main'] =  dict_m['Main'] if dict_m['Main'] else "Error"

    dict_m['Main|sum1']  = hand_num( soup.select_one(".bm-fullbet-summ.sys-stat-abs-1").text) if soup.select_one(".bm-fullbet-summ.sys-stat-abs-1") else 0
    dict_m['Main|sum2']  = hand_num( soup.select_one(".bm-fullbet-summ.sys-stat-abs-2").text) if soup.select_one(".bm-fullbet-summ.sys-stat-abs-2") else 0
    dict_m['Main|koef1'] = hand_num( soup.select_one(".stat-koef.sys-stat-koef-1").text) if soup.select_one(".stat-koef.sys-stat-koef-1") else 1.00
    dict_m['Main|koef2'] = hand_num( soup.select_one(".stat-koef.sys-stat-koef-2").text) if soup.select_one(".stat-koef.sys-stat-koef-2") else 1.00
    dict_m['Main|proc1'] = hand_num( soup.select_one(".sys-stat-proc-1").text) if soup.select_one(".sys-stat-proc-1") else 0
    dict_m['Main|proc2'] = hand_num( soup.select_one(".sys-stat-proc-2").text) if soup.select_one(".sys-stat-proc-2") else 0

    t1 = soup.select_one(".btn-bet-head.t1name").text.strip() if soup.select_one(".btn-bet-head.t1name") else "error"
    t2 = soup.select_one(".btn-bet-head.t2name").text.strip() if soup.select_one(".btn-bet-head.t2name") else "error"
    
    markets = soup.select(".bma-bet")

    for market in markets:
        map_ = ""
        [x.extract() for x in market.select("div[class*=bma-title-]")]
        name_market = market.select_one(".bma-title").text.strip()

        try:
            map_ = list(market.parent.previous_siblings)[1].text.strip()
        except IndexError:
            map_ = list( market.parent.parent.previous_siblings )[1].text.strip()

        map_ = "[{}]".format(map_) if "Карта" in map_ else ""
        name_market = " ".join([map_, name_market]) if map_ else name_market
        if "Победа на карте" in name_market:
            dict_m[name_market + "|sum1"]  = hand_num( market.parent.select_one(".sys-stat-abs-1").text )  if market.parent.select_one(".sys-stat-abs-1") else 0
            dict_m[name_market + "|sum2"]  = hand_num( market.parent.select_one(".sys-stat-abs-2").text )  if market.parent.select_one(".sys-stat-abs-2") else 0
            dict_m[name_market + "|proc1"] = hand_num( market.parent.select_one(".sys-stat-proc-1").text ) if market.parent.select_one(".sys-stat-proc-1") else 0
            dict_m[name_market + "|proc2"] = hand_num( market.parent.select_one(".sys-stat-proc-2").text ) if market.parent.select_one(".sys-stat-proc-2") else 0
            dict_m[name_market + "|koef1"] = hand_num( market.parent.select_one(".sys-stat-koef-1").text ) if market.parent.select_one(".sys-stat-koef-1")  else 1.00
            dict_m[name_market + "|koef2"] = hand_num( market.parent.select_one(".sys-stat-koef-2").text ) if market.parent.select_one(".sys-stat-koef-2") else 1.00

        else:
            dict_m[name_market + "|sum1"] = hand_num(market.select_one(".sys-stat-abs-1").text) if market.select_one(".sys-stat-abs-1") else 0
            dict_m[name_market + "|sum2"] = hand_num(market.select_one(".sys-stat-abs-2").text) if market.select_one(".sys-stat-abs-2") else 0
            dict_m[name_market + "|proc1"] = 0
            dict_m[name_market + "|proc2"] = 0
            dict_m[name_market + "|koef1"] = 1.00
            dict_m[name_market + "|koef2"] = 1.00
        # breakpoint()
        winner = detect( market['class'] )
        if winner == 0:
            score_text = market.select_one(".bma-score").text if market.select_one(".bma-score") else "0"
            data = list( map( lambda x : int(x), re.findall("\d+", score_text) ) )
            if data:
                winner = score_text
            else:
                winner = 0

        dict_m[ name_market ] = winner

    assert dict_m
    dict_m["t1name"] = t1
    dict_m["t2name"] = t2
    return dict_m

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


# Market  = namedtuple( 'Market', ['name', 'left', 'right', 'winner', 'time_snapshot',  "koefleft", "koefright"] )

class Market(namedtuple('Market', ['name', 'left', 'right', 'winner', 'time_snapshot',  "koefleft", "koefright"])):

    def __new__(cls, *args, koefleft=None, koefright=None):
        return super().__new__( cls, *args, koefleft=koefleft, koefright=koefright)

def get_fields_snapshot(html, winner, t_snapshot):
    soup = html
    # soup = BeautifulSoup(html, "html.parser")
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
        # M.append( Market( *param ) )
        M[name_market] = Market( *param )
        # {"name_market" : Market}
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



def worker(conn):
    try:
        while True:
            payload = conn.recv()
            q_input.put( payload )
    except EOFError:
        print("Connected close")

def server(address, authkey):
    serv = Listener(address, authkey=authkey)
    while True:
        try:
            client = serv.accept()
            worker( client )

        except Exception:
            traceback.print_exc()


def main():
    Thread(target=queue_service, daemon=True).start()
    server("/tmp/build_obj", authkey=b'qwerty') 

if __name__ == '__main__':
    main()