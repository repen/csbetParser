import re, os, requests, zlib, time
from Model import TMStatus, TSnapshot, TCSGame
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from Globals import WORK_DIR, REMOTE_API
from tools import log as _log
from collections import namedtuple
from ydisk import upload_object
from dataclasses import dataclass
from multiprocessing.connection import Client
from multiprocessing import Process

log = _log("prebuild")

mstatus   = namedtuple("mstatus",   ["m_id", "m_status", "m_time"])
msnapshot = namedtuple("msnapshot", ["m_id", "m_snapshot", "m_time_snapshot", "m_status"])


PATH_OBJECT = os.path.join( WORK_DIR, "data", "objects" )

class NotElementErr( Exception ):
    pass

def hand_num(text):
    if not text:
        return 0
    if "." in text and "x" in text:
        return float(".".join( re.findall(r"(\d+)\.?", text) ) )
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
    dict_m = {}

    main_res =  soup.select_one( ".bm-main .bm-result")
    if not main_res:
        raise NotElementErr( "No element .bm-main .bm-result" )
    
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


def create_task(m_id):
    url = REMOTE_API + "/task/{}".format(m_id)
    log.info( "Create Task %s", url )
    response = requests.get(url)

def get_result_page(m_id):
    RES_URL = REMOTE_API + "/result/{}".format(m_id)
    log.info("Get result page: %s", RES_URL)
    response = requests.get(RES_URL)
    data = response.json()
    html = data['result']
    log.info("Response for %s. Result: [%s]. Length [%d]", m_id, bool( html ), len(html) )
    return data["result"]

@dataclass
class IParams:
    m_id:str
    m_time:int
    team01:str
    team02:str
    league:str
    ts_snapshots:list
    winner_dict:dict

def handling_process():
    from objbuild2 import main
    main()

def object_building():
    log.debug("Start. -- Build object --")

    time_difference = int( datetime.now().timestamp() - ( datetime.now() - timedelta( seconds = 60 * 60 * 5 ) ).timestamp() )

    game_happened = [ mstatus(**x) for x in TMStatus.get_live_csgame() ]

    log.debug("Quantity fixtures for handling: %d ( all )", len( game_happened ))

    game_happened = list( filter(
        lambda x :  datetime.fromtimestamp( x.m_time ).timestamp() + time_difference < datetime.now().timestamp(), game_happened )
    )

    log.debug("Quantity fixtures for handling: %s ( filter time )", str( game_happened ))



    #Запустить другой процесс
    if game_happened:
        proc1 = Process(target=handling_process, daemon=True).start()
        time.sleep(2)
        build_srv = Client("/tmp/build_obj", authkey=b"qwerty")
    
    for game in game_happened:

        if not TSnapshot.check_m_id_in_db(game.m_id):
            log.debug( "Fixture {} is not in Shanpshot.db".format( game.m_id) )
            continue


        _html = get_result_page(game.m_id)
        if not bool(_html):
            create_task(game.m_id)
            log.info("Continue: not html %s", bool(_html))
            continue

        
        upload_object( zlib.compress(_html.encode("utf8")), "id_" + str(game.m_id) )
        soup_html = BeautifulSoup(_html, "html.parser")
        winner_dict = get_winner( soup_html )
        log.debug( "winner determine: %s", str(winner_dict)[:100]  )


        r = tuple(TCSGame.get_csgame(game.m_id).values())

        rtemp = r

        params = ( r[0], r[-1], winner_dict['t1name'], winner_dict['t2name'] )
        

        _league = soup_html.select_one(".bm-champpic-text")
        league = _league.text if _league else ""

        ts_snapshots = [ msnapshot( **x )  for x in TSnapshot.get_collection( game.m_id )]
        
        dparams = {
            "m_id" : r[0],
            "m_time" : r[-1],
            "team01" : winner_dict['t1name'],
            "team02" : winner_dict['t2name'],
            "league" : league,
            "ts_snapshots" : ts_snapshots,
            "winner_dict" : winner_dict,
        }
        
        # send
        IParams(**dparams)
        build_srv.send( dparams )
        # update
        TSnapshot.snapshot_del(game.m_id)
        TMStatus.csgame_processed(game.m_id)
    build_srv.close()
    log.debug("============End func============")


if __name__ == '__main__':
    pass