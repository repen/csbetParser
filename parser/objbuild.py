import re, os, requests, zlib, json
from Model import TMStatus, TSnapshot, TCSGame, finished, zopedb
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from Globals import WORK_DIR, REMOTE_API
from tools import log as _log
from collections import namedtuple
from ydisk import upload_object

log = _log("BUILD")

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
        # self._markets = []
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



def object_building():
    log.debug("Start. -- Build object --")

    time_difference = int( datetime.now().timestamp() - ( datetime.now() - timedelta( seconds = 60 * 60 * 5 ) ).timestamp() )
    # time_difference = int( datetime.now().timestamp() - ( datetime.now() - timedelta( seconds = 60  ) ).timestamp() )

    game_happened = [ mstatus(**x) for x in TMStatus.tmstatus if  x["m_status"] == 1]

    log.debug("Quantity fixtures for handling: {} ( all )".format(len( game_happened )))

    game_happened = list( filter(
        lambda x :  datetime.fromtimestamp( x.m_time ).timestamp() + time_difference < datetime.now().timestamp(), game_happened )
    )
    log.debug("Quantity fixtures for handling: {} ( filter time )".format(len( game_happened )))

    # i think what this excess =====
    # objList = listdir_fullpath(PATH_OBJECT)
    objList = finished.get_all_id()
    game_happened = list(
        filter( lambda x: x.m_id not in objList, game_happened)
    )
    log.debug("Quantity fixtures for handling: {} ( filter already )".format(len( game_happened )))


    for game in game_happened:

        if game.m_id not in TSnapshot.container.keys():
            log.debug( "Fixture {} is not in Shanpshot.db".format( game.m_id) )
            continue


        _html = get_result_page(game.m_id)
        if not bool(_html):
            create_task(game.m_id)
            log.info("Continue: not html %s", bool(_html))
            continue


        # try:
        upload_object(zlib.compress(_html.encode("utf8")), "id_" + str(game.m_id) )
        soup_html = BeautifulSoup(_html, "html.parser")
        winner_dict = get_winner( soup_html )
        log.debug( "winner determine: %s", str(winner_dict)[:100]  )
        # except ( AssertionError, NotElementErr ) as Err:
        #     log.info("= Error", Err)
        #     log.debug( "winner determine: Error"  )
        #     continue


        r = tuple(TCSGame.csgame[game.m_id].values())

        rtemp = r

        params = ( *r[:2], winner_dict['t1name'], winner_dict['t2name'] )
        

        _league = soup_html.select_one(".bm-champpic-text")
        league = _league.text if _league else ""
        fixture = Fixture( *params , league = league.strip())
        # breakpoint()
        log.debug( "Object create {}".format( game.m_id )   )


        decompress = [ msnapshot( **json.loads( zlib.decompress(x) ) )  for x in TSnapshot.container[game.m_id]]
        decompress = sorted( decompress, key=lambda x: x.m_time_snapshot )
        
        for snapshot in decompress:
            html = snapshot.m_snapshot
            markets, names = get_fields_snapshot( BeautifulSoup(html, "html.parser"), winner_dict, snapshot.m_time_snapshot )
            fixture.name_markets = names
            fixture.markets = markets 

        fixture.markets = extract_last_snapshot( winner_dict )

        log.debug( "Object save %s", r[0]  )

        finished.add(fixture._asdict())
        finished.transaction_commit()
        # with open( os.path.join(PATH_OBJECT, r[0] ), "wb") as f:
        #     pickle.dump( fixture, f )

        log.debug( "Object done {}".format(  game.m_id )  )
    log.debug("============End func============")

if __name__ == '__main__':
    object_building()
    pass