import re, pickle, redis, time
from Model import Snapshot, CSGame, MStatus
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from tools import listdir_fullpath, hash_
from Globals import WORK_DIR, REDIS_HOST, REDIS_PORT
from tools import log
from collections import namedtuple

logBuild = log("objbuild", "/logs/build-log.log")

Redis = redis.StrictRedis( host=REDIS_HOST, port=REDIS_PORT, db=0 )
print(Redis)

PATH_OBJECT = WORK_DIR + "/data/objects"
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


    soup = BeautifulSoup(html, "html.parser")
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


Market = namedtuple( 'Market', ['name', 'left', 'right', 'winner', 'time_snapshot'] )

def get_fields_snapshot(html, winner, t_snapshot):
    soup = BeautifulSoup(html, "html.parser")
    markets = soup.select("div.bet-events__item")

    M = {}
    names = []
    name_market = "Main"
    left  = soup.select_one(".sys-stat-abs-1").text
    right = soup.select_one(".sys-stat-abs-2").text
    
    param = ( 
        name_market, hand_num( left ) , hand_num( right ), winner[ name_market ], t_snapshot,
    )
    
    names.append( name_market )
    M[name_market] = Market( *param )

    for market in markets:
        name_market = market.select_one(".bet-event__text-inside-part").text.strip()
        left = market.select_one(".bet-currency.bet-currency_RUB.sys-stat-abs-1").text
        right = market.select_one(".bet-currency.bet-currency_RUB.sys-stat-abs-2").text

        param = ( 
            name_market, hand_num( left ) , hand_num( right ), winner[ name_market ], t_snapshot,
        )

        names.append( name_market )
        # M.append( Market( *param ) )
        M[name_market] = Market( *param )
        # {"name_market" : Market}

    return M, names


class Fixture:
    def __init__(self,*args):
        self.qid    = args[0]
        self.m_id   = args[1]
        self.m_time = args[2]
        self.team01 = args[3]
        self.team02 = args[4]
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


def object_building(**kwargs):
    logBuild.debug("Start func")

    time_difference = int( datetime.now().timestamp() - ( datetime.now() - timedelta( seconds = 60 * 60 * 5 ) ).timestamp() )
    # time_difference = int( datetime.now().timestamp() - ( datetime.now() - timedelta( seconds = 60  ) ).timestamp() )
    query05 = MStatus.select().where( MStatus.m_status == 1)
    game_happened = [ x for x in query05.namedtuples() ]
    logBuild.debug("Quantity fixtures for handling: {} ( all )".format(len( game_happened )))

    game_happened = list( filter(
        lambda x :  datetime.fromtimestamp( int( x.m_time ) ).timestamp() + time_difference < datetime.now().timestamp(), game_happened )
    )
    logBuild.debug("Quantity fixtures for handling: {} ( filter time )".format(len( game_happened )))

    # i think what this excess =====
    objList = listdir_fullpath(PATH_OBJECT)
    game_happened = list( filter( lambda x: PATH_OBJECT + "/" + hash_( x.m_id ) not in objList, game_happened) )
    logBuild.debug("Quantity fixtures for handling: {} ( filter already )".format(len( game_happened )))
    # =====
    
    t_url = "https://betscsgo.cc/match/{}/"

    for game in game_happened:

        query02 = Snapshot.select().where(Snapshot.m_id == game.m_id)
        if not query02:
            logBuild.debug( "Fixture {} is not in Shanpshot.db".format( game.m_id) )
            continue

        data = { "m_id" : game.m_id, "url" : t_url.format( game.m_id )  }
        Redis.set("get_html", pickle.dumps( data ))
        logBuild.debug( "Request {}".format( t_url.format( game.m_id ) ) )

        qn = 1500
        while True:
            response = Redis.get( game.m_id )
            if response:
                rdata = pickle.loads( response )
                print("response done")
                logBuild.debug( "response done {}".format( t_url.format( game.m_id ) ) )
                break
            if not qn:
                logBuild.debug( "response error {}".format( t_url.format( game.m_id ) ) )
                break
            qn = qn - 1
            time.sleep(0.2)
        
        Redis.delete( game.m_id )
        if not qn:continue
        
        try:
            winner_dict = get_winner( rdata["html"] )
            logBuild.debug( "winner determine: {}".format( winner_dict )  )
            print( winner_dict )
        except ( AssertionError, NotElementErr ) as Err:
            print("= Error", Err)
            logBuild.debug( "winner determine: Error"  )
            continue 

        # =====
        try:
            query01 = CSGame.select().where( CSGame.m_id == game.m_id )
            r = query01.tuples()[0]


            params = ( *r[:3], winner_dict['t1name'], winner_dict['t2name'] )
            fixture = Fixture( *params )

            logBuild.debug( "Object create {}".format( game.m_id )   )
            
            for snapshot in query02.namedtuples():
                html = snapshot.m_snapshot.decode('unicode-escape')
                markets, names = get_fields_snapshot( html, winner_dict, snapshot.m_time_snapshot )
                fixture.name_markets = names
                fixture.markets = markets

            # Andrey will add last snapshot
            fixture.markets = extract_last_snapshot( winner_dict )
            # It's ok. I done.
            
            logBuild.debug( "Object build"  )
            
            with open("{}/{}".format(PATH_OBJECT, hash_( r[1] ) ), "wb") as f:
                pickle.dump( fixture, f )
            
            logBuild.debug( "Object done {}".format(  game.m_id )  )
            print(game.m_id, "Id")

            # Snapshot.delete_snapshot( game.m_id )
            # MStatus.delete().where(  MStatus.m_id == game.m_id  ).execute()
            Snapshot.delete().where( Snapshot.m_id == game.m_id ).execute()
            MStatus.update({
                "m_status" : 0,
            }).where(  MStatus.m_id == game.m_id  ).execute()

            logBuild.debug( "Snapshot deleted from database".format( game.m_id  )  )
            print("Snapshot.delete")
        except Exception as e:
            print("= Error", e)
            logBuild.error( "Error {}".format( game.m_id  ), exc_info=True  )
            continue
        # =====
        print('============')
    logBuild.debug("============End func============")
    print("END")

if __name__ == '__main__':
    object_building()
    pass