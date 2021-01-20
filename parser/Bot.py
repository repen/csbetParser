# import redis
import Model
# from Globals import REDIS_HOST, REDIS_PORT
from datetime import datetime
from tools import log as _log


class ErrorMatchGoneUpcoming( Exception ):
    pass

class SnapshotData:
    __slots__ = ('m_id', 'data', 'timestamp', 'm_status')

    def __init__(self, m_id, data, ms_time):
        self.data = data
        self.m_id = m_id
        self.timestamp = ms_time
        self.m_status = 0

    def to_dict(self):
        return {
            "m_id" : self.m_id,
            "m_time_snapshot": self.timestamp,
            "m_snapshot" : self.data,
            "m_status"   : self.m_status,
        }

class Bot:
    __slots__ = ('m_id', 'm_time', 'log' ,'COUNTER_ERROR', "Live")

    def __init__(self, m_id, m_time):
        self.m_id    = m_id
        self.m_time  = m_time
        self.log  = _log( self.m_id, "{}.log".format(self.m_id) )
        self.COUNTER_ERROR = 0
        self.Live = False


    def extract_fixture(self, soup):
        '''extract html code from page https://betscsgo.me/ for self.m_id'''
        fixture = False
        css_selector = '.bet-item.sys-betting.bet_coming[data-id="{}"]'.format( self.m_id )
        element      = soup.select_one(css_selector)

        CSS_LIVE = '.bet-item.sys-betting.bet-now[data-id="{}"]'.format( self.m_id )
        match_get_live = soup.select_one( CSS_LIVE )

        if not element:
            self.log.debug("Data extract_fixture no data" )
            self.COUNTER_ERROR += 1
        else:
            fixture = str( element )
            self.COUNTER_ERROR = 0
            self.log.debug("Data extract_fixture len: {}".format( len( fixture ) ) )

        if match_get_live:
            result = Model.MStatus.insert_safe({
                 "m_id" : self.m_id,
                 "m_status" : 1,
                 "m_time" : datetime.now().timestamp(),
            })
            self.Live = result
            self.log.debug("Fixture is live {}".format( str( result ) ) )

        return fixture


    def write_snapshot( self, time_snapshot, data ):
        data = SnapshotData( self.m_id, data, time_snapshot )

        self.log.debug( "Data write on database Snapshot" )
        
        # Model.Snapshot.insert( data.to_dict() ).execute()
        result = Model.Snapshot.insert_safe( data.to_dict() )

        self.log.debug( "Data write successfully {}".format( str(result) ) )


    def main(self, *args):
        time_snapshot = args[0]
        soup = args[1]
        code = 200
        data = self.extract_fixture( soup )

        if data:
            self.log.debug("Data extract done. Snapshot write")
            self.write_snapshot( time_snapshot, data )
            self.log.debug("record done")
        else:
            self.log.debug( "Not data" )
        
        if self.COUNTER_ERROR == 60:
            code = 402
        
        if self.Live:
            code = 401
        
        return code
