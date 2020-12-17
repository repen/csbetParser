# https://stackabuse.com/scheduling-jobs-with-python-crontab/
from tools import log
from datetime import datetime
from bs4 import BeautifulSoup


# This selector needs check on website
CSS_SELECTOR = ".sys-games-next .bet-item.sys-betting.bet_coming[data-id]"


class Match:
    def __init__(self, id_match, m_time):
        self.id_match = id_match
        self.m_time   = int(m_time)
        self.parent   = []
        self.child    = []

    def to_dict(self):
        return {
            "time_snapshot" : datetime.now().strftime("%s"),
            "parent": self.parent,
            "child" : self.child,
        }


def extract_data(html):

    data = []
    soup = BeautifulSoup(html, "html.parser")
    fixtures = soup.select( CSS_SELECTOR )
    for fixture in fixtures:

        t1 = fixture.select_one(".sys-t1name")
        t2 = fixture.select_one(".sys-t2name")
        m_time = fixture.select_one( "div[data-timestamp]" )

        d_fixture = {
            "m_id"   : fixture['data-id'] if fixture.has_attr('data-id') else "unknown",
            "t1name" : t1.text.strip() if t1 else "unknown",
            "t2name" : t2.text.strip() if t2 else "unknown",
            "m_time" : m_time['data-timestamp'] if m_time.has_attr('data-timestamp') else "unknown",
        }
        data.append( d_fixture )
    
    return data


def main(*args):
    # time_snapshot = args[0]
    html = args[1]
    data = extract_data( html )

    return data



if __name__ == '__main__':
    res = main()
    print(res)