from datetime import datetime
import re, json, logging, hashlib
from Globals import WORK_DIR, BASE_DIR
import os, copy, pickle


def listdir_fullpath(d):
    return [os.path.join(d, f) for f in os.listdir(d)]


def hash_(string):
    return hashlib.sha1(string.encode()).hexdigest()

def sheduler(*args):
    m_time            = args[0]
    current_time      = args[1]

    current_time_int  = current_time.timestamp()
    shedule = []
    # import pdb; pdb.set_trace()
    while True:
        res = datetime.fromtimestamp(m_time) - current_time
        # print(datetime.fromtimestamp(m_time).strftime("%Y-%m-%d %H:%M:%S"))

        if 4320 * 60  >= res.total_seconds() > 1440*60:
            m_time = m_time - 7200
            current_time_int = current_time_int + 7200
            shedule.append(current_time_int)
            continue
        elif 1440*60  >= res.total_seconds() > 360 * 60:
            m_time = m_time - 3600
            current_time_int = current_time_int + 3600
            shedule.append(current_time_int )
            continue
        elif 360*60 >= res.total_seconds() > 60 * 60:
            m_time = m_time - 1800
            current_time_int = current_time_int + 1800
            shedule.append(current_time_int)
            continue
        elif 60*60 >= res.total_seconds() > 30 * 60:
            m_time = m_time - 600
            current_time_int = current_time_int + 600
            shedule.append(current_time_int)
            continue
        elif 30*60 >= res.total_seconds() > 10 * 60:
            m_time = m_time - 300
            current_time_int = current_time_int + 300
            shedule.append(current_time_int)
            continue
        elif 10*60 >= res.total_seconds() > 60:
            m_time = m_time - 60
            current_time_int = current_time_int + 60
            shedule.append(current_time_int)
            continue
        else:
            break
    if len(shedule) > 10: 
        first = shedule[-1]
        additional = []
        for _ in range(1):
            first += 25200
            additional.append(first)
        shedule.extend(additional)

    return shedule


def log(name, filename=None):
    # создаём logger
    logger = logging.getLogger(name)
    logger.setLevel( logging.DEBUG )

    # создаём консольный handler и задаём уровень
    if filename:
        ch = logging.FileHandler(os.path.join(  BASE_DIR, "logs" , filename ))
    else:
        ch = logging.StreamHandler()

    ch.setLevel(logging.DEBUG)

    # создаём formatter
    formatter = logging.Formatter('%(asctime)s : %(lineno)d : %(name)s : %(levelname)s : %(message)s')
    # %(lineno)d :
    # добавляем formatter в ch
    ch.setFormatter(formatter)

    # добавляем ch к logger
    logger.addHandler(ch)

    # logger.debug('debug message')
    # logger.info('info message')
    # logger.warn('warn message')
    # logger.error('error message')
    # logger.critical('critical message')
    return logger

class Handler:

    @staticmethod
    def get_data_fixtures(html):
        res = re.search(r"_bets.populateBets.*?;", html)
        if res:
            data_json = res.group()
            data_json = data_json[data_json.index("["):-2]
            return json.loads(data_json)

    @staticmethod
    def load_json(path):
        with open(path, ) as f:
            data = json.load(f)
        return data

    @staticmethod
    def save_json(path, data):
        with open(path, "w") as f:
            json.dump(data, f)
if __name__ == '__main__':
    pass