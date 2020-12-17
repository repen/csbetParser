#!/home/repente/prog/python/project-edu/css_debuger/venv/bin/python
import unittest, json
from tools import sheduler
from datetime import datetime
# import sys
# sys.stderr = open("test_result/load_page_result.txt", 'a')


class TestToolsFunc(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass
    
    def setUp(self):
        pass


    def test_sheduler(self):
        first_time = datetime(2020, 1, 23, 12, 00)
        last_time  = datetime(2020, 1, 23, 22, 00)
        m_time = last_time.timestamp()

        sh   = sheduler(m_time, first_time)
        new  = [datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S") for x in sh]
        data = [
            '2020-01-23 13:00:00', '2020-01-23 14:00:00', '2020-01-23 15:00:00', 
            '2020-01-23 16:00:00', '2020-01-23 16:30:00', '2020-01-23 17:00:00', 
            '2020-01-23 17:30:00', '2020-01-23 18:00:00', '2020-01-23 18:30:00', 
            '2020-01-23 19:00:00', '2020-01-23 19:30:00', '2020-01-23 20:00:00', 
            '2020-01-23 20:30:00', '2020-01-23 21:00:00', '2020-01-23 21:10:00', 
            '2020-01-23 21:20:00', '2020-01-23 21:30:00', '2020-01-23 21:35:00', 
            '2020-01-23 21:40:00', '2020-01-23 21:45:00', '2020-01-23 21:50:00', 
            '2020-01-23 21:51:00', '2020-01-23 21:52:00', '2020-01-23 21:53:00', 
            '2020-01-23 21:54:00', '2020-01-23 21:55:00', '2020-01-23 21:56:00', 
            '2020-01-23 21:57:00', '2020-01-23 21:58:00', '2020-01-23 21:59:00', 
            '2020-01-24 04:59:00',
        ]
        self.assertEqual(new, data)
        # print(new)

    def test_start_cron(self):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestToolsFunc('test_sheduler'))
    return suite


if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    runner.run(suite())