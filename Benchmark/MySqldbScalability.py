import threading
import sys
from Mysqldb import Mysqldb
from time import time
from time import sleep
import json
from SelectTest import SelectTest

reload(sys)
sys.setdefaultencoding('utf8')

with open('config.json') as data_file:
    data = json.load(data_file)


user = data['user']
password = data['password']
host = 'localhost'
database = data['database']


def select_test_1(times):
    mysql = Mysqldb()
    conn1 = mysql.create_connexion(user=user,
                                   password=password,
                                   host=host,
                                   database=database,
                                   string_connect="")
    s1 = SelectTest()
    cursor1 = conn1.cursor()
    time_start = time()
    s1.aux_selects_sql(cursor1)
    time_end = time()
    res_t = time_end - time_start
    times.append(res_t)
    if len(times) == 100:
        sleep(5)
        print times
    conn1.commit()
    conn1.close()


def threads_select_test():
    threads = []
    times = []
    for i in range(0, 100):
        t = threading.Thread(target=select_test_1, args=(times,))
        threads.append(t)
    for t in threads:
        t.start()
    print "-------"


# coll = ["IDXProperty76", "IDXProperty78", "PropertyTypes", "BlobStore", "IDXProperty79", "PropertyTypes"]
# i = InsertTest(coll, coll)
# i.insert_mysql('localhost',user,password,database,False)
# threads_select_test()
s = SelectTest()
# s.selects_mysql('localhost', 100)
