import threading
from Couch import Couch
from time import time
import json

with open('config.json') as data_file:
    data = json.load(data_file)

host_couch_scalability = data['host_couch_scalability']
coll = data['coll']


def insert_cluster():
    if data['arkisdata'] == 'true':
        from InsertTestArkisData import InsertTest
        from SelectTestArkisData import SelectTest
    else:
        from InsertTest import InsertTest
        from SelectTest import SelectTest
    aux = InsertTest(coll, coll)
    aux.insert_couch(host_couch_scalability, False)


def select_test_1(s1, conn, times):
    time_start = time()
    s1.aux_selects_couch(conn)
    time_end = time()
    res_t = time_end - time_start
    times.append(res_t)
    if len(times) == 100:
        print times


def threads_select_test(se, conn):
    threads = []
    times = []
    for i in range(0, 100):
        t = threading.Thread(target=select_test_1, args=(se, conn, times))
        threads.append(t)
    for t in threads:
        t.start()
    print "-------"


def select_test_thread_cluster():
    if data['arkisdata'] == 'true':
        from InsertTestArkisData import InsertTest
        from SelectTestArkisData import SelectTest
    else:
        from InsertTest import InsertTest
        from SelectTest import SelectTest
    s = SelectTest()
    couch = Couch()
    conn = couch.create_connexion(host_couch_scalability, None)
    # print c.read_preference
    threads_select_test(s, conn)


# insert_cluster()
# select_test_thread_cluster()
# s = SelectTest()
# s.selects_couch(host, 100)
