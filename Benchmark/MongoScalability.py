from pymongo import MongoClient
from time import time
import threading
from InsertTest import InsertTest
from Mongo import Mongo
from SelectTest import SelectTest
import json

with open('config.json') as data_file:
    data = json.load(data_file)


host_mongo_1 = data['host_mongo_1']
host_mongo_2 = data['host_mongo_2']
host_mongo_3 = data['host_mongo_3']
host_mongo_4 = data['host_mongo_4']
coll = data['coll']
host_no__mongo_cluster = data['host_no__mongo_cluster']
port_no__mongo_cluster = data['port_no__mongo_cluster']
mongo_replica_set = data['mongo_replica_set']


def connect_cluster():
    return MongoClient(host=[host_mongo_1, host_mongo_2, host_mongo_3, host_mongo_4],
                       replicaset=mongo_replica_set)
    # readPreference='secondaryPreferred')


def insert_cluster(c):
    one = False
    name_file = 'insert_mongo_cluster'
    db = data['database']
    aux = InsertTest(coll, coll)
    aux.insert_mongo(None, None, db, one, c, name_file)


def select_test_1(s1, db, times):
    time_start = time()
    s1.aux_selects_mongo(db)
    time_end = time()
    res_t = time_end - time_start
    times.append(res_t)
    if len(times) == 100:
        print times


def threads_select_test(se, db):
    threads = []
    times = []
    for i in range(0, 100):
        t = threading.Thread(target=select_test_1, args=(se, db, times))
        threads.append(t)
    for t in threads:
        t.start()
    print "-------"


def select_test_thread_cluster():

    s = SelectTest()
    c = connect_cluster()
    # print c.read_preference
    db1 = c.arkis
    threads_select_test(s, db1)


def select_test_thread_host():
    s = SelectTest()
    m = Mongo()
    c = m.create_connexion(host_no__mongo_cluster, port_no__mongo_cluster)
    db1 = c.arkis
    threads_select_test(s, db1)

# select_test_thread_host()
# select_test_thread_cluster()