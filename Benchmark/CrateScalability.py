import sys
import threading
from Crate import Crate
from InsertTest import InsertTest
from SelectTest import SelectTest
from time import time
import json

reload(sys)
sys.setdefaultencoding('utf8')


with open('config.json') as data_file:
    data = json.load(data_file)

host_cluster_crate_1 = data['host_cluster_crate_1']
host_cluster_crate_2 = data['host_cluster_crate_2']
host_cluster_crate_3 = data['host_cluster_crate_3']
host_cluster_crate_4 = data['host_cluster_crate_4']
host_no_cluster_crate = data['host_no_cluster_crate']
coll = data['coll']


def insert_crate():
    ins = InsertTest(coll, coll)
    ins.insert_crate(host_no_cluster_crate, False)


def select_crate_cluster_parallel_all_nodes():
    s = SelectTest()
    crate = Crate()
    conn = crate.create_connexion(None, None, None, None,
                                  string_connect=[host_cluster_crate_1, host_cluster_crate_2, host_cluster_crate_3,
                                                  host_cluster_crate_4])
    # print conn.client._active_servers
    cursor = conn.cursor()
    threads_select_test(s, cursor)


def select_crate_cluster_sequential():
    s = SelectTest()
    s.selects_crate(host_cluster_crate_1, None, 'name_results', 100)


def select_crate_cluster_parallel_one_node():
    s = SelectTest()
    crate = Crate()
    conn = crate.create_connexion(None, None, None, None, string_connect=host_cluster_crate_2)
    cursor = conn.cursor()
    threads_select_test(s, cursor)


def select_crate_no_cluster_sequential():
    s = SelectTest()
    s.selects_crate(host_no_cluster_crate, None, 'name_results', 100)


def select_crate_no_cluster_parallel():
    s = SelectTest()
    crate = Crate()
    conn = crate.create_connexion(None, None, None, None, string_connect=host_no_cluster_crate)
    cursor = conn.cursor()
    threads_select_test(s, cursor)


def select_test_1(s1, cursor, times):
    time_start = time()
    s1.aux_selects_sql(cursor)
    time_end = time()
    res_t = time_end - time_start
    times.append(res_t)
    if len(times) == 100:
        print times


def threads_select_test(se, cursor):
    threads = []
    times = []
    for i in range(0, 100):
        t = threading.Thread(target=select_test_1, args=(se, cursor, times))
        threads.append(t)
    for t in threads:
        t.start()
    print "-------"

# select_crate_cluster_parallel_primary()
# select_crate_cluster_parallel_one()
# select_crate_cluster_sequential()
# select_crate_cluster_parallel_one()
# select_crate_no_cluster_parallel()
# select_crate_no_cluster_sequential()
