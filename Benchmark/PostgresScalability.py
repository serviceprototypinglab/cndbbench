import threading
import sys
from Postgres import Postgres
from time import time
from time import sleep
from SelectTest import SelectTest
import json
with open('config.json') as data_file:
    data = json.load(data_file)

reload(sys)
sys.setdefaultencoding('utf8')

string_connect_postgres_scalability = data['string_connect_postgres_scalability']


def select_test_1(times):
    p = Postgres()
    conn1 = p.create_connexion(None, None, None, None, string_connect_postgres_scalability)
    s1 = SelectTest()
    cursor1 = conn1.cursor()
    time_start = time()
    collection = data['collections']
    value_eq = data['value_eq']
    value_neq = data['value_neq']
    value_many = data['value_many']
    value_contains = data['value_contains']
    s1.aux_selects_sql(cursor1, collection, value_eq, value_neq, value_many, value_contains)
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


# threads_select_test()
