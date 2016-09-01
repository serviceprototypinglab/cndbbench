import sys
from Crate import Crate
from Mongo import Mongo
from Mysqldb import Mysqldb
from Postgres import Postgres
from time import time
import json
reload(sys)
sys.setdefaultencoding('utf8')

with open('config.json') as data_file:
    data = json.load(data_file)

host_mongo_join = data['host_mongo_join']
port_mongo_join = data['port_mongo_join']
host_crate_join = data['host_crate_join']
string_connect_postgres_join = data['string_connect_postgres_join']
host_mysql_join = data['host_mysql_join']
user_mysql_join = data['user_mysql_join']
database_mysql_join = data['database_mysql_join']
password_mysql_join = data['password_mysql_join']
sql_joins_query = data['sql_joins_query']


def sql_joins(db):
    if db == 'postgres':
        database = Postgres()
        conn = database.create_connexion(user='user',
                                         password='password',
                                         host='localhost',
                                         database='database',
                                         string_connect=string_connect_postgres_join)
    elif db == 'crate':
        database = Crate()
        conn = database.create_connexion(user='',
                                         password='',
                                         host='',
                                         database='',
                                         string_connect=host_crate_join)
    else:
        database = Mysqldb()
        conn = database.create_connexion(user=user_mysql_join,
                                         password=password_mysql_join,
                                         host=host_mysql_join,
                                         database=database_mysql_join,
                                         string_connect="")
    cursor = conn.cursor()

    times_sql_joins = []
    for i in range(0, 100):
        time1 = time()
        cursor.execute(sql_joins_query)
        results = cursor.fetchall()
        for r in results:
            pass
        time2 = time()
        aux_t = time2 - time1
        print i
        print aux_t
        times_sql_joins.append(aux_t)
    print times_sql_joins


def mongo_joins():
    mongo = Mongo()
    conn = mongo.create_connexion(host_mongo_join, port_mongo_join)
    print "connected"
    db = conn.dbexample
    times_sql_joins = []
    for i in range(0, 100):
        time1 = time()
        results = db['big_json'].find({'$or': [{"ZRowIdent": 3530}, {"ZRowIdent": 34}]})
        for r in results:
            pass
        time2 = time()
        aux_t = time2 - time1
        print i
        print aux_t
        times_sql_joins.append(aux_t)

    conn.close()
    print times_sql_joins
