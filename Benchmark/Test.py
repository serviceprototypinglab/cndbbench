# ADD HERE THE TEST THAT YOU WANT DO
from InsertTest import InsertTest
from SelectTest import SelectTest


def insert_bluemix_mysql():
    coll = ['example']
    i = InsertTest(coll, coll)
    host = "us-cdbr-iron-east-04.cleardb.net"
    password = "f87a3844"
    username = "b88cd1fb83f44d"
    dbname = 'ad_6e5db755ddea001'
    i.insert_mysql(host, username, password, dbname, False)


def insert_bluemix_postgres():
    coll = ['example']
    i = InsertTest(coll, coll)
    string_connect = "host='qdjjtnkv.db.elephantsql.com' port='5432' user='jhyngeej'" \
                     " password='GwI1AjVT7XBInW0WY2g_IjMXUujfXgdX' dbname='jhyngeej'"
    i.insert_postgres(string_connect, False)


def selects_bluemix_postgres():
    s = SelectTest()
    string_connect = "host='qdjjtnkv.db.elephantsql.com' port='5432' user='jhyngeej'" \
                     " password='GwI1AjVT7XBInW0WY2g_IjMXUujfXgdX' dbname='jhyngeej'"
    s.selects_postgres(string_connect, 100)


def selects_bluemix_mysql():
    s = SelectTest()
    host = "us-cdbr-iron-east-04.cleardb.net"
    s.selects_mysql(host, 100)




# selects_bluemix_postgres()
# insert_bluemix_mysql()
selects_bluemix_mysql()
