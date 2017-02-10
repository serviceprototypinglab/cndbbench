import pymongo
from Mongo import Mongo
from InsertTest import InsertTest
from SelectTest import SelectTest
import json
import ResilienceTest


class Test:
    def __init__(self):
        print "Init test"
        with open('config.json') as data_file1:
            data1 = json.load(data_file1)
        self.data = data1

    @staticmethod
    def read_data(folder_name, name):
        try:
            with open("/" + folder_name + "/" + name + ".json") as json_file:
                json_data = json.load(json_file)
        except Exception, e1:
            try:
                with open("../" + folder_name + "/" + name + ".json") as json_file:
                    json_data = json.load(json_file)
            except Exception, e:
                print e1
                print e
                print "Error reading 2 " + name
                json_data = []
        return json_data

    @staticmethod
    def create_full_index():
        with open('config.json') as data_file1:
            data1 = json.load(data_file1)
        mongo = Mongo()
        host = data1['host_multitenant']
        port = int(data1['port_multitenant'])
        db_name = 'arkis'
        number_tenants = data1['number_tenants']

        for user in range(0, number_tenants):
            print user
            conn = mongo.create_connexion(host, port + user)
            conn.arkis.documents.create_index([('blob', pymongo.TEXT)])
            print "created full text index"

    def select_mongo(self):
        host = self.data['host_mongo']
        port = self.data['port_mongo']
        conn = None
        name_file = 'select_mongo'
        number_loops = self.data['number_loops']
        db = self.data['database']
        collection = self.data['collections']
        value_eq = self.data['value_eq']
        value_neq = self.data['value_neq']
        value_many = self.data['value_many']
        value_contains = self.data['value_contains']
        s = SelectTest()
        s.selects_mongo(host, port, conn, name_file,
                        number_loops, db, collection,
                        value_eq, value_neq, value_many,
                        value_contains)

    def insert_mongo_one(self):
        collection = self.data['collections']
        i = InsertTest(collection, collection)
        one = True
        host = self.data['host_mongo']
        port = self.data['port_mongo']
        conn = None
        name_file = 'insert_mongo_one'
        db = self.data['database']
        i.insert_mongo(host, port, db, one, conn, name_file)

    def insert_mongo(self):
        collection = self.data['collections']
        i = InsertTest(collection, collection)
        one = False
        host = self.data['host_mongo']
        port = self.data['port_mongo']
        conn = None
        name_file = 'insert_mongo'
        db = 'arkis'
        i.insert_mongo(host, port, db, one, conn, name_file)

    def select_couch(self):
        host = self.data['host_couch']
        name_file = 'select_couch'
        number_loops = self.data['number_loops']
        collection = self.data['collections']
        value_eq = self.data['value_eq']
        value_neq = self.data['value_neq']
        value_many = self.data['value_many']
        value_contains = self.data['value_contains']
        s = SelectTest()
        s.selects_couch(host, number_loops, collection, value_eq, value_neq, value_many,
                        value_contains, name_file)

    def insert_couch_one(self):
        collection = self.data['collections']
        i = InsertTest(collection, collection)
        one = True
        host = self.data['host_couch']
        db = self.data['database']
        i.insert_couch(host, db, one, 'insert_couch_one')

    def insert_couch(self):
        collection = self.data['collections']
        i = InsertTest(collection, collection)
        one = False
        host = self.data['host_couch']
        db = self.data['database']
        i.insert_couch(host, db, one, 'insert_couch')

    def select_crate(self):
        host = self.data['host_crate']
        port = self.data['port_crate']
        name_file = 'select_crate'
        number_loops = self.data['number_loops']
        collection = self.data['collections']
        value_eq = self.data['value_eq']
        value_neq = self.data['value_neq']
        value_many = self.data['value_many']
        value_contains = self.data['value_contains']
        s = SelectTest()
        s.selects_crate(host, port, name_file, number_loops, collection, value_eq, value_neq, value_many,
                        value_contains)

    def insert_crate_one(self):
        collection = self.data['collections']
        i = InsertTest(collection, collection)
        one = True
        host = self.data['host_crate']
        name_file = 'insert_crate_one'
        i.insert_crate(host, one, name_file)

    def insert_crate(self):
        collection = self.data['collections']
        i = InsertTest(collection, collection)
        one = False
        host = self.data['host_crate']
        name_file = 'insert_crate'
        i.insert_crate(host, one, name_file)

    def select_postgres(self):
        host = self.data['host_postgres']
        port = self.data['port_postgres']
        name_file = 'select_postgres'
        number_loops = self.data['number_loops']
        collection = self.data['collections']
        value_eq = self.data['value_eq']
        value_neq = self.data['value_neq']
        value_many = self.data['value_many']
        value_contains = self.data['value_contains']
        s = SelectTest()
        s.selects_postgres(host, port, name_file, number_loops, collection, value_eq, value_neq, value_many,
                           value_contains)

    def insert_postgres_one(self):
        collection = self.data['collections']
        i = InsertTest(collection, collection)
        one = True
        string_connect_postgres = self.data['string_connect_postgres']
        name_file = 'insert_postgres_one'
        i.insert_postgres(string_connect_postgres, one, name_file)

    def insert_postgres(self):
        collection = self.data['collections']
        i = InsertTest(collection, collection)
        one = False
        string_connect_postgres = self.data['string_connect_postgres']
        name_file = 'insert_postgres'
        i.insert_postgres(string_connect_postgres, one, name_file)

    def select_mysql(self):
        host = self.data['host_mysql']
        user = self.data['user_mysql']
        password = self.data['pass_mysql']
        port = self.data['port_mysql']
        dbname = self.data['dbname_mysql']
        name_file = 'select_mysql'
        number_loops = self.data['number_loops']
        collection = self.data['collections']
        value_eq = self.data['value_eq']
        value_neq = self.data['value_neq']
        value_many = self.data['value_many']
        value_contains = self.data['value_contains']
        s = SelectTest()
        s.selects_mysql(host, port, user, password, dbname, name_file, number_loops,
                        collection, value_eq, value_neq, value_many, value_contains)

    def insert_mysql_one(self):
        collection = self.data['collections']
        i = InsertTest(collection, collection)
        one = True
        host = self.data['host_mysql']
        user = self.data['user_mysql']
        password = self.data['pass_mysql']
        dbname = self.data['dbname_mysql']
        name_file = 'insert_mysql_one'
        i.insert_mysql(host, user, password, dbname, one, name_file)

    def insert_mysql(self):
        collection = self.data['collections']
        i = InsertTest(collection, collection)
        one = False
        host = self.data['host_mysql']
        user = self.data['user_mysql']
        password = self.data['pass_mysql']
        dbname = self.data['dbname_mysql']
        name_file = 'insert_mysql'
        i.insert_mysql(host, user, password, dbname, one, name_file)


t = Test()
with open('config.json') as data_file:
    data = json.load(data_file)
    test_name = data['test_name']


# Inserts ans selects test
if test_name == 'select_mongo':
    print test_name
    t.select_mongo()
elif test_name == 'insert_mongo':
    print test_name
    t.insert_mongo()
elif test_name == 'insert_mongo_one':
    print test_name
    t.insert_mongo_one()
elif test_name == 'select_couch':
    print test_name
    t.select_couch()
elif test_name == 'insert_couch':
    print test_name
    t.insert_couch()
elif test_name == 'insert_couch_one':
    print test_name
    t.insert_couch_one()
elif test_name == 'select_crate':
    print test_name
    t.select_crate()
elif test_name == 'insert_crate':
    print test_name
    t.insert_crate()
elif test_name == 'insert_crate_one':
    print test_name
    t.insert_crate_one()
elif test_name == 'select_postgres':
    print test_name
    t.select_postgres()
elif test_name == 'insert_postgres':
    print test_name
    t.insert_postgres()
elif test_name == 'insert_postgres_one':
    print test_name
    t.insert_postgres_one()
elif test_name == 'select_mysql':
    print test_name
    t.select_mysql()
elif test_name == 'insert_mysql':
    print test_name
    t.insert_mysql()
elif test_name == 'insert_mysql_one':
    print test_name
    t.insert_mysql_one()
# Scalability tests
elif test_name == 'scalability_mongo':
    print test_name
elif test_name == 'scalability_couch':
    print test_name
elif test_name == 'scalability_crate':
    print test_name
elif test_name == 'scalability_postgres':
    print test_name
elif test_name == 'scalability_mysql':
    print test_name
# MultiTenant tests
elif test_name == 'mt_mongo':
    print test_name
elif test_name == 'mt_couch':
    print test_name
elif test_name == 'mt_crate':
    print test_name
elif test_name == 'mt_postgres':
    print test_name
elif test_name == 'mt_mysql':
    print test_name

