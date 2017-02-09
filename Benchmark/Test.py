import pymongo
from Mongo import Mongo
import json


class Test:
    def __init__(self):
        print "Init test"


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

    def i_coll_database_mongo(self, coll, db_name, users):
        with open('config.json') as data_file:
            data = json.load(data_file)
        mongo = Mongo()
        host = data['host_multitenant']
        port = int(data['port_multitenant'])
        db_name = 'arkis'
        number_tenants = data['number_tenants']
        if users:
            conn = mongo.create_connexion(host, port - 1)
            db = mongo.create_database(conn, db_name)
            json_data = self.read_data('sharedData', 'arkisEUsers')
            mongo.insert_all_data(db['users'], json_data)
        else:
            for user in range(0, number_tenants):
                print user
                conn = mongo.create_connexion(host, port + user)
                db = mongo.create_database(conn, db_name)
                # for coll in self.collections:
                # json_data = self.read_data('sharedData', coll)
                json_data = self.read_data('sharedData', 'arkisE_E_user_' + str(user))
                mongo.insert_all_data(db['documents'], json_data)

    def s_mongo(self):
        print "aaaaaaa"

    def create_full_index(self):
        with open('config.json') as data_file:
            data = json.load(data_file)
        mongo = Mongo()
        host = data['host_multitenant']
        port = int(data['port_multitenant'])
        db_name = 'arkis'
        number_tenants = data['number_tenants']

        for user in range(0, number_tenants):
            print user
            conn = mongo.create_connexion(host, port + user)
            conn.arkis.documents.create_index([('blob', pymongo.TEXT)])
            print "created full text index"


t = Test()
with open('config.json') as data_file:
    data = json.load(data_file)
    test_name = data['test_name']

if test_name == 'select_mongo':
    t.s_mongo()
elif test_name == 'insert_mongo':
    print test_name