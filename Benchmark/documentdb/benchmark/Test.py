import pymongo
from InsertTest import InsertTest
from SelectTest import SelectTest
import ssl
import json

with open('config.json') as data_file:
    data = json.load(data_file)


def i_documentdb():
    coll = data['collections']
    i = InsertTest(coll, coll)
    uri3 = data['uri']
    client = pymongo.MongoClient(uri3)
    dbname = data['dbname_documentdb']
    i.insert_mongo(None, None, dbname, False, client, None, None, None)


def s_documentdb():
    uri3 = data['uri']
    client = pymongo.MongoClient(uri3)
    value_eq = data['value_eq']
    value_neq = data['value_neq']
    value_many = data['value_many']
    value_contains = data['value_contains']
    collection = data['collections']
    s = SelectTest()
    s.selects_mongo(None, None, client, 'selects_test_documentdb',
                    100, 'arkis', collection,
                    value_eq, value_neq, value_many,
                    value_contains)

test_name = data['test_name']
if test_name == 'inserts_documentdb':
    print("start i_documentdb")
    i_documentdb()
    print("end i_documentdb")
else:
    print("start s_documentdb")
    s_documentdb()
    print("end s_documentdb")
