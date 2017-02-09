import pymongo
from InsertTest import InsertTest
from SelectTest import SelectTest
import ssl


def i_documentdb():
    print("start")
    coll = ["IDXProperty76"]
    # coll = ["PropertyTypes", "IDXProperty78"]
    # coll = ["IDXProperty79"]
    # coll = ["BlobStore"]
    i = InsertTest(coll, coll)
    uri3 = 'mongodb://azurebenchmark2:rzrPYYssgK1I9puvYbJOWB9eiHXjlcKxt4k3PzEXy4skbj1zcInLQkqbhdt5unykuY4BKRcA7dW' \
           'DRgQS3UKJiw==@azurebenchmark2.documents.azure.com:10250/?ssl=true&ssl_cert_reqs=CERT_NONE'
    uri2 = "mongodb://azurebenchmark:YcxU6CWMY0t8PIUvskJX1cDLcAMwBZx5XU7kjuTHMYKeUDinrpWhFSKRPir5ngq13BPQCcm37JPC" \
           "z8uRlM74OA==@azurebenchmark.documents.azure.com:10250/?ssl=true&ssl_cert_reqs=CERT_NONE"
    client = pymongo.MongoClient(uri3)
    # client = pymongo.MongoClient(uri2, ssl=True, ssl_cert_reqs=ssl.CERT_NONE)
    i.insert_mongo(None, None, 'dbexample', False, client, None, None, None)
    print("inserted")


def s_documentdb():
    s = SelectTest()
    uri3 = 'mongodb://azurebenchmark2:rzrPYYssgK1I9puvYbJOWB9eiHXjlcKxt4k3PzEXy4skbj1zcInLQkqbhdt5unykuY4BKRcA7dW' \
           'DRgQS3UKJiw==@azurebenchmark2.documents.azure.com:10250/?ssl=true&ssl_cert_reqs=CERT_NONE'
    uri2 = 'mongodb://azurebenchmark:YcxU6CWMY0t8PIUvskJX1cDLcAMwBZx5XU7kjuTHMYKeUDinrpWhFSKRPir5ngq13BPQCcm37JPC' \
           'z8uRlM74OA==@azurebenchmark.documents.azure.com:10250/?ssl=true&ssl_cert_reqs=CERT_NONE'
    client = pymongo.MongoClient(uri3)
    s.selects_mongo(None, None, client, "selects_documentdb", 10)


print("start i_documentdb")
#i_documentdb()
print("end i_documentdb")

print("start s_documentdb")
s_documentdb()
print("end s_documentdb")
