from pymongo import MongoClient
from DocumentDb import DocumentDb


class Mongo(DocumentDb):

    def create_connexion(self, host, port, user=None, pwd=None):
        if user and pwd:
            url = "mongodb://%s:%s@%s:%i" % (user, pwd, host, port)
            client = MongoClient(url, ssl=True)
            return client

        # return MongoClient('mongo', 27017)
        if port:
            return MongoClient(host, port, ssl=True)
        else:
            return MongoClient(host)

    def close_connexion(self, connexion, name):
        return connexion.close()

    def create_database(self, connexion, db_name):
        return connexion[db_name]

    def create_collection(self, database, collection_name):
        return database[collection_name]

    def delete_collection(self, collection):
        return collection.remove()

    def insert_all_data(self,  collection, json_data):
        collection.insert_many(json_data)

    def insert_one_data(self, collection, json_data):
        collection.insert_one(json_data)

    def get_all_data(self, table):
        return table.find({})

    def get_one_data(self, table):
        return table.find_one()

    def get_size(self, db):
        return db.command("dbstats")['dataSize']

    def get_stats(self, db):
        return db.command("dbstats")
