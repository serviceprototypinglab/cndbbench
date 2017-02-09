import httplib

import couchdb
from DocumentDb import DocumentDb


class Couch(DocumentDb):
    def create_connexion(self, host, port):
        # return couchdb.Server('http://172.17.0.1:5984/')
        if host:
            return couchdb.Server(host)
        else:
            return couchdb.Server()
        # return httplib.HTTPConnection('localhost', 5984)

    def close_connexion(self, connexion, name):
        return connexion.delete(name)

    def create_database(self, connexion, db_name):
        return connexion.create(db_name)

    def delete_database(self, connexion, db_name):
        del connexion[db_name]

    def create_collection(self, database, collection_name):
        return database.create(collection_name)

    def delete_collection(self, collection):
        return collection.remove()

    def insert_all_data(self, collection, json_data):
        collection.update(json_data)

    def insert_one_data(self, collection, json_data):
        collection.save(json_data)

    def get_all_data(self, collection):
        mapfn = """function(doc)
        {
            emit(doc, null);
        }"""
        return collection.query(mapfn)

    def get_one_data(self, collection):
        mapfn = """function(doc)
        {
        var count = 0;
        if (count == 0){
            emit(doc, null);
            count = count + 1;
            }
        }"""
        return collection.query(mapfn)

    def get_size(self, collection):
        return collection.info()

    def get_stats(self, db):
        return db.stats()
