import json
import time


def delete_table(cursor, name):
    query_delete = "DROP TABLE IF EXISTS " + name
    cursor.execute(query_delete)


class SqlDb:

    def __init__(self):
        pass

    def create_connexion(self, user, password, host, database, string_connect):
        pass

    def create_database(self, cursor, dbname):
        string_create_database = 'CREATE DATABASE ' + dbname
        cursor.execute(string_create_database)

    def delete_database(self, connexion):
        pass

    @staticmethod
    def close_connexion(connexion):
        return connexion.close()

    @staticmethod
    def create_table(cursor, query):
        cursor.execute(query)

    @staticmethod
    def insert_all_data(cursor, query, data):
        cursor.executemany(query, data)

    @staticmethod
    def insert_one_data(cursor, query, data):
        cursor.execute(query, data)

    def get_all_data(self, cursor, table_name):
        cursor.execute("SELECT * FROM " + table_name)
        res = cursor.fetchall()
        return res

    def get_one_data(self, cursor, table_name):
        cursor.execute("SELECT * FROM " + table_name + " LIMIT 1")
        res = cursor.fetchone()
        return res

    def get_data(self, table, queries):
        pass

    def get_size(self, cursor, db):
        pass

    def get_stats(self, db):
        pass

    @staticmethod
    def write_results1(results, name):
        try:
            day = time.strftime("%Y_%m_%d")
            hour = time.strftime("%H_%M_%S")
            str_date = day + "T" + hour
            f = open("/results/" + str_date + "_" + name + ".json", "w")
            json.dump(results, f)
            f.close()
        except Exception, e:
            print e
            print "error saving results in " + name + ".json"

    @staticmethod
    def write_results(results, name):
        # Running with docker compose
        try:
            f = open("/results/" + name + ".json", "w")
            json.dump(results, f)
            f.close()
        except Exception, e1:
            # Running in local
            try:
                f = open("../results/" + name + ".json", "w")
                json.dump(results, f)
                f.close()
            except Exception, e:
                print e1
                print e
                print "error saving results in " + name + ".json"
