import json
import time


class DocumentDb:

    def __init__(self):
        pass

    def create_connexion(self, host, port):
        pass

    def close_connexion(self, connexion, name):
        pass

    def create_database(self, connexion, db_name):
        pass

    def delete_database(self, connexion, name):
        pass

    def create_collection(self, database, collection_name):
        pass

    def delete_collection(self, collection):
        pass

    def insert_all_data(self, collection, json_data):
        pass

    def insert_one_data(self, collection, json_data):
        pass

    def get_all_data(self, collection):
        pass

    def get_one_data(self, collection):
        pass

    def get_size(self, db):
        pass

    def get_stats(self, db):
        pass

    @staticmethod
    def write_results2(results, name):
        try:
            day = time.strftime("%Y_%m_%d")
            hour = time.strftime("%H_%M_%S")
            str_date = day + "T" + hour
            f = open("/results/" + str_date + "_" + name + ".json", "w")
            json.dump(results, f)
            f.close()
        except Exception:
            pass

    @staticmethod
    def write_results(results, name):
        # Running with docker compose
        try:
            f = open("/results/" + name + ".json", "w")
            json.dump(results, f)
            f.close()
        except Exception:
            # Running in local
            try:
                f = open("../results/" + name + ".json", "w")
                json.dump(results, f)
                f.close()
            except Exception:
                pass