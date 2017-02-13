import json
from time import time
from Mongo import Mongo


class InsertTest:
    def __init__(self, tables, collections):
        with open('config.json') as data_file:
            self.data = json.load(data_file)
        if tables:
            self.tables = tables
        else:
            self.sql_tables = self.data['sql_tables']

        if collections:
            self.collections = collections
        else:
            self.collections = self.data['collections']

    @staticmethod
    def delete_table(cursor1, table_name):
        query_delete = 'DROP TABLE IF EXISTS ' + table_name
        cursor1.execute(query_delete)

    @staticmethod
    def read_data(folder_name, name):
        try:
            with open("/" + folder_name + "/" + name + ".json") as json_file:
                json_data = json.load(json_file)
        except Exception:
            try:
                with open("../" + folder_name + "/" + name + ".json") as json_file:
                    json_data = json.load(json_file)
            except Exception:
                print("Error reading 2 " + name)
                json_data = []
        return json_data

    # MONGO
    def insert_mongo(self, host, port, database, one, conn, number_user, user, option):
        # sleep(15)
        inserts_time_one = 0
        delete_table_time = 0
        create_database_time = 0
        create_table_time = 0
        close_connexion_time = 0
        create_connexion_time = 0
        time_mongo_start = time()
        mongo = Mongo()
        # Create connexion
        try:
            create_connexion_start = time()
            if conn:
                pass
            else:
                conn = mongo.create_connexion(host, port)
            create_connexion_end = time()
            create_connexion_time = create_connexion_end - create_connexion_start
            print("connected")
        except Exception:
            pass
            print("connection with mongo problem")

        # Create database
        try:
            create_database_start = time()
            db_name = 'arkis'
            if database:
                db_name = database
            db = mongo.create_database(conn, db_name)
            create_database_end = time()
            create_database_time = create_database_end - create_database_start
        except Exception:
            pass
            print("create a database mongo problem")
        print("created")
        # Delete all collections if exits
        try:
            for coll in self.collections:
                mongo.delete_collection(db[coll])
                # print coll + "delete"
                pass
        except Exception:
            pass
            print("Deleting mongo problem")

        # Create self.collections
        try:
            create_table_start = time()
            # self.collections will be create when you add data
            create_table_end = time()
            create_table_time = create_table_end - create_table_start
        except Exception:
            pass
            # print collection_name
            print("create a table mongo problem")

        print("----------------------------------------------------------")

        if one:
            # Insert all one per one
            inserts_time_one = {}
            j = None
            try:
                insert_one_data_time = 0
                for coll in self.collections:
                    print("inserting one" + coll)
                    collection = db[coll]
                    json_data = self.read_data('sharedData', coll)[:100]
                    if json_data:
                        insert_one_start = time()
                        for j in json_data:
                            mongo.insert_one_data(collection, j)
                        insert_one_end = time()
                        count_documents = len(json_data)
                        time_aux = {'time': insert_one_end - insert_one_start,
                                    'count': count_documents}
                        inserts_time_one[coll] = time_aux
                        insert_one_data_time += (insert_one_end - insert_one_start)
                inserts_time_one['total'] = insert_one_data_time
            except Exception:
                pass
                if j:
                    print(j)
                print("Insert one mongo problem")

            print("----------------------------------------------------------")

            # Delete all
            try:
                delete_table_start = time()
                for coll in self.collections:
                    mongo.delete_collection(db[coll])
                delete_table_end = time()
                delete_table_time = delete_table_end - delete_table_start
            except Exception:
                pass
                print("Deleting mongo problem")

            print("-------------------------------------------------------------")

        # Insert all
        inserts_time_all = {}
        try:
            insert_all_data_time = 0
            for coll in self.collections:
                print("inserting all" + coll)
                json_data = self.read_data('sharedData', coll)
                if json_data:
                    len_json_data = len(json_data)
                    print(len_json_data)
                    count1 = 0
                    count2 = 10000
                    count3 = 0
                    ok = True
                    insert_all_start = time()
                    while ok:
                        print(count1)
                        if count3 > len_json_data:
                            count3 = len_json_data
                            ok = False
                        else:
                            count3 = count1 + count2
                        aux = json_data[count1:count3 - 1]
                        count1 = count3
                        if aux and len(aux) > 0:
                            try:
                                print(count1)
                                mongo.insert_all_data(db[coll], aux)
                            except Exception as err:
                                print(err)
                                print(coll)
                                print("Insert all mongo problem")
                        else:
                            ok = False
                    # conn = couch.create_connexion(host)
                    insert_all_end = time()
                    #insert_all_start = time()
                    #mongo.insert_all_data(db[coll], json_data)
                    #insert_all_end = time()
                    inserts_time_all[coll] = insert_all_end - insert_all_start
                    insert_all_data_time += (insert_all_end - insert_all_start)
            inserts_time_all['total'] = insert_all_data_time
        except Exception:
            pass
            print("Problem collection inserting all")
            print(coll)

        # Close connexion
        try:
            close_connexion_time_start = time()
            mongo.close_connexion(conn, "")
            close_connexion_time_end = time()
            close_connexion_time = close_connexion_time_end - close_connexion_time_start
        except Exception:
            pass
            print("Exception close")

        time_mongo_end = time()
        time_mongo = time_mongo_end - time_mongo_start
        if one:
            # Results mongo
            time_results = {'create_connexion_time': create_connexion_time,
                            'close_connexion_time': close_connexion_time,
                            'create_database_time': create_database_time,
                            'create_table_time': create_table_time,
                            'inserts_time_all': inserts_time_all,
                            'inserts_time_one': inserts_time_one,
                            'delete_table_time': delete_table_time,
                            'stats': 0,
                            'size': 0,
                            'total_time': time_mongo}
            print(time_results)
            # Write results mongo
            mongo.write_results(time_results, "inserts_mongo")

        print("End inserting")

