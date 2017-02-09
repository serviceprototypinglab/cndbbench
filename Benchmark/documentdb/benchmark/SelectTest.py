import json
from time import time
from pymongo import CursorType
from Mongo import Mongo



class SelectTest:
    # INIT
    def __init__(self):
        pass

    # MONGO
    @staticmethod
    def get_number_property(param, db):
        return db['PropertyTypes'].find_one({"ZIdxGUID": param}, {"ZTableName": 1, "_id": 0})

    def aux_selects_mongo(self, db):
        # Search order number equal
        time_total_start = time()
        time_order_number_eq_start = time()
        property_id = "c5d354b1-4d7e-11e6-80bb-080027a54cf0"
        value = '0000015233.'
        table_property = self.get_number_property(property_id, db)['ZTableName']
        r = db[table_property].find({"ZValue": value},
                                    cursor_type=CursorType.EXHAUST)
        print(r.count())
        # cursor.CursorType = CursorType.EXHAUST
        time_order_number_eq_end = time()
        time_order_number_eq = time_order_number_eq_end - time_order_number_eq_start
        # time_order_number_eq =  cursor.explain()['executionStats']['executionTimeMillis']

        # Search order number not equal
        time_order_number_neq_start = time()
        property_id = "c5d354b1-4d7e-11e6-80bb-080027a54cf0"
        value = '0000015233.'
        table_property = self.get_number_property(property_id, db)['ZTableName']
        print(table_property)
        r = db[table_property].find({"ZValue": {"$ne": value}}, {"ZRowIdent": 1, "_id": 0},
                                    cursor_type=CursorType.EXHAUST)
        print("something")
        print(r.count())
        time_order_number_neq_end = time()
        time_order_number_neq = time_order_number_neq_end - time_order_number_neq_start
        # time_order_number_neq = cursor.explain()['executionStats']['executionTimeMillis']

        # Search many files
        time_many_files_start = time()
        property_id = "d4025f99-4d7e-11e6-80bb-080027a54cf0"
        value = 'IBM'
        table_property = self.get_number_property(property_id, db)['ZTableName']
        r = db[table_property].find({"ZValue": value}, {"ZRowIdent": 1, "_id": 0},
                                    cursor_type=CursorType.EXHAUST)
        print(r.count())
        time_many_files_end = time()
        time_many_files = time_many_files_end - time_many_files_start
        # time_many_files = cursor.explain()['executionStats']['executionTimeMillis']
        # r = query_complex(db)
        # print len(r)
        # Search contains
        time_contains_start = time()
        property_id = "db00e250-4d7e-11e6-80bb-080027a54cf0"
        value_contains = ".*Redmond.*"
        table_property = self.get_number_property(property_id, db)['ZTableName']
        r = db[table_property].find({"ZValue": {"$regex": value_contains}}, {"ZRowIdent": 1, "_id": 0},
                                    cursor_type=CursorType.EXHAUST)
        print(r.count())
        time_contains_end = time()
        time_contains = time_contains_end - time_contains_start
        # time_contains = cursor.explain()['executionStats']['executionTimeMillis']
        time_contains_end = time()
        time_contains = time_contains_end - time_contains_start
        time_total_end = time()
        time_total = time_total_end - time_total_start
        time_results = {'time_order_number_eq': time_order_number_eq,
                        'time_contains': time_contains,
                        'time_many_files': time_many_files,
                        'time_order_number_neq': time_order_number_neq,
                        'time_total': time_total}
        print(time_results)
        return time_results

    def selects_mongo(self, host, port, conn, name_file, number_loops):
        mongo = Mongo()
        if conn:
            pass
        else:
            conn = mongo.create_connexion(host, port)
        db = conn.dbexample
        times = []
        print("connected")
        for i in range(0, number_loops):
            print("Iteration " + str(i))
            t = self.aux_selects_mongo(db)
            # TODO save the time
            times.append(t)
            # Write results postgres
            try:
                f = open("/results/" + name_file + str(i) + ".json", "w")
                json.dump(times, f)
                f.close()
            except Exception:
                print("error saving results in postgres.json")

        print(times)
        # todo save the results
        conn.close()
        # Write results postgres
        try:
            f = open("/results/" + name_file + ".json", "w")
            json.dump(times, f)
            f.close()
        except Exception:
            print("error saving results in postgres.json")
            #
            # print "----------------------------------------------------------"
            # print times

            # try:
            #     time_results = {'times': times}
            #     mongo.insert_all_data(db['results'], time_results)
            # except Exception, e:
            #     print "problen inserting resutls in mongo"
            #     print e

            # mongo.close_connexion(conn, None)

