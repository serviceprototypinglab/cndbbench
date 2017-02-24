import json
from time import time
from pymongo import CursorType
from Mongo import Mongo


class SelectTest:
    # INIT
    def __init__(self):
        pass

    # MONGO
    def aux_selects_mongo(self, db, collection, value_eq, value_neq, value_many, value_contains):
        # Search order number equal
        time_total_start = time()
        time_order_number_eq_start = time()
        r = db[collection].find({"number": value_eq}, cursor_type=CursorType.EXHAUST)
        time_order_number_eq_end = time()
        time_order_number_eq = time_order_number_eq_end - time_order_number_eq_start
        # time_order_number_eq =  cursor.explain()['executionStats']['executionTimeMillis']

        # Search order number not equal
        eq_val = r.count()
        time_order_number_neq_start = time()
        r = db[collection].find({"number": {"$ne": value_neq}}, {"other_id": 1, "_id": 0},
                                cursor_type=CursorType.EXHAUST)
        no_eq_val = r.count()
        time_order_number_neq_end = time()
        time_order_number_neq = time_order_number_neq_end - time_order_number_neq_start
        # time_order_number_neq = cursor.explain()['executionStats']['executionTimeMillis']

        # Search many files
        time_many_files_start = time()
        r = db[collection].find({"tenant_option": value_many}, {"other_id": 1, "_id": 0},
                                cursor_type=CursorType.EXHAUST)
        many_val = r.count()
        time_many_files_end = time()
        time_many_files = time_many_files_end - time_many_files_start
        # time_many_files = cursor.explain()['executionStats']['executionTimeMillis']
        # r = query_complex(db)
        # print len(r)
        # Search contains
        time_contains_start = time()
        r = db[collection].find({"blob": {"$regex": value_contains}}, {"other_id": 1, "_id": 0},
                                cursor_type=CursorType.EXHAUST)
        contain_val = r.count()
        time_contains_end = time()
        time_contains = time_contains_end - time_contains_start
        # time_contains = cursor.explain()['executionStats']['executionTimeMillis']
        # time_contains_end = time()
        # time_contains = time_contains_end - time_contains_start
        time_total_end = time()
        time_total = time_total_end - time_total_start
        time_results = {'time_order_number_eq': time_order_number_eq,
                        'time_contains': time_contains,
                        'time_many_files': time_many_files,
                        'time_order_number_neq': time_order_number_neq,
                        'time_total': time_total}
        results_json = {'eq_val': eq_val, 'no_eq_val': no_eq_val, 'many_val': many_val, 'contain_val': contain_val}
        # print(time_results)
        print(results_json)
        return results_json

    def selects_mongo(self, host, port, conn, name_file, number_loops, db, collection, value_eq, value_neq, value_many,
                      value_contains):
        db = conn[db]
        times = []
        print("connected")
        for i in range(0, number_loops):
            print("Iteration " + str(i))
            t = self.aux_selects_mongo(db, collection, value_eq, value_neq, value_many, value_contains)
            times.append(t)
        print(times)
        # todo save the results
        conn.close()
        # Write results mongo
        try:
            f = open("/results/" + name_file + ".json", "w")
            json.dump(times, f)
            f.close()
        except Exception:
            print("error saving results in documentdb.json")
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
