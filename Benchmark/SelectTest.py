import json
from time import time
from pymongo import CursorType
from Mongo import Mongo
from Couch import Couch
from Mysqldb import Mysqldb
from Crate import Crate
from Postgres import Postgres


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
        print r.count()
        # Search order number not equal
        time_order_number_neq_start = time()
        r = db[collection].find({"number": {"$ne": value_neq}}, {"other_id": 1, "_id": 0},
                                cursor_type=CursorType.EXHAUST)
        print r.count()
        time_order_number_neq_end = time()
        time_order_number_neq = time_order_number_neq_end - time_order_number_neq_start
        # time_order_number_neq = cursor.explain()['executionStats']['executionTimeMillis']

        # Search many files
        time_many_files_start = time()
        r = db[collection].find({"tenant_option": value_many}, {"other_id": 1, "_id": 0},
                                cursor_type=CursorType.EXHAUST)
        print r.count()
        time_many_files_end = time()
        time_many_files = time_many_files_end - time_many_files_start
        # time_many_files = cursor.explain()['executionStats']['executionTimeMillis']
        # r = query_complex(db)
        # print len(r)
        # Search contains
        time_contains_start = time()
        r = db[collection].find({"blob": {"$regex": value_contains}}, {"other_id": 1, "_id": 0},
                                cursor_type=CursorType.EXHAUST)
        print r.count()
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
        return time_results

    def selects_mongo(self, host, port, conn, name_file, number_loops, db, collection, value_eq, value_neq, value_many,
                      value_contains):
        mongo = Mongo()

        if conn:
            pass
        else:
            conn = mongo.create_connexion(host, port)
        db = conn[db]
        print conn
        print db
        times = []
        print "connected"
        for i in range(0, number_loops):
            print "Iteration " + str(i)
            t = self.aux_selects_mongo(db, collection, value_eq, value_neq, value_many, value_contains)
            times.append(t)
        print times
        # todo save the results
        conn.close()
        # Write results mongo
        try:
            f = open("/results/" + name_file + ".json", "w")
            json.dump(times, f)
            f.close()
        except Exception, e:
            print e
            print "error saving results in postgres.json"
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

    # COUCH
    def aux_selects_couch(self, conn, collection, value_eq, value_neq, value_many, value_contains):
        # Search order number equal
        print"start"
        time_total_start = time()
        time_order_number_eq_start = time()
        try:
            map_fun = "function(doc) { if (doc.number == '" + value_eq + "') emit(doc.other_id, null);}"
            a = conn[collection].query(map_fun)
            print len(a)
        except Exception, e:
            print e
            print "ErrorCouch1"
        time_order_number_eq_end = time()
        time_order_number_eq = time_order_number_eq_end - time_order_number_eq_start

        #  search order number not equal

        time_order_number_neq_start = time()

        try:
            map_fun = "function(doc) { if (doc.number != '" + value_neq + "') emit(doc.other_id, null);}"
            r = conn[collection].query(map_fun)
            print len(r)
        except Exception, e:
            print e
            print "ErrorCouch2"
        time_order_number_neq_end = time()
        time_order_number_neq = time_order_number_neq_end - time_order_number_neq_start

        #  search undefined
        #  search many files
        time_many_files_start = time()

        try:
            map_fun = "function(doc) { if (doc.tenant_option == '" + value_many + "') emit(doc.other_id, null);}"
            s = conn[collection].query(map_fun)
            print len(s)
        except Exception, e:
            print e
            print "ErrorCouch 3"
        time_many_files_end = time()
        time_many_files = time_many_files_end - time_many_files_start
        #  search complex
        # r = query_complex(db)
        # print len(r)
        #  search contains
        time_contains_start = time()

        try:
            map_fun = "function(doc) { var a = doc.blob; var b = '" + value_contains + \
                      "'; if (a.indexOf(b) !== -1) { emit(doc.other_id, null);}}"
            c = conn[collection].query(map_fun)
            print len(c)
        except Exception, e:
            print e
            print "ErrorCouch4"
        time_contains_end = time()
        time_contains = time_contains_end - time_contains_start
        time_total_end = time()
        time_total = time_total_end - time_total_start
        time_results = {'time_order_number_eq': time_order_number_eq,
                        'time_contains': time_contains,
                        'time_many_files': time_many_files,
                        'time_order_number_neq': time_order_number_neq,
                        'time_total': time_total}
        return time_results

    def selects_couch(self, host, number_loops, collection, value_eq, value_neq, value_many, value_contains, name_file):
        couch = Couch()
        conn = couch.create_connexion(host, None)
        times = []
        for i in range(0, number_loops):
            print "Iteration " + str(i)
            t = self.aux_selects_couch(conn, collection, value_eq, value_neq, value_many, value_contains)
            times.append(t)

        # Write results postgres
        try:
            f = open("/results/" + name_file + ".json", "w")
            json.dump(times, f)
            f.close()
        except Exception, e:
            print e
            print "error saving results in postgres.json"
        #  search full text
        # query_full_text(db)

        # time_results = {'time_order_number_eq': time_order_number_eq,
        #                 'time_contains': time_contains,
        #                 'time_many_files': time_many_files,
        #                 'time_order_number_neq': time_order_number_neq}
        #
        # # Write results postgres
        # try:
        #     f = open("/results/queries_couch.json", "w")
        #     json.dump(time_results, f)
        #     f.close()
        # except Exception, e:
        #     print e
        #     print "error saving results in couch.json"
        #
        # print "----------------------------------------------------------"
        # print time_results
        #
        # del conn['propertytypes']

    # SQL
    @staticmethod
    def aux_selects_sql(cursor, table_name, value_eq, value_neq, value_many, value_contains):
        print "start"
        time_total_start = time()
        time_order_number_eq_start = time()
        try:
            query2 = "SELECT other_id FROM " + table_name + " WHERE number = " + value_eq
            cursor.execute(query2)
            r = cursor.fetchall()
            print len(r)
        except Exception, e:
            print e
            print "error thread 1"

        time_order_number_eq_end = time()
        time_order_number_eq = time_order_number_eq_end - time_order_number_eq_start

        #  search order number not equal
        time_order_number_not_eq_start = time()

        try:

            query4 = "SELECT other_id FROM " + table_name + " WHERE number != " + value_neq + " LIMIT 100000000"
            cursor.execute(query4)
            r = cursor.fetchall()
            print len(r)
        except Exception, e:
            print e
            print "error thread 2"

        time_order_number_not_eq_end = time()
        time_order_number_not_eq = time_order_number_not_eq_end - time_order_number_not_eq_start
        #  search undefined
        #  search many files

        time_many_files_start = time()
        try:
            query6 = "SELECT other_id FROM " + table_name + " WHERE tenant_option = '" + value_many + "' LIMIT 10000000"
            cursor.execute(query6)
            r = cursor.fetchall()
            print len(r)
        except Exception, e:
            print e
            print "error thread 3"
        # print len(r)
        time_many_files_end = time()
        time_many_files = time_many_files_end - time_many_files_start

        #  search complex
        #  search contains
        time_contains_start = time()
        try:
            query8 = "SELECT other_id FROM " + table_name + " WHERE ZValue LIKE '%" + value_contains + "%' LIMIT 100000000"
            cursor.execute(query8)
            r = cursor.fetchall()
            print len(r)
        except Exception, e:
            print e
            print "error thread 4"

        # print len(r)
        time_contains_end = time()
        time_contains = time_contains_end - time_contains_start
        time_total_end = time()
        time_total = time_total_end - time_total_start
        time_results = {'time_order_number_eq': time_order_number_eq,
                        'time_contains': time_contains,
                        'time_many_files': time_many_files,
                        'time_order_number_neq': time_order_number_not_eq,
                        'time_total': time_total}
        return time_results

    def selects_sql(self, database_name, host, port, user, password, dbname, name_results, number_of_loops, table_name,
                    value_eq, value_neq, value_many, value_contains):
        with open('config.json') as data_file:
            data = json.load(data_file)
        print "config file readed"
        if database_name == 'postgres':
            database = Postgres()
            if host:
                conn_string_postgres = host
            else:
                conn_string_postgres = data['conn_string_postgres']
            conn = database.create_connexion(None, None, None, None, conn_string_postgres)
        elif database_name == 'mysql':
            database = Mysqldb()
            if user:
                user_mysql = user
            else:
                user_mysql = data['user_mysql']
            if password:
                password_mysql = password
            else:
                password_mysql = data['password_mysql']
            if dbname:
                database_mysql = dbname
            else:
                database_mysql = data['database_mysql']
            conn = database.create_connexion(user=user_mysql,
                                             password=password_mysql,
                                             host=host,
                                             database=database_mysql,
                                             string_connect="")
        else:
            database = Crate()
            conn = database.create_connexion(user='',
                                             password='',
                                             host='',
                                             database='',
                                             string_connect=host)
        times = []
        times_total = []
        cursor = conn.cursor()

        for i in range(0, number_of_loops):
            print "iteration " + str(i)
            if database_name == 'crate':
                t = self.aux_selects_sql(cursor, table_name, value_eq, value_neq, value_many, value_contains)
            else:
                t = self.aux_selects_sql(cursor, table_name, value_eq, value_neq, value_many, value_contains)
            total_times = t['time_total']
            times.append(t)
            times_total.append(total_times)
        print times
        print times_total
        # query10 = "SELECT ZRowIdent FROM BlobStore WHERE CONTAINS(ZBlob, 'Europe')"
        # cursor.execute(query10)
        # r = cursor.fetchall()
        # print len(r)

        conn.close()

        # time_results = {'time_order_number_eq': time_order_number_eq,
        #                 'time_contains': time_contains,
        #                 'time_many_files': time_many_files,
        #                 'time_order_number_neq': time_order_number_neq,
        #                 'times_100': times}
        # Write results
        # if cloud == "amazon":
        #    name_results = 'selects_100_cloud' + database_name
        # elif cloud == "kubernetes":
        #    name_results = 'selects_100_kubernetes' + database_name
        # else:
        #    name_results = 'selects_100' + database_name
        database.write_results(times, name_results)

        print "------------------------*----------------------------------"
        print times

    # POSTGRES
    def selects_postgres(self, host, port, name_results, number_of_loops, table_name, value_eq, value_neq, value_many,
                         value_contains):
        self.selects_sql('postgres', host, port, None, None, None, name_results, number_of_loops, table_name, value_eq,
                         value_neq, value_many, value_contains)

    # MYSQL
    def selects_mysql(self, host, port, user, password, dbname, name_results, number_of_loops, table_name, value_eq,
                      value_neq, value_many, value_contains):
        self.selects_sql('mysql', host, port, user, password, dbname, name_results, number_of_loops, table_name,
                         value_eq, value_neq, value_many, value_contains)

    # CRATE
    def selects_crate(self, host, port, name_results, number_of_loops, table_name, value_eq, value_neq, value_many,
                      value_contains):
        self.selects_sql('crate', host, port, None, None, None, name_results, number_of_loops, table_name, value_eq,
                         value_neq, value_many, value_contains)
