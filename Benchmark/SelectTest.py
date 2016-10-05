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
    @staticmethod
    def get_number_property(param, db):
        return db['PropertyTypes'].find_one({"ZIdxGUID": param}, {"ZTableName": 1, "_id": 0})

    def aux_selects_mongo(self, db):
        pass

    def selects_mongo(self, host, conn, number_loops):
        mongo = Mongo()
        if conn:
            pass
        else:
            conn = mongo.create_connexion(host, 27017)
        db = conn.dbexample
        times = []

        for i in range(0, number_loops):
            time_start = time()
            self.aux_selects_mongo(db)
            time_end = time()
            times.append(time_end - time_start)
        print times
        conn.close()
        # Write results postgres
        # try:
        #     f = open("/results/queries_mongo.json", "w")
        #     json.dump(times, f)
        #     f.close()
        # except Exception, e:
        #     print e
        #     print "error saving results in postgres.json"
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
    @staticmethod
    def get_number_property_couch(param, db):
        map_fun = "function(doc) { if (doc.ZIdxGUID == '" + param + "') emit(doc.ZTableName, null);}"
        for row in db.query(map_fun):
            return row.key.lower()

    def aux_selects_couch(self, conn):
        pass

    def selects_couch(self, host, number_loops):
        couch = Couch()
        conn = couch.create_connexion(host, None)
        times = []
        for i in range(0, number_loops):
            time_start = time()
            self.aux_selects_couch(conn)
            time_end = time()
            times.append(time_end - time_start)

        print times
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
    def aux_selects_crate(cursor):
        query1 = "SELECT * FROM example"
        query2 = "SELECT * FROM example WHERE exampleid = 1"
        query3 = "SELECT * FROM example WHERE exampleid > 1000 and exampleid < 10000"
        cursor.execute(query1)
        r = cursor.fetchall()
        print len(r)

        cursor.execute(query2)
        r = cursor.fetchall()
        print len(r)

        cursor.execute(query3)
        r = cursor.fetchall()
        print len(r)

    def selects_sql(self, database_name, cloud, host, name_results, number_of_loops):
        with open('config.json') as data_file:
            data = json.load(data_file)
        if database_name == 'postgres':
            # database = Postgres()
            database = Postgres()
            conn_string_postgres = host
            conn = database.create_connexion(None, None, None, None, conn_string_postgres)
        elif database_name == 'mysql':
            database = Mysqldb()
            if cloud == "amazon":
                host_amazon_mysql = data['host_amazon_mysql']
                user_amazon_mysql = data['user_amazon_mysql']
                password_amazon_mysql = data['password_amazon_mysql']
                dbname_amazon_mysql = data['dbname_amazon_mysql']
                conn = database.create_connexion(user=user_amazon_mysql,
                                                 password=password_amazon_mysql,
                                                 host=host_amazon_mysql,
                                                 database=dbname_amazon_mysql,
                                                 string_connect="")
            elif cloud == "kubernetes":
                user_kubernetes_mysql = data['user_kubernetes_mysql']
                password_kubernetes_mysql = data['password_kubernetes_mysql']
                host_kubernetes_mysql = data['host_kubernetes_mysql']
                database_kubernetes_mysql = data['database_kubernetes_mysql']
                conn = database.create_connexion(user=user_kubernetes_mysql,
                                                 password=password_kubernetes_mysql,
                                                 host=host_kubernetes_mysql,
                                                 database=database_kubernetes_mysql,
                                                 string_connect="")
            elif cloud == "aurora":
                user_aurora = data['user_aurora']
                password_aurora = data['password_aurora']
                host_aurora = data['host_aurora']
                database_aurora = data['database_aurora']
                conn = database.create_connexion(user=user_aurora,
                                                 password=password_aurora,
                                                 host=host_aurora,
                                                 database=database_aurora,
                                                 string_connect="")
            else:
                password = "f87a3844"
                username = "b88cd1fb83f44d"
                dbname = 'ad_6e5db755ddea001'
                conn = database.create_connexion(user=username,
                                                 password=password,
                                                 host=host,
                                                 database=dbname,
                                                 string_connect="")
        else:
            database = Crate()
            conn = database.create_connexion(user='',
                                             password='',
                                             host='',
                                             database='',
                                             string_connect=host)
        times = []
        cursor = conn.cursor()

        for i in range(0, number_of_loops):
            print i
            time_start = time()
            self.aux_selects_crate(cursor)
            time_end = time()
            times.append(time_end - time_start)
        print times
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
    def selects_postgres(self, string_connect, numbers_of_loop):
        self.selects_sql('postgres', False, string_connect, 'postgresselects', numbers_of_loop)

    # MYSQL
    def selects_mysql(self, host, numbers_of_loop):
        self.selects_sql('mysql', False, host, 'mysqlbenchamrkkubernetes', numbers_of_loop)

    # CRATE
    def selects_crate(self, host, name_results, numbers_of_loop):
        self.selects_sql('crate', False, host, name_results, numbers_of_loop)
