import json
from time import time
from time import sleep
import binascii
from Mongo import Mongo
from Couch import Couch
from Postgres import Postgres
from Mysqldb import Mysqldb
from Crate import Crate
import ijson


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
    def read_big_json():
        with open('/sharedData/big_json.json', 'r') as f:
            for j in ijson.items(f, 'item'):
                print j
        return ""
        # with open('/sharedData/big_json.json', 'r') as f:
        #    for line in f:
        #        return json.loads(line)[:1000]

    @staticmethod
    def read_data(folder_name, name):
        try:
            with open("/" + folder_name + "/" + name + ".json") as json_file:
                json_data = json.load(json_file)
        except Exception, e1:
            try:
                with open("../" + folder_name + "/" + name + ".json") as json_file:
                    json_data = json.load(json_file)
            except Exception, e:
                print e1
                print e
                print "Error reading 2 " + name
                json_data = []
        return json_data

    # MONGO
    def insert_mongo(self, host, port, database, one, conn):
        sleep(15)
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
        except Exception, e:
            print e
            print "connection with mongo problem"

        # Create database
        try:
            create_database_start = time()
            db_name = 'dbexample'
            if database:
                db_name = database
            db = mongo.create_database(conn, db_name)
            create_database_end = time()
            create_database_time = create_database_end - create_database_start
        except Exception, e:
            print e
            print "create a database mongo problem"

        # Delete all collections if exits
        try:
            for coll in self.collections:
                pass
                # mongo.delete_collection(db[coll])
        except Exception, e:
            print e
            print "Deleting mongo problem"

        # Create self.collections
        try:
            create_table_start = time()
            # self.collections will be create when you add data
            create_table_end = time()
            create_table_time = create_table_end - create_table_start
        except Exception, e:
            print e
            # print collection_name
            print "create a table mongo problem"

        print "----------------------------------------------------------"

        if one:
            # Insert all one per one
            inserts_time_one = {}
            try:
                insert_one_data_time = 0
                for coll in self.collections:
                    print "inserting one" + coll
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
            except Exception, e:
                print e
                print coll
                print j
                print "Insert one mongo problem"

            print "----------------------------------------------------------"

            # Delete all
            try:
                delete_table_start = time()
                for coll in self.collections:
                    mongo.delete_collection(db[coll])
                delete_table_end = time()
                delete_table_time = delete_table_end - delete_table_start
            except Exception, e:
                print e
                print "Deleting mongo problem"

            print "-------------------------------------------------------------"

        # Insert all
        inserts_time_all = {}
        try:
            insert_all_data_time = 0
            for coll in self.collections:
                print "inserting all" + coll
                json_data = self.read_data('sharedData', coll)
                if json_data:
                    insert_all_start = time()
                    mongo.insert_all_data(db[coll], json_data)
                    insert_all_end = time()
                    inserts_time_all[coll] = insert_all_end - insert_all_start
                    insert_all_data_time += (insert_all_end - insert_all_start)
            inserts_time_all['total'] = insert_all_data_time
        except Exception, e:
            print e
            print coll
            print "Insert all mongo problem"

        print "----------------------------------------------------------"

        if one:
            # Stats and size
            try:
                stats = mongo.get_stats(db)
                size = mongo.get_size(db)
            except Exception, e:
                print e
                print "get stats mongo problem"

        # Close connexion
        try:
            close_connexion_time_start = time()
            mongo.close_connexion(conn, "")
            close_connexion_time_end = time()
            close_connexion_time = close_connexion_time_end - close_connexion_time_start
        except Exception, e:
            print e
            print "Problem with close connexion"

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
                            'stats': stats,
                            'size': size,
                            'total_time': time_mongo}

            # Write results mongo
            mongo.write_results(time_results, "inserts_mongo")

            print "----------------------------------------------------------"
            print time_results
        print "End inserting"

    # COUCHDB
    def insert_couch(self, host, one):
        create_connexion_time = -1
        create_database_time = -1
        create_table_time = -1
        close_connexion_time = - 1
        lower_collections = []
        for collection_name in self.collections:
            lower_collections.append(collection_name.lower())
        sleep(15)
        time_couch_start = time()
        couch = Couch()
        # Create connexion
        try:
            create_connexion_start = time()
            conn = couch.create_connexion(host, None)
            create_connexion_end = time()
            create_connexion_time = create_connexion_end - create_connexion_start
        except Exception, e:
            print e
            print "connection with couch problem"

        # Delete database

        for collection_name in lower_collections:
            try:
                couch.delete_collection(conn[collection_name])
            except Exception, e:
                print e
                print collection_name
                print "Deleting couch problem"
                try:
                    couch.delete_database(conn, collection_name)
                except Exception, e1:
                    print e1
                    print "deleting database problem"

        print "--------------------------------------------"

        # Create database
        try:
            create_database_start = time()
            for collection_name in lower_collections:
                couch.create_database(conn, collection_name)
            create_database_end = time()
            create_database_time = create_database_end - create_database_start
        except Exception, e:
            print e
            print collection_name
            print "create a database couch problem"

        print "--------------------------------------------"

        # Create collections
        try:
            create_table_start = time()
            # for collection_name in collections:
            #    db[collection_name]
            # Collections will be create when you add data
            create_table_end = time()
            create_table_time = create_table_end - create_table_start
        except Exception, e:
            print e
            # print collection_name
            print "create a table couch problem"

        print "--------------------------------------------"

        # Insert all one per one (Only 100 first) (Very slow)
        if one:
            inserts_time_one = {}
            try:
                insert_one_data_time = 0
                for collection_name in self.collections:
                    lower_table = collection_name.lower()
                    print "inserting one" + collection_name
                    json_data = self.read_data('sharedData', collection_name)
                    if json_data:
                        json_data = json_data[:100]
                        insert_one_start = time()
                        for j in json_data:
                            couch.insert_one_data(conn[lower_table], j)
                        insert_one_end = time()
                        time_aux = {'time': insert_one_end - insert_one_start,
                                    'count': 100}
                        inserts_time_one[collection_name] = time_aux
                        insert_one_data_time += (insert_one_end - insert_one_start)
                inserts_time_one['total'] = insert_one_data_time
            except Exception, e:
                print e
                print collection_name
                print str(j)[:100]
                print "Insert one couch problem"

            print "--------------------------------------------"

            # Delete all

            delete_table_start = time()
            for collection_name in lower_collections:
                try:
                    couch.delete_collection(conn[collection_name])
                except Exception, e:
                    print e
                    print collection_name
                    print "Deleting couch problem"
            delete_table_end = time()
            delete_table_time = delete_table_end - delete_table_start

        try:
            for collection_name in lower_collections:
                couch.create_database(conn, collection_name)
        except Exception, e:
            print e
            print collection_name
            print "Creating couch problem"

        print "--------------------------------------------"
        print "Inserting all"
        # Insert all
        inserts_time_all = {}
        try:
            insert_all_data_time = 0
            for collection_name in self.collections:
                print "inserting all " + collection_name.lower()
                json_data = self.read_data('sharedData', collection_name)
                if json_data:

                    collection_name.lower()
                    len_json_data = len(json_data)
                    count1 = 0
                    count2 = 10000
                    count3 = 0
                    ok = True
                    insert_all_start = time()
                    while ok:
                        print count1
                        if count3 > len_json_data:
                            count3 = len_json_data
                            ok = False
                        else:
                            count3 = count1 + count2
                            aux = json_data[count1:count3 - 1]
                            count1 = count3
                            if aux and len(aux) > 0:
                                try:
                                    couch.insert_all_data(conn[collection_name.lower()], aux)
                                except Exception, e:
                                    print e
                                    print collection_name
                                    print "Insert all couch problem"
                            else:
                                ok = False
                    # conn = couch.create_connexion(host)
                    insert_all_end = time()
                    inserts_time_all[collection_name] = insert_all_end - insert_all_start
                    insert_all_data_time += (insert_all_end - insert_all_start)
            inserts_time_all['total'] = insert_all_data_time
        except Exception, e:
            print e
            print collection_name
            print "Insert all couch problem"

        print "--------------------------------------------"

        # Stats
        try:
            size = []
            for coll in lower_collections:
                size.append(couch.get_size(coll))
            stats = couch.get_stats(conn)
        except Exception, e:
            print e
            print "get stats couch problem"

        print "--------------------------------------------"

        # Close connexion
        try:
            close_connexion_time_start = time()
            # for coll in lower_collections:
            #    couch.delete_database(conn, coll)
            close_connexion_time_end = time()
            close_connexion_time = close_connexion_time_end - close_connexion_time_start
        except Exception, e:
            print e
            print "Problem with close connexion"

        print "--------------------------------------------"

        time_couch_end = time()
        time_couch = time_couch_end - time_couch_start
        contents = ""
        try:
            import urllib2
            response = urllib2.urlopen(host).read()
            contents = json.load(response)
        except Exception, e:
            print e

        try:
            # Results couch
            time_results = {'create_connexion_time': create_connexion_time,
                            'close_connexion_time': close_connexion_time,
                            'create_database_time': create_database_time,
                            'create_table_time': create_table_time,
                            'inserts_time_all': inserts_time_all,
                            'inserts_time_one': inserts_time_one,
                            'delete_table_time': delete_table_time,
                            'stats': stats,
                            'contents': contents,
                            'size': size,
                            'total_time': time_couch}

            # Write results couch
            couch.write_results(time_results, "inserts_couch")

            print "--------------------------------------------"

            print time_results
        except Exception, e:
            print e
        print "End inserting"

    # POSTGRES
    def insert_postgres(self, string_connect, one):
        sleep(10)
        time_postgres_start = time()
        postgres = Postgres()
        # Create connexion
        try:
            create_connexion_start = time()
            conn = postgres.create_connexion(None, None, None, None, string_connect=string_connect)
            cursor = conn.cursor()
            create_connexion_end = time()
            create_connexion_time = create_connexion_end - create_connexion_start
        except Exception, e:
            print e
            print "connection with postgres problem"

        # Create database
        try:
            create_database_start = time()
            create_database_end = time()
            create_database_time = create_database_end - create_database_start
        except Exception, e:
            print e
            print "create a database postgres problem"

        # Delete all self.tables if exits
        try:
            for table_name in self.tables:
                self.delete_table(cursor, table_name)
                query_delete = 'DROP TABLE IF EXISTS ' + table_name
                cursor.execute(query_delete)
            conn.commit()
        except Exception, e:
            print e
            print "Deleting postgres problem"

        # Create self.tables
        try:
            queries_create = self.read_data('sharedData', "creates")
            create_table_start = time()
            for table in self.tables:
                postgres.create_table(cursor, queries_create['postgres'][table])
            conn.commit()
            create_table_end = time()
            create_table_time = create_table_end - create_table_start
        except Exception, e:
            print e
            print table
            print "create a table postgres problem"

        print "----------------------------------------------------------"

        if one:
            # Insert all one per one
            inserts_time_one = {}
            try:
                queries_insert = self.read_data('sharedData', 'inserts')
                insert_one_data_time = 0
                for table_name in self.tables:
                    print "inserting one" + table_name
                    json_data = self.read_data('sharedData', table_name)[:100]
                    if json_data:
                        query_insert = queries_insert['postgres'][table_name]
                        insert_one_start = time()
                        for j in json_data:
                            postgres.insert_one_data(cursor, query_insert, j)
                        conn.commit()
                        insert_one_end = time()
                        count_documents = len(json_data)
                        time_aux = {'time': insert_one_end - insert_one_start,
                                    'count': count_documents}
                        inserts_time_one[table_name] = time_aux
                        insert_one_data_time += (insert_one_end - insert_one_start)
                inserts_time_one['total'] = insert_one_data_time
            except Exception, e:
                print e
                print table_name
                print j
                print "Insert one postgres problem"

            print "----------------------------------------------------------"

            # Delete all
            try:
                delete_table_start = time()
                for table_name in self.tables:
                    self.delete_table(cursor, table_name)
                conn.commit()
                delete_table_end = time()
                delete_table_time = delete_table_end - delete_table_start
            except Exception, e:
                print e
                print table_name
                print "Deleting postgres problem"

            print "-------------------------------------------------------------"

            # Create self.tables
            try:
                queries_create = self.read_data('sharedData', "creates")
                create_table_start = time()
                for table in self.tables:
                    postgres.create_table(cursor, queries_create['postgres'][table])
                conn.commit()
                create_table_end = time()
                create_table_time = create_table_end - create_table_start
            except Exception, e:
                print e
                print table
                print "create a table postgres problem"

            print "----------------------------------------------------------"

        # Insert all
        inserts_time_all = {}
        try:
            queries_insert = self.read_data('sharedData', 'inserts')
            insert_all_data_time = 0
            for table_name in self.tables:
                print "inserting all" + table_name
                json_data = self.read_data('sharedData',  table_name)
                if json_data:
                    query_insert = queries_insert['postgres'][table_name]
                    insert_all_start = time()
                    postgres.insert_all_data(cursor, query_insert, json_data)
                    conn.commit()
                    insert_all_end = time()
                    inserts_time_all[table_name] = insert_all_end - insert_all_start
                    insert_all_data_time += (insert_all_end - insert_all_start)
            inserts_time_all['total'] = insert_all_data_time
        except Exception, e:
            print e
            print table_name
            print "Insert all postgres problem"

        print "---------------------------------------------------------------"

        # Stats
        try:
            db_name = 'postgres'
            size = postgres.get_size(cursor, db_name)
        except Exception, e:
            print e
            print "get stats postgres problem"

        # Close connexion
        try:
            close_connexion_time_start = time()
            cursor.close()
            postgres.close_connexion(conn)
            close_connexion_time_end = time()
            close_connexion_time = close_connexion_time_end - close_connexion_time_start
        except Exception, e:
            print e
            print "Problem with close connexion"

        time_postgres_end = time()
        time_postgres = time_postgres_end - time_postgres_start

        try:
            # Results postgres
            time_results = {'create_connexion_time': create_connexion_time,
                            'close_connexion_time': close_connexion_time,
                            'create_database_time': create_database_time,
                            'create_table_time': create_table_time,
                            'inserts_time_all': inserts_time_all,
                            'inserts_time_one': inserts_time_one,
                            'delete_table_time': delete_table_time,
                            'size': size,
                            'total_time': time_postgres}

            # Write results postgres
            postgres.write_results(time_results, "inserts_postgres")

            print "----------------------------------------------------------"
            print time_results
        except Exception, e:
            print e
            print "time results error"
        print "End inserting"

    # MYSQL
    def insert_mysql(self, host, user, password, database, one):
        sleep(10)
        time_mysql_start = time()
        mysqldb = Mysqldb()
        # Create connexion
        try:
            create_connexion_start = time()
            conn = mysqldb.create_connexion(user=user,
                                            password=password,
                                            host=host,
                                            database=database,
                                            string_connect="")
            cursor = conn.cursor()
            create_connexion_end = time()
            create_connexion_time = create_connexion_end - create_connexion_start
        except Exception, e:
            print e
            print "connection with mysql problem"

        # Create database
        try:
            create_database_start = time()
            create_database_end = time()
            create_database_time = create_database_end - create_database_start
        except Exception, e:
            print e
            print "create a database mysql problem"

        # Delete all self.tables if exits
        try:
            for table_name in self.tables:
                self.delete_table(cursor, table_name)
            conn.commit()
        except Exception, e:
            print e
            print "Deleting mysql problem"

        # Create self.tables
        try:
            queries_create = self.read_data('sharedData', "creates")
            create_table_start = time()
            for table in self.tables:
                print queries_create['mysql'][table]
                mysqldb.create_table(cursor, queries_create['mysql'][table])
            conn.commit()
            create_table_end = time()
            create_table_time = create_table_end - create_table_start
        except Exception, e:
            print e
            print table
            print "create a table mysql problem"

        print "------------------------------------------------------------"

        if one:
            # Insert all one per one
            inserts_time_one = {}
            try:
                queries_insert = self.read_data('sharedData', 'inserts')
                insert_one_data_time = 0
                for table_name in self.tables:
                    print "inserting one" + table_name
                    json_data = self.read_data('sharedData', table_name)[:100]
                    if json_data:
                        query_insert = queries_insert['mysql'][table_name]
                        insert_one_start = time()
                        for j in json_data:
                            mysqldb.insert_one_data(cursor, query_insert, j)
                        conn.commit()
                        insert_one_end = time()
                        count_documents = len(json_data)
                        time_aux = {'time': insert_one_end - insert_one_start,
                                    'count': count_documents}
                        inserts_time_one[table_name] = time_aux
                        insert_one_data_time += (insert_one_end - insert_one_start)
                inserts_time_one['total'] = insert_one_data_time
            except Exception, e:
                print e
                print table_name
                print j
                print "Insert one mysql problem"

            print "------------------------------------------------------------"

            # Delete all
            try:
                delete_table_start = time()
                for table_name in self.tables:
                    self.delete_table(cursor, table_name)
                conn.commit()
                delete_table_end = time()
                delete_table_time = delete_table_end - delete_table_start
            except Exception, e:
                print e
                print table_name
                print "Deleting mysql problem"

            print "-------------------------------------------------------------"

            # Create self.tables
            try:
                queries_create = self.read_data('sharedData', "creates")
                create_table_start = time()
                for table in self.tables:
                    mysqldb.create_table(cursor, queries_create['mysql'][table])
                conn.commit()
                create_table_end = time()
                create_table_time = create_table_end - create_table_start
            except Exception, e:
                print e
                print table
                print "create a table mysql problem"

        print "-------------------------------------------------------------"

        # Insert all
        inserts_time_all = {}
        try:
            queries_insert = self.read_data('sharedData', 'inserts')
            insert_all_data_time = 0
            for table_name in self.tables:
                print "inserting all" + table_name
                json_data = self.read_data('sharedData', table_name)
                if json_data:
                    query_insert = queries_insert['mysql'][table_name]
                    insert_all_start = time()
                    mysqldb.insert_all_data(cursor, query_insert, json_data)
                    conn.commit()
                    insert_all_end = time()
                    inserts_time_all[table_name] = insert_all_end - insert_all_start
                    insert_all_data_time += (insert_all_end - insert_all_start)
            inserts_time_all['total'] = insert_all_data_time
        except Exception, e:
            print e
            print table_name
            print "Insert all mysql problem"

        print "---------------------------------------------------------------"

        # Stats
        try:
            size = mysqldb.get_size(cursor, "")
        except Exception, e:
            print e
            print "get stats mysql problem"

        # Close connexion
        try:
            close_connexion_time_start = time()
            mysqldb.close_connexion(conn)
            close_connexion_time_end = time()
            close_connexion_time = close_connexion_time_end - close_connexion_time_start
        except Exception, e:
            close_connexion_time = 0
            print e
            print "Problem with close connexion"

        time_mysql_end = time()
        time_mysql = time_mysql_end - time_mysql_start

        try:
            # Results mysql
            time_results = {'create_connexion_time': create_connexion_time,
                            'close_connexion_time': close_connexion_time,
                            'create_database_time': create_database_time,
                            'create_table_time': create_table_time,
                            'inserts_time_all': inserts_time_all,
                            'inserts_time_one': inserts_time_one,
                            'delete_table_time': delete_table_time,
                            'size': size,
                            'total_time': time_mysql}

            # Write results mysql
            mysqldb.write_results(time_results, "inserts_mysql")

            print "----------------------------------------------------------"
            print time_results
        except Exception, e:
            print e
            print "Error time results"

        print "End inserting"

    # CRATE
    def insert_crate(self, host, one):
        print "WAIT 60 SECONDS"
        sleep(60)
        print "START"

        time_crate_start = time()
        crate_sql = Crate()
        # Create connexion
        try:
            create_connexion_start = time()
            conn = crate_sql.create_connexion(user='',
                                              password='',
                                              host='',
                                              database='',
                                              string_connect=host)
            cursor = conn.cursor()
            create_connexion_end = time()
            create_connexion_time = create_connexion_end - create_connexion_start
        except Exception, e:
            print e
            print "connection with crate problem"
        print "connected"
        # Create database
        try:
            create_database_start = time()
            create_database_end = time()
            create_database_time = create_database_end - create_database_start
        except Exception, e:
            print e
            print "create a database crate problem"

        # Delete all self.tables if exits
        try:
            for table_name in self.tables:
                self.delete_table(cursor, table_name)
            conn.commit()
        except Exception, e:
            print e
            print "Deleting crate problem"

        # Create self.tables
        try:
            queries_create = self.read_data('sharedData', "creates")
            create_table_start = time()
            for table in self.tables:
                crate_sql.create_table(cursor, queries_create['crate'][table])
            conn.commit()
            create_table_end = time()
            create_table_time = create_table_end - create_table_start
        except Exception, e:
            print e
            print table
            print "create a table crate problem"

        print "----------------------------------------------------------"

        if one:
            # Insert all one per one
            inserts_time_one = {}
            try:
                insert_one_data_time = 0
                for table_name in self.tables:
                    print "inserting one" + table_name
                    json_data = self.read_data('sharedData', table_name)[:100]
                    if json_data:
                        json_example = json_data[0]
                        query_insert = 'INSERT INTO ' + table_name + ' ('
                        for k in json_example:
                            query_insert += k + ","
                        query_insert = query_insert[:len(query_insert) - 1] + ") " + "VALUES ("
                        for i in json_example:
                            query_insert += "?,"
                        query_insert = query_insert[:len(query_insert) - 1] + ")"
                        values = []
                        for j in json_data:
                            item = ()
                            for key, value in j.items():
                                item = item + (value,)
                        insert_one_start = time()
                        for v in values:
                            crate_sql.insert_one_data(cursor, query_insert, v)
                        conn.commit()
                        insert_one_end = time()
                        count_documents = len(json_data)
                        time_aux = {'time': insert_one_end - insert_one_start,
                                    'count': count_documents}
                        inserts_time_one[table_name] = time_aux
                        insert_one_data_time += (insert_one_end - insert_one_start)
                inserts_time_one['total'] = insert_one_data_time
            except Exception, e:
                print e
                print table_name
                print j
                print "Insert one crate problem"

            print "----------------------------------------------------------"

            # Delete all
            try:
                delete_table_start = time()
                for table_name in self.tables:
                    self.delete_table(cursor, table_name)
                conn.commit()
                delete_table_end = time()
                delete_table_time = delete_table_end - delete_table_start
            except Exception, e:
                print e
                print table_name
                print "Deleting crate problem"

            print "-------------------------------------------------------------"

            # Create self.tables
            try:
                queries_create = self.read_data('sharedData', "creates")
                create_table_start = time()
                for table in self.tables:
                    cursor.execute(queries_create['crate'][table])
                conn.commit()
                create_table_end = time()
                create_table_time = create_table_end - create_table_start
            except Exception, e:
                print e
                print table
                print "create a table crate problem"

            print "----------------------------------------------------------"

        # Insert all
        inserts_time_all = {}
        try:
            insert_all_data_time = 0
            for table_name in self.tables:
                print "inserting all" + table_name
                json_data = self.read_data('sharedData', table_name)
                if json_data:
                    len_json_data = len(json_data)
                    count1 = 0
                    count2 = 1000
                    count3 = 0
                    ok = True
                    insert_all_start = time()
                    while ok:
                        if count3 > len_json_data:
                            count3 = len_json_data
                            ok = False
                        else:
                            count3 = count1 + count2
                            aux = json_data[count1:count3 - 1]
                            count1 = count3
                            if aux and len(aux) > 0:
                                values = []
                                for j in aux:
                                    query_insert = 'INSERT INTO ' + table_name + ' ('
                                    for k in j:
                                        query_insert += k + ","
                                    query_insert = query_insert[:len(query_insert) - 1] + ") " + "VALUES ("
                                    for i in j:
                                        query_insert += "?,"
                                    query_insert = query_insert[:len(query_insert) - 1] + ")"
                                    item = ()
                                    for key, value in j.items():
                                        item = item + (value,)
                                    values.append(item)
                                try:
                                    crate_sql.insert_all_data(cursor, query_insert, values)
                                    conn.commit()
                                except Exception, e:
                                    print query_insert
                                    print e
                            else:
                                ok = False
                    insert_all_end = time()
                    inserts_time_all[table_name] = insert_all_end - insert_all_start
                    insert_all_data_time += (insert_all_end - insert_all_start)
            inserts_time_all['total'] = insert_all_data_time
        except Exception, e:
            print e
            print table_name
            print "Insert all crate problem"

        print "---------------------------------------------------------------"

        # Stats
        try:
            size = crate_sql.get_size("", None)
        except Exception, e:
            print e
            print "get stats crate problem"

        # Close connexion
        try:
            close_connexion_time_start = time()
            cursor.close()
            crate_sql.close_connexion(conn)
            close_connexion_time_end = time()
            close_connexion_time = close_connexion_time_end - close_connexion_time_start
        except Exception, e:
            print e
            print "Problem with close connexion"

        time_crate_end = time()
        time_crate = time_crate_end - time_crate_start

        try:
            # Results crate
            time_results = {'create_connexion_time': create_connexion_time,
                            'close_connexion_time': close_connexion_time,
                            'create_database_time': create_database_time,
                            'create_table_time': create_table_time,
                            'inserts_time_all': inserts_time_all,
                            'inserts_time_one': inserts_time_one,
                            'delete_table_time': delete_table_time,
                            'size': size,
                            'total_time': time_crate}

            # Write results crate
            crate_sql.write_results(time_results, "inserts_crate")

            print "----------------------------------------------------------"
            print time_results
        except Exception, e:
            print e
            print "Error time results"
        print "End inserting"

    # INSERT MYSQL AMAZON
    def insert_mysql_amazon(self):
        print "START"
        host_amazon_mysql = self.data['host_amazon_mysql']
        user_amazon_mysql = self.data['user_amazon_mysql']
        password_amazon_mysql = self.data['password_amazon_mysql']
        dbname_amazon_mysql = self.data['dbname_amazon_mysql']
        self.insert_mysql(host=host_amazon_mysql, user=user_amazon_mysql, password=password_amazon_mysql,
                          database=dbname_amazon_mysql, one=False)
        print "END"
