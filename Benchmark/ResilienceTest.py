import json
import time
import sys
# import dockerhook
import thread
# import spur
from Couch import Couch
from Mongo import Mongo
from Postgres import Postgres
from Mysqldb import Mysqldb
from Crate import Crate
import threading
import commands
import random


reload(sys)
sys.setdefaultencoding('utf8')


with open('config.json') as data_file:
    data = json.load(data_file)


host_resilience_mongo = data['host_resilience_mongo']
port_resilience_mongo = data['port_resilience_mongo']
host_resilience_couch = data['host_resilience_couch']
port_resilience_couch = None
host_resilience_postgres = data['host_resilience_postgres']
host_resilience_crate = data['host_resilience_crate']
host_resilience_mysql = data['host_resilience_mysql']
user_resilience_mysql = data['user_resilience_mysql']
password_resilience_mysql = data['password_resilience_mysql']
database_resilience_mysql = data['database_resilience_mysql']


@staticmethod
def delete_table(cursor1, table_name):
    query_delete = 'DROP TABLE IF EXISTS ' + table_name
    cursor1.execute(query_delete)

def kill_container(name, delay):
    try:
        pass
        # time.sleep(5)
        # time.sleep(delay)
        # r1 = dockerhook.check_output_remote(['docker', 'ps', '-aqf' ,'"name=' + name + '"'])
        # r1 = dockerhook.check_output_remote(['docker', 'images'])
        # r1 = commands.getoutput('docker ps -aqf "name=' + name + '"')
        # shell = spur.SshShell(
        #   hostname="staff-ramz-0.zhaw.ch",
        #   username="manuel",
        #   private_key_file="./known_hosts"
        # )
        # with shell:
        #    r1 = shell.run(["echo", "-n", "hello"])
        # print(r1.output)  # prints hello
        # print "kill container " + name
        # print r1
        # r2= commands.getoutput('docker kill --signal="SIGKILL" ' + r1)
        # r2 = dockerhook.check_output_remote('docker kill --signal="SIGKILL" ' + r1)
        # print r2
    except Exception, e:
        print e
        print "problem with kill container"


def read_data(folder_name, name):
    try:
        with open("/" + folder_name + "/" + name + ".json") as json_file:
            data1 = json.load(json_file)
    except Exception, e:
        print e
        print "Error reading " + name
        data1 = []
    return data1


def get_count(name):
    data1 = read_data('results', name)
    return data1['count']


# MONGO
# INSERT ONE PER ONE
# MEMORY PROBLEM
def mongo_test(name_collection, name_test, type_insert, type_test):
    name_database = "arkis"
    mongo = Mongo()
    # CONNECT

    conn = mongo.create_connexion(host_resilience_mongo, port_resilience_mongo)

    # GET OR CREATE DATABASE
    database = mongo.create_database(conn, name_database)

    # CLEAN DATABASE
    try:
        mongo.delete_collection(database[name_collection])
    except Exception, e:
        print e
        print "problem connect mongo"

    # CREATE COLLECTION
    mongo.create_collection(database, name_collection)

    # GET DATA TO INSERT
    json_data = read_data('sharedData/data', 'documents')
    count = 0

    # INSERT FIRST 1000
    first_1000 = json_data[:1000]
    time_1000_start = time.time()
    for j in first_1000:
        mongo.insert_one_data(database[name_collection], j)
        count += 1
    time_1000 = time.time() - time_1000_start
    rest_data = json_data[1001:]
    extimated_time = (len(rest_data) * time_1000) / 1000
    delay = round(extimated_time / 10)
    print "Inserted 1000 documents"
    print "Extimated time and delay"
    print extimated_time
    print delay
    print "--------------------------------------------------------"
    try:
        print mongo.get_all_data(database[name_collection]).count()
        print count
    except Exception, e:
        print e
        print "Problem getting the first 1000 documents"

    if type_test == 'kill':
        try:
            print "start thread"
            print delay
            thread.start_new_thread(kill_container, ('mongo_resilience', delay,))
        except Exception, e:
            print e
            print "Error: unable to start thread"

    # Size of rest_data is more that 500 mb, limit of container is 60
    print "START inserting "
    print len(rest_data)
    if type_insert == "one":
        try:
            while True:
                for j in rest_data:
                    j['ZRandom'] = random.random()
                    mongo.insert_one_data(database[name_collection], j)
                    count += 1
                    if count % 10000 == 0:
                        print count
                print "Inserting with success !!!!!!!!!!"
        except Exception, e:
            print count
            print e
            print "Problem inserting data in mongo"
            try:
                conn.close()
            except Exception, e:
                print e
                print "Problem closing connexion"
    else:
        try:
            mongo.insert_all_data(database[name_collection], rest_data)
            print "Inserting with succes !!!!!!!!!!"
        except Exception, e:
            print e
            print "Problem inserting data in mongo"
            try:
                conn.close()
            except Exception, e:
                print e
                print "Problem closing connexion"

    mongo.write_results({"count": count}, name_test)
    print "end"


def mongo_check_data(name_collection, name_test):
    # CONNECT AND GET COLLECTION
    name_database = "arkis"
    mongo = Mongo()
    conn = mongo.create_connexion(host_resilience_mongo, port_resilience_mongo)
    database = mongo.create_database(conn, name_database)
    mongo.create_collection(database, name_collection)

    # GET COUNT AND GET REAL COUNT
    try:
        r = mongo.get_all_data(database[name_collection])
        real_count = r.count()
        count = get_count(name_test)
        res = {'real_count': real_count, 'count': count}
        mongo.write_results(res, name_test)
        print real_count
        print count
    except Exception, e:
        print e
        print "Problem geting data"


def mongo_one_memory_test1():
    name_collection = "resilicience_memory_collection"
    name_test = "mongo_one_memory_test"
    mongo_test(name_collection, name_test, "one", "memory")


def mongo_one_memory_test2():
    name_collection = "resilicience_memory_collection"
    name_test = "mongo_one_memory_test"
    mongo_check_data(name_collection, name_test)


# KILL CONTAINER
def mongo_one_kill_container_test1():
    name_collection = "resilicience_kill_collection"
    name_test = "mongo_one_kill_container"
    mongo_test(name_collection, name_test, "one", 'kill')


def mongo_one_kill_container_test2():
    name_collection = "resilicience_kill_collection"
    name_test = "mongo_one_kill_container"
    mongo_check_data(name_collection, name_test)


# INSERT ALL
# MEMORY PROBLEM
def mongo_all_memory_test1():
    name_collection = "resilicience_memory_collection"
    name_test = "mongo_all_memory_test"
    mongo_test(name_collection, name_test, "all", "memory")


def mongo_all_memory_test2():
    name_collection = "resilicience_memory_collection"
    name_test = "mongo_all_memory_test"
    mongo_check_data(name_collection, name_test)


# KILL CONTAINER
def mongo_all_kill_container_test1():
    name_collection = "resilicience_kill_collection"
    name_test = "mongo_all_kill_container"
    mongo_test(name_collection, name_test, "all", "kill")


def mongo_all_kill_container_test2():
    name_collection = "resilicience_kill_collection"
    name_test = "mongo_all_kill_container"
    mongo_check_data(name_collection, name_test)


# COUCH
# INSERT ONE PER ONE
# MEMORY PROBLEM
def couch_test(name_collection, name_test, type_insert, type_test):
    time.sleep(10)
    name_database = "arkis"
    couch = Couch()
    # CONNECT
    conn = couch.create_connexion(host_resilience_couch, port_resilience_couch)
    try:
        conn.delete(name_collection)
    except Exception, e:
        print e
        print "problem delete couch"
    # GET OR CREATE DATABASE
    couch.create_database(conn, name_collection)

    # CREATE COLLECTION
    # couch.create_collection(database, name_collection)

    # GET DATA TO INSERT
    json_data = read_data('sharedData/data', 'documents')
    count = 0

    # INSERT FIRST 1000
    first_1000 = json_data[:1000]
    try:
        couch.insert_all_data(conn[name_collection], first_1000)
    except Exception, e:
        print e
        print "Problem inserting first 1000 documents"

    print "Inserted 1000 documents"
    print "--------------------------------------------------------"
    try:
        r = len(couch.get_all_data(conn[name_collection]))
        count = r
        print r
    except Exception, e:
        print e
        print "Problem getting the first 1000 documents"

    rest_data = json_data[1001:51001]
    # Size of rest_data is more that 500 mb, limit of container is 60
    print "START"
    if type_insert == "one":
        try:
            for j in rest_data:
                couch.insert_one_data(conn[name_collection], j)
                count += 1
            print "Inserting with succes !!!!!!!!!!"
        except Exception, e:
            print e
            print "Problem inserting data in couch"
            try:
                conn.close()
            except Exception, e:
                print e
                print "Problem closing connexion"
    else:
        try:
            couch.insert_all_data(conn[name_collection], rest_data)
            print "Inserting with succes !!!!!!!!!!"
        except Exception, e:
            print e
            print "Problem inserting data in couch"
            try:
                conn.close()
            except Exception, e:
                print e
                print "Problem closing connexion"

    couch.write_results({"count": count}, name_test)
    print "end"


def couch_check_data(name_collection, name_test):
    # CONNECT AND GET COLLECTION
    name_database = "arkis"
    couch = Couch()
    conn = couch.create_connexion(host_resilience_couch, port_resilience_couch)
    # couch.create_collection(database, name_collection)

    # GET COUNT AND GET REAL COUNT
    try:
        r = couch.get_all_data(conn[name_collection])
        real_count = len(r)
        count = get_count(name_test)
        res = {'real_count': real_count, 'count': count}
        couch.write_results(res, name_test)
        print real_count
        print count
    except Exception, e:
        print e
        print "Problem getting data"


def couch_disk_size_test1():
    time.sleep(10)
    couch = Couch()
    # CONNECT
    conn = couch.create_connexion(host_resilience_couch, port_resilience_couch)
    try:
        conn.delete('documents')
    except Exception, e:
        print e
        print "problem delete couch"
    # GET OR CREATE DATABASE
    couch.create_database(conn, 'documents')

    # CREATE COLLECTION
    # couch.create_collection(database, name_collection)

    # GET DATA TO INSERT
    json_data = read_data('sharedData/data', 'documents')
    count = 0
    while True:
        try:
            for j in json_data:
                try:
                    couch.insert_one_data(conn['documents'], j)
                    count += 1
                    print count
                except Exception, e:
                    print e
                    print j
                    conn = couch.create_connexion(conn, 'documents')
            print "Inserting with succes !!!!!!!!!!"
        except Exception, e2:
            print e2
            print "Problem inserting data in couch"
            try:
                conn.close()
            except Exception, e1:
                print e1
                print "Problem closing connexion"



def couch_disk_size_test2():
    name_collection = 'documents'
    name_test = 'couch_size_disk'
    couch_check_data(name_collection, name_test)


def couch_one_memory_test1():
    name_collection = "resilicience_memory_collection"
    name_test = "couch_one_memory_test"
    couch_test(name_collection, name_test, "one", "memory")


def couch_one_memory_test2():
    name_collection = "resilicience_memory_collection"
    name_test = "couch_one_memory_test"
    couch_check_data(name_collection, name_test)


# KILL CONTAINER
def couch_one_kill_container_test1():
    name_collection = "resilicience_kill_collection"
    name_test = "couch_one_kill_container"
    couch_test(name_collection, name_test, "one", "kill")


def couch_one_kill_container_test2():
    name_collection = "resilicience_kill_collection"
    name_test = "couch_one_kill_container"
    couch_check_data(name_collection, name_test)


# INSERT ALL
# MEMORY PROBLEM
def couch_all_memory_test1():
    name_collection = "resilicience_memory_collection"
    name_test = "couch_all_memory_test"
    couch_test(name_collection, name_test, "all", "memory")


def couch_all_memory_test2():
    name_collection = "resilicience_memory_collection"
    name_test = "couch_all_memory_test"
    couch_check_data(name_collection, name_test)


# KILL CONTAINER
def couch_all_kill_container_test1():
    name_collection = "resilicience_kill_collection"
    name_test = "couch_all_kill_container"
    couch_test(name_collection, name_test, "all", "kill")


def couch_all_kill_container_test2():
    name_collection = "resilicience_kill_collection"
    name_test = "couch_all_kill_container"
    couch_check_data(name_collection, name_test)


# SQL
def sql_test(name_test, type_insert, type_test, commit, db):
    if db == 'postgres':
        database = Postgres()
        # CONNECT

        conn = database.create_connexion(user='postgres',
                                         password='postgres',
                                         host='host',
                                         database='arkis',
                                         string_connect=host_resilience_postgres)
    elif db == 'crate':
        database = Crate()
        time.sleep(30)
        conn = database.create_connexion(user='',
                                         password='',
                                         host='',
                                         database='',
                                         string_connect=host_resilience_crate)
    else:
        database = Mysqldb()
        time.sleep(30)
        conn = database.create_connexion(user=user_resilience_mysql,
                                         password=password_resilience_mysql,
                                         host=host_resilience_mysql,
                                         database=database_resilience_mysql,
                                         string_connect="")
    cursor = conn.cursor()
    # GET OR CREATE DATABASE
    # CLEAN DATABASE
    try:
        delete_table(cursor, 'documents')
    except Exception, e:
        print e
        print "problem connect sql"

    # CREATE TABLE
    queries_create = read_data('sharedData', "creates")
    database.create_table(cursor, queries_create[db]['documents'])

    # GET DATA TO INSERT
    json_data = read_data('sharedData/data', 'documents')
    count = 0

    # INSERT FIRST 1000
    first_1000 = json_data[:1000]
    time_1000_start = time.time()
    if db == 'crate':
        json_example = json_data[0]
        query_insert = 'INSERT INTO documents ('
        for k in json_example:
            query_insert += k + ","
        query_insert = query_insert[:len(query_insert) - 1] + ") " + "VALUES ("
        for i in json_example:
            query_insert += "?,"
        query_insert = query_insert[:len(query_insert) - 1] + ")"
        values = []
        for j in first_1000:
            item = ()
            for key, value in j.items():
                item = item + (value,)
            values.append(item)
        for v in values:
            cursor.execute(query_insert, v)
            conn.commit()
            count += 1
    else:
        queries_insert = read_data('sharedData', 'inserts')
        query_insert = queries_insert[db]['documents']
        for j in first_1000:
            database.insert_one_data(cursor, query_insert, j)
            conn.commit()
            count += 1

    time_1000 = time.time() - time_1000_start
    rest_data = json_data[1001:]
    extimated_time = (len(rest_data) * time_1000) / 1000
    delay = round(extimated_time / 10)

    print query_insert
    print "Inserted 1000 documents"
    print "Extimated time and delay"
    print extimated_time
    print delay
    print "--------------------------------------------------------"
    try:
        r = database.get_all_data(cursor, 'documents')
        count = len(r)
        print count
    except Exception, e:
        print e
        print "Problem getting the first 1000 documents"

    if type_test == 'kill':
        try:
            print "start thread"
            print delay
            thread.start_new_thread(kill_container, ('couch_resilience', delay,))
        except Exception, e:
            print e
            print "Error: unable to start thread"

    # Size of rest_data is more that 500 mb, limit of container is 60
    print "START inserting "
    print len(rest_data)
    if type_insert == "one":
        try:
            if db == 'crate':
                values = []
                for j in rest_data:
                    item = ()
                    for key, value in j.items():
                        item = item + (value,)
                    values.append(item)
                for v in values:
                    cursor.execute(query_insert, v)
                    count += 1
                    if commit:
                        conn.commit()
                    if count % 10000 == 0:
                        print count
            else:
                for j in rest_data:
                    database.insert_one_data(cursor, query_insert, j)
                    count += 1
                    if commit:
                        conn.commit()
                    if count % 10000 == 0:
                        print count
                print "Inserting with succes !!!!!!!!!!"
        except Exception, e:
            print count
            print e
            print "Problem inserting data in sql"
            try:
                conn.close()
            except Exception, e:
                print e
                print "Problem closing connexion"
    else:
        try:
            if db == 'crate':
                values = []
                for j in rest_data[:10000]:
                    item = ()
                    for key, value in j.items():
                        item = item + (value,)
                    values.append(item)
                cursor.executemany(query_insert, values)
                conn.commit()
            else:
                database.insert_all_data(cursor, query_insert, rest_data)
                conn.commit()
            print "Inserting with succes !!!!!!!!!!"
        except Exception, e:
            print e
            print "Problem inserting data in sql"
            try:
                conn.close()
            except Exception, e:
                print e
                print "Problem closing connexion"

    database.write_results({"count": count}, name_test)
    print "end"


def sql_check_data(name_test, db):
    # CONNECT AND GET COLLECTION
    if db == 'postgres':
        database = Postgres()
        # CONNECT

        conn = database.create_connexion(user='user',
                                         password='password',
                                         host='host',
                                         database='database',
                                         string_connect=host_resilience_postgres)
    elif db == 'crate':
        database = Crate()
        time.sleep(30)
        conn = database.create_connexion(user='',
                                         password='',
                                         host='',
                                         database='',
                                         string_connect=host_resilience_crate)
    else:
        database = Mysqldb()
        time.sleep(30)
        conn = database.create_connexion(user=user_resilience_mysql,
                                         password=password_resilience_mysql,
                                         host=host_resilience_mysql,
                                         database=database_resilience_mysql,
                                         string_connect="")
    cursor = conn.cursor()

    # GET COUNT AND GET REAL COUNT
    try:
        r = database.get_all_data(cursor, 'documents')
        real_count = len(r)
        print real_count
        count = get_count(name_test)
        res = {'real_count': real_count, 'count': count}
        database.write_results(res, name_test)
        print real_count
        print count
    except Exception, e:
        print real_count
        print e
        print "Problem getting data"


def sql_disk_size_test1(db):
    if db == 'postgres':
        database = Postgres()
        # CONNECT
        conn = database.create_connexion(user='user',
                                         password='password',
                                         host='host',
                                         database='arkis',
                                         string_connect=host_resilience_postgres)
    elif db == 'crate':
        database = Crate()
        time.sleep(30)
        conn = database.create_connexion(user='',
                                         password='',
                                         host='',
                                         database='',
                                         string_connect=host_resilience_crate)
    else:
        database = Mysqldb()
        time.sleep(30)
        conn = database.create_connexion(user=user_resilience_mysql,
                                         password=password_resilience_mysql,
                                         host=host_resilience_mysql,
                                         database=database_resilience_mysql,
                                         string_connect="")
    cursor = conn.cursor()
    try:
        delete_table(cursor, 'documents')
    except Exception, e:
        print e
        print "problem connect sql"

        # CREATE TABLE
    queries_create = read_data('sharedData', "creates")
    database.create_table(cursor, queries_create[db]['documents'])

    # GET DATA TO INSERT
    aux_data = read_data('sharedData/data', 'documents')
    json_data = []
    for j in aux_data:
        j['ZBlob'] = j['ZBlob'].encode('unicode_escape')
        json_data.append(j)
    count = 0

    while True:
        if db == 'crate':
            json_example = json_data[0]
            query_insert = 'INSERT INTO documents ('
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
                values.append(item)
            for v in values:
                try:
                    cursor.execute(query_insert, v)
                    conn.commit()
                    count += 1
                except Exception, e:
                    print e
                    print count
                    print v

        else:
            queries_insert = read_data('sharedData', 'inserts')
            query_insert = queries_insert[db]['documents']
            for j in json_data:
                try:
                    database.insert_one_data(cursor, query_insert, j)
                    conn.commit()
                    count += 1
                except Exception, e:
                    print e
                    print count
                    print j


def sql_disk_size_test2(db):
    sql_check_data("", db)


# POSTGRES
# INSERT ONE PER ONE
# MEMORY PROBLEM
# WITH COMMIT
def postgres_one_commit_memory_test1():
    name_test = 'postgres_one_commit_memory'
    sql_test(name_test, 'one', 'memory', True, 'postgres')


def postgres_one_commit_memory_test2():
    name_test = 'postgres_one_commit_memory'
    sql_check_data(name_test, 'postgres')


# WITHOUT COMMIT
def postgres_one_memory_test1():
    name_test = 'postgres_one_memory'
    sql_test(name_test, 'one', 'memory', False, 'postgres')


def postgres_one_memory_test2():
    name_test = 'postgres_one_memory'
    sql_check_data(name_test, 'postgres')


# KILL CONTAINER
# WITH COMMIT
def postgres_one_commit_kill_container_test1():
    name_test = 'postgres_one_commit_kill_container'
    sql_test(name_test, 'one', 'kill', True, 'postgres')


def postgres_one_commit_kill_container_test2():
    name_test = 'postgres_one_commit_kill_container'
    sql_check_data(name_test, 'postgres')


# WITHOUT COMMIT
def postgres_one_kill_container_test1():
    name_test = 'postgres_one_kill_container'
    sql_test(name_test, 'one', 'kill', False, 'postgres')


def postgres_one_kill_container_test2():
    name_test = 'postgres_one_kill_container'
    sql_check_data(name_test, 'postgres')


# INSERT ALL
# MEMORY PROBLEM
def postgres_all_memory_test1():
    name_test = 'postgres_all_memory'
    sql_test(name_test, 'all', 'memory', True, 'postgres')


def postgres_all_memory_test2():
    name_test = 'postgres_all_memory'
    sql_check_data(name_test, 'postgres')


# KILL CONTAINER
def postgres_all_kill_container_test1():
    name_test = 'postgres_all_kill_container'
    sql_test(name_test, 'all', 'kill', True, 'postgres')


def postgres_all_kill_container_test2():
    name_test = 'postgres_all_kill_container'
    sql_check_data(name_test, 'postgres')


# MYSQL
# INSERT ONE PER ONE
# MEMORY PROBLEM
# WITH COMMIT
def mysql_one_commit_memory_test1():
    name_test = 'mysql_one_commit_memory'
    sql_test(name_test, 'one', 'memory', True, 'mysql')


def mysql_one_commit_memory_test2():
    name_test = 'mysql_one_commit_memory'
    sql_check_data(name_test, 'mysql')


# WITHOUT COMMIT
def mysql_one_memory_test1():
    name_test = 'mysql_one_memory'
    sql_test(name_test, 'one', 'memory', False, 'mysql')


def mysql_one_memory_test2():
    name_test = 'mysql_one_memory'
    sql_check_data(name_test, 'mysql')


# KILL CONTAINER
# WITH COMMIT
def mysql_one_commit_kill_container_test1():
    name_test = 'mysql_one_commit_kill_container'
    sql_test(name_test, 'one', 'kill', True, 'mysql')


def mysql_one_commit_kill_container_test2():
    name_test = 'mysql_one_commit_kill_container'
    sql_check_data(name_test, 'mysql')


# WITHOUT COMMIT
def mysql_one_kill_container_test1():
    name_test = 'mysql_one_kill_container'
    sql_test(name_test, 'one', 'kill', False, 'mysql')


def mysql_one_kill_container_test2():
    name_test = 'mysql_one_kill_container'
    sql_check_data(name_test, 'mysql')


# INSERT ALL
# MEMORY PROBLEM
def mysql_all_memory_test1():
    name_test = 'mysql_all_memory'
    sql_test(name_test, 'all', 'memory', True, 'mysql')


def mysql_all_memory_test2():
    name_test = 'mysql_all_memory'
    sql_check_data(name_test, 'mysql')


# KILL CONTAINER
def mysql_all_kill_container_test1():
    name_test = 'mysql_all_kill_container'
    sql_test(name_test, 'all', 'kill', True, 'mysql')


def mysql_all_kill_container_test2():
    name_test = 'mysql_all_kill_container'
    sql_check_data(name_test, 'mysql')


# CRATE
# INSERT ONE PER ONE
# MEMORY PROBLEM
# WITH COMMIT
def crate_one_commit_memory_test1():
    name_test = 'crate_one_commit_memory'
    sql_test(name_test, 'one', 'memory', True, 'crate')


def crate_one_commit_memory_test2():
    name_test = 'crate_one_commit_memory'
    sql_check_data(name_test, 'crate')


# WITHOUT COMMIT
def crate_one_memory_test1():
    name_test = 'crate_one_memory'
    sql_test(name_test, 'one', 'memory', False, 'crate')


def crate_one_memory_test2():
    name_test = 'crate_one_memory'
    sql_check_data(name_test, 'crate')


# KILL CONTAINER
# WITH COMMIT
def crate_one_commit_kill_container_test1():
    name_test = 'crate_one_commit_kill_container'
    sql_test(name_test, 'one', 'kill', True, 'crate')


def crate_one_commit_kill_container_test2():
    name_test = 'crate_one_commit_kill_container'
    sql_check_data(name_test, 'crate')


# WITHOUT COMMIT
def crate_one_kill_container_test1():
    name_test = 'crate_one_kill_container'
    sql_test(name_test, 'one', 'kill', False, 'crate')


def crate_one_kill_container_test2():
    name_test = 'crate_one_kill_container'
    sql_check_data(name_test, 'crate')


# INSERT ALL
# MEMORY PROBLEM
def crate_all_memory_test1():
    name_test = 'crate_all_memory'
    sql_test(name_test, 'all', 'memory', True, 'crate')


def crate_all_memory_test2():
    name_test = 'crate_all_memory'
    sql_check_data(name_test, 'crate')


# KILL CONTAINER
def crate_all_kill_container_test1():
    name_test = 'crate_all_kill_container'
    sql_test(name_test, 'all', 'kill', True, 'crate')


def crate_all_kill_container_test2():
    name_test = 'crate_all_kill_container'
    sql_check_data(name_test, 'crate')

# TEST
# MONGO
# mongo_one_kill_container_test1()
# mongo_one_kill_container_test2()

# mongo_all_kill_container_test1()
# mongo_all_kill_container_test2()

# mongo_all_memory_test1()
# mongo_all_memory_test2()

# mongo_one_memory_test1()
# mongo_one_memory_test2()

# COUCHDB
# couch_one_kill_container_test1()
# couch_one_kill_container_test2()

# couch_all_kill_container_test1()
# couch_all_kill_container_test2()

# couch_all_memory_test1()
# couch_all_memory_test2()

# couch_one_memory_test1()
# couch_one_memory_test2()

# POSTGRES
# postgres_one_commit_memory_test1()
# postgres_one_commit_memory_test2()
#
# postgres_one_memory_test1()
# postgres_one_memory_test2()
#
# postgres_one_commit_kill_container_test1()
# postgres_one_commit_kill_container_test2()
#
# postgres_one_kill_container_test1()
# postgres_one_kill_container_test2()
#
# postgres_all_memory_test1()
# postgres_all_memory_test2()
#
# postgres_all_kill_container_test1()
# postgres_all_kill_container_test2()


# CRATE
# crate_one_commit_memory_test1()
# crate_one_commit_memory_test2()
#
# crate_one_memory_test1()
# crate_one_memory_test2()
#
# crate_one_commit_kill_container_test1()
# crate_one_commit_kill_container_test2()
#
# crate_one_kill_container_test1()
# crate_one_kill_container_test2()
#
# crate_all_memory_test1()
# crate_all_memory_test2()
#
# crate_all_kill_container_test1()
# crate_all_kill_container_test2()
#
#
# MYSQL
# mysql_one_commit_memory_test1()
# mysql_one_commit_memory_test2()
#
# mysql_one_memory_test1()
# mysql_one_memory_test2()
#
# mysql_one_commit_kill_container_test1()
# mysql_one_commit_kill_container_test2()
#
# mysql_one_kill_container_test1()
# mysql_one_kill_container_test2()
#
# mysql_all_memory_test1()
# mysql_all_memory_test2()
#
# mysql_all_kill_container_test1()
# mysql_all_kill_container_test2()
#
# couch_disk_size_test1()
# couch_disk_size_test2()
# sql_disk_size_test1('postgres')
# sql_disk_size_test2('postgres')
# sql_disk_size_test1('mysql')
# sql_disk_size_test2('mysql')
# sql_disk_size_test1('crate')
# sql_disk_size_test2('crate')
