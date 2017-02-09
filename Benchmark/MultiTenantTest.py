import threading
import json
from time import time


class MultiTenantTest:

    def __init__(self):
        with open('config.json') as data_file:
            data = json.load(data_file)
        self.host = data['host_multitenant']
        self.port = data['port_multitenant']
        self.name = data['name_multitenant']
        print "START MULTI-TENANT"

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

    def aux_selects_user(self, db, user):
        # todo
        pass

    def aux_selects(self, db):
        # todo
        pass

    def select_mt(self, host, port, conn, times_all, name_file, number_loops, user, number_users, option):
        # TODO Get the database
        db = conn
        times = []
        for i in range(0, number_loops):
            print("Iteration " + str(i) + "for user " + str(user))
            if option == 'A':
                t = self.aux_selects_user(db, user)
            else:
                t = self.aux_selects(db)
            times.append(t)
        times_all.append(times)
        if len(times_all) == number_users:
            print times_all
        try:
            f = open("../results/" + name_file + str(user) + ".json", "w")
            json.dump(times, f)
            f.close()
        except Exception:
            print("error saving results in .json")
        if len(times_all) == number_users:
            try:
                f = open("../results/" + name_file + str(user) + "_end.json", "w")
                json.dump(times_all, f)
                f.close()
            except Exception:
                print("error saving results in .json")
        return times

    def test_multitenant_selects(self, thread_bool, option, users_number, filename):
        dbs = []
        # TODO Add databases to dbs
        times = []
        if thread_bool:
            threads = []
            for i in range(0, users_number):
                t = threading.Thread(target=self.select_mt,
                                     args=(self.host, self.port, dbs[i], times, filename, 10, i, users_number, option))
                threads.append(t)
            for t in threads:
                t.start()
        else:
            times_1 = []
            for i in range(0, users_number):
                print "start without thread " + str(i)
                times.append(self.select_mt(None, None, dbs[i], times_1, filename, 10, i, users_number, option))
                print "end without thread " + str(i)
            print times_1
        return times

    def insert_mt(self, db, colls, user, times, user_numbers, json_datas_users):
        print "start thread " + str(user)
        t_start = time()
        for coll in colls:
            print "Inserting " + coll + " for user " + str(user)
            try:
                pass
                # TODO Insert data
            except Exception, e:
                print e
                print "problem with user " + str(user) + "and coll " + coll
        t_end = time()
        time_total = t_end - t_start
        times.append(time_total)
        if len(times) == user_numbers:
            print times
        print "end thread " + str(user)
        return time_total

    def test_multitenant_inserts(self, thread_bool, users_number, option, rows_per_user):
        dbs = []
        # TODO Add databases to dbs
        times = []

        with open('config.json') as data_file:
            data = json.load(data_file)
        colls = data['coll_names_multitenant']
        if thread_bool:
            threads = []
            for s in range(0, users_number):
                json_datas_users = {}
                for coll in colls:
                    json_datas_users[coll] = self.read_data('sharedData', coll)
                t = threading.Thread(target=self.insert_mt, args=(dbs[s], colls, s, times, users_number,
                                                                  json_datas_users))
                threads.append(t)
                print "readed " + str(s)
            for t in threads:
                t.start()
        else:
            times_1 = []
            json_datas_users_array = []
            for a in range(0, users_number):
                json_datas_users = {}
                for coll in colls:
                    json_datas_users[coll] = self.read_data('sharedData', coll)
                json_datas_users_array.append(json_datas_users)
            print "All readed"
            for i in range(0, users_number):
                print "start without thread " + str(i)
                times.append(self.insert_mt(dbs[i], colls, i, times_1, users_number,
                                            json_datas_users_array[i]))
                print "end without thread " + str(i)
            print times_1
        return times


m = MultiTenantTest()
# m.test_multitenant_inserts(True, 10, 'A', 100000)
# m.test_multitenant_inserts(True, 10, 'B', 100000)
# m.test_multitenant_inserts(False, 10, 'C', 100000)

# print m.test_multitenant_selects(True, 'A', 10, 'awt_3')
# print m.test_multitenant_selects(False, 'A', 10, 'anot_3')

# print m.test_multitenant_selects(True, 'B', 10, 'bwt_3')
# print m.test_multitenant_selects(False, 'B', 10, 'bnot_3')

# print m.test_multitenant_selects(True, 'C', 10, 'cwt_3')
# print m.test_multitenant_selects(False, 'C', 10, 'cnot_3')
