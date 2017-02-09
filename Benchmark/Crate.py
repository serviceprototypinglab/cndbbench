from Mysqldb import Mysqldb
from crate import client


class Crate(Mysqldb):

    def create_connexion(self, user, password, host, database, string_connect):
        try:
            connection_10 = client.connect(string_connect, timeout=10)
            if connection_10:
                return connection_10
        except Exception, e:
            print e
            print "Error connecting"
        try:
            connection4 = client.connect('172.18.0.2:4200', timeout=10)
            if connection4:
                return connection4
        except Exception, e:
            print e
            print "Error connecting"
        try:
            print "in"
            connection1 = client.connect('0.0.0.0:4200', timeout=10)
            if connection1:
                return connection1
        except Exception, e:
            print e
            print "Error connecting"
        try:
            connection2 = client.connect('0.0.0.0:4300', timeout=10)
            if connection2:
                return connection2
        except Exception, e:
            print e
            print "Error connecting"
        try:
            connection3 = client.connect('0.0.0.0:5432', timeout=10)
            if connection3:
                return connection3
        except Exception, e:
            print e
            print "Error connecting"
        try:
            connection4 = client.connect('172.18.0.2:4200', timeout=10)
            if connection4:
                return connection4
        except Exception, e:
            print e
            print "Error connecting"
        try:
            connection5 = client.connect('172.18.0.2:4300', timeout=10)
            if connection5:
                return connection5
        except Exception, e:
            print e
            print "Error connecting"
        try:
            connection6 = client.connect('172.18.0.2:5432', timeout=10)
            if connection6:
                return connection6
        except Exception, e:
            print e
            print "Error connecting"

    def get_size(self, cursor, db):
        pass

