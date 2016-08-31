from Mysqldb import Mysqldb
from crate import client


class Crate(Mysqldb):

    def create_connexion(self, user, password, host, database, string_connect):
        return client.connect(string_connect)

    def get_size(self, cursor, db):
        pass
