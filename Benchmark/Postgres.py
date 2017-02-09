import psycopg2
from SqlDb import SqlDb


class Postgres(SqlDb):

    def create_connexion(self, user, password, host, database, string_connect):
        return psycopg2.connect(string_connect)
        # TODO

    def get_size(self, cursor, db):
        q = "select pg_database_size(%s);"
        cursor.execute(q, (db,))
        res = cursor.fetchone()
        cursor.close()
        return str(res[0])
