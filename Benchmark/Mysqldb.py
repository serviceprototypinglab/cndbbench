from SqlDb import SqlDb
import mysql.connector


class Mysqldb(SqlDb):

    def create_connexion(self, user, password, host, database, string_connect):
        if string_connect:
            return mysql.connector.connect(string_connect)
        if database:
            #return mysql.connector.connect(user=user, password=password, host=host, database=database)
            return mysql.connector.connect(user=user, password=password, host=host, database=database)
            # return mysql.connector.connect(user=user, password=password, host=host, port=3306, database=database)
        else:
            return mysql.connector.connect(user=user, password=password, host=host)

    def get_size(self, cursor, db):
        query_size = "SELECT Round(Sum(data_length + index_length) / 1024 / 1024, 1) " \
                     "FROM information_schema.tables GROUP BY table_schema"
        cursor.execute(query_size)
        size = cursor.fetchone()[0]
        return size
