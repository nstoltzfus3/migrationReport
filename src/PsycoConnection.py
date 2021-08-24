import psycopg2


class PsycoConnection:

    def __init__(self, PSQLdb):
        self.psqldb = PSQLdb
        self.conn = None
        self.establishConnections()
        self.cursor = None
        self.data = None
        self.tablename = ''
        self.n = 0

        if (self.conn):
            self.cursor = self.conn.cursor()

    def establishConnections(self):
            try:
                print("Database: %s running on port %d" % (self.psqldb.dbname, self.psqldb.port))

                conn = psycopg2.connect(
                    host="localhost",
                    database=self.psqldb.dbname,
                    user=self.psqldb.user,
                    password=self.psqldb.password,
                    port=self.psqldb.port
                )
                print("Psycopg2 connection established...")
                self.conn = conn
            except Exception as e:
                print(e)

    def query(self, psqlQuery:str, n=None):
        print(psqlQuery)
        print(self.tablename)
        self.cursor.execute(psqlQuery % self.tablename)
        self.n = n

    def fetchNext(self):
        self.data = self.cursor.fetchmany(size=self.n)
        return self.data

    def close(self):
        self.cursor.close()
        self.conn.close()
