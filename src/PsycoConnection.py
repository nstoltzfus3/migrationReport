import psycopg2


class PsycoConnection:
    '''
    Custom wrapper for Psycopg2 connection with a postgres database. Facilitates making connections and
    maintaining cursors for ease of use.
    '''

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
        '''
        Creates a connection to a postgres database via psycopg2.
        :return:
        '''
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
        '''
        Queries a database with the given query string. Psycopg2 keeps track of the response in memory.
        :param psqlQuery: a string that contains postgres commands.
        :param n: size of the data chunk to increment.
        :return:
        '''
        print(psqlQuery)
        print(self.tablename)
        self.cursor.execute(psqlQuery % self.tablename)
        self.n = n

    def fetchNext(self):
        '''
        Fetches the next chunk of data from the last query that was made.
        :return:
        '''
        self.data = self.cursor.fetchmany(size=self.n)
        return self.data

    def close(self):
        '''
        Closes the connection to the docker psql server.
        :return:
        '''
        self.cursor.close()
        self.conn.close()
