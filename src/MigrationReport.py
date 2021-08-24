import requests
import json
import docker
import subprocess
import os
import time
import requests

from PSQLServer import PSQLServer
from PsycoConnection import PsycoConnection
from DataComparator import DataComparator


class MigrationReport:

    def __init__(self, inifile, port=5432):
        '''
        The docker connect class takes in a list of filenames to construct a docker container communication platform.
        The filename is an initialization file that consists of the image names for each container.
        :param inifile: contains the names of the docker images that contain the postgres servers in question.
        '''

        client = docker.from_env()
        self.dbs = {}
        self.initialize(inifile, port)
        self.containers = client.containers.list()
        self.connections = []
        self.connectToPsql()
        self.verifyConnections()
        self.dataComparator = None

    def initialize(self, inifile, port):
        '''
        Consumes the initialization file to launch docker containers with corresponding PSQL servers on monotonically
        increasing ports on the host computer.
        :param inifile: name of the initialization file.
        :param port: starting port for multiple docker images to serve PSQL on.
        :return:
        '''
        bashCommand = "docker run -d -p %d:5432 %s"
        for line in open(inifile):
            # starts containers while distributing monotonically increasing ports.
            # Opted to bash launch the containers then scrape the containers off the daemon
            # Made because original documentation was given for bash.

            # client.containers.run(line, ports={'%d/tcp' % port : 5432}, detach=True)
            singlePSQLiniLine = line.strip().split(',')
            user = singlePSQLiniLine[0]
            dbname = singlePSQLiniLine[1]
            password = singlePSQLiniLine[2]
            self.dbs[singlePSQLiniLine[3]] = PSQLServer(user, dbname, password, port)
            process = subprocess.run((bashCommand % (port, line.split(',')[3])).split())
            port += 1

    def connectToPsql(self):
        '''
        Populates the list of connections by launching Psycopg2 and pointing it at each of the docker
        containers launched with initialize().
        :return:
        '''
        for container in self.containers[::-1]:
            PSQLdb = self.dbs[container.image.attrs['RepoTags'][0]]
            self.connections.append(PsycoConnection(PSQLdb))

    def verifyConnections(self):
        '''
        Tries to make a basic connection with each of the databases. Ensures that the databases' tables
        are named the same.
        :return:
        '''
        # this also handles the connection of the table name to the object, and ensures
        # that the table in each database has equivalent names.
        tableNames = None
        for psycoconnection in self.connections:

            cur = psycoconnection.cursor
            cur.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
            psycoconnection.tablename = cur.fetchall()[0][0]
            if not tableNames:
                tableNames = cur.fetchall()
            else:
                if tableNames != cur.fetchall():
                    print("No equivalent tables found.")


    def closeAll(self):
        '''
        Closes all the connections made via psycopg2 and deletes the docker processes.
        :return:
        '''
        for psycoconnection in self.connections:
            psycoconnection.close()
        for container in self.containers:
            container.kill()

    def testData(self):
        '''
        Test function that returns all information in a database.
        :return:
        '''
        for psycoconnection in self.connections:
            print(psycoconnection.query("select * from %s"))

    def testLimitedData(self, n):
        '''
        Test function that returns ordered and limited information from a database.
        :param n: size of loaded chunk
        :return:
        '''
        for psycoconnection in self.connections:
            print(psycoconnection.query("select * from %s order by id asc", n))

    def testFetchAllData(self, n):
        '''
        Test function that retrieves information from the database, then consumes it in fixed chunk sizes.
        :param n: size of loaded chunk.
        :return:
        '''
        response = True
        for psycoconnection in self.connections:
            out = psycoconnection.query("select * from %s order by id asc", n)

        while(response):
            response = False
            for psycoconnection in self.connections:
                out = psycoconnection.fetchNext()
                print(out)
                if (len(out) > 0):
                    response = True

    def compareData(self, n, dataComparator):
        '''
        We limit this application to 2 databases at a time, but make it easy to configure to many in series.


        :param n: size of data chunk from databases.
        :param dataComparator: a DataComparator() object that produces a running report of all errors encountered
                                between database information chunks.
        :return:
        '''
        response = True
        self.dataComparator = dataComparator

        adata = self.connections[0].query("select * from %s order by id asc", n)
        bData = self.connections[1].query("select * from %s order by id asc", n)
        while (response):
            response = False

            a = self.connections[0].fetchNext()
            b = self.connections[1].fetchNext()
            dataComparator.prepareDataChunks(a, b)

            if (len(a) > 0 or len(b) > 0):
                response = True
        dataComparator.finish()

    def generateReport(self):
        self.dataComparator.produceReports()

if __name__ == "__main__":
    mr = MigrationReport("init.txt")

    # The migration report generator takes in a datacomparator object.
    dc = DataComparator()
    N = 100 # datachunk size
    mr.compareData(N, dc)
    mr.generateReport()
    mr.closeAll()





