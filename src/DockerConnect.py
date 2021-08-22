import requests
import psycopg2
import json
import docker
import subprocess
import os
import time
import requests

from PSQLServer import PSQLServer


class DockerConnect:

    def __init__(self, inifile, port=5432):
        '''
        The docker connect class takes in a list of filenames to construct a docker container communication platform.
        The filename is an initialization file that consists of the image names for each container.
        :param inifile: contains the names of the docker images that contain the postgres servers in question.
        '''

        client = docker.from_env()

        bashCommand = "docker run -d -p %d:5432 %s"

        self.dbs = {}
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

        self.containers = client.containers.list()



    def closeAll(self):
        for container in self.containers:
            container.kill()



class DockerConnectTest:
    def __init__(self):
        '''
        Needs to generate and delete the files afterward. Proper Excepts
        '''
        pass

if __name__ == "__main__":
    dc = DockerConnect("init.txt")

    psqlConnections = []
    for container in dc.containers[::-1]:
        try:
            PSQLdb = dc.dbs[container.image.attrs['RepoTags'][0]]
            print("Database: %s running on port %d" % (PSQLdb.dbname, PSQLdb.port))

            conn = psycopg2.connect(
                host="localhost",
                database=PSQLdb.dbname,
                user=PSQLdb.user,
                password=PSQLdb.password,
                port=PSQLdb.port
                )
            psqlConnections.append(conn)
            print("Connection established...")
        except Exception as e:
            print(e)

    for connection in psqlConnections:
        cur = connection.cursor()
        cur.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
        print(cur.fetchall())

    dc.closeAll()




