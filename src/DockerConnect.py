import requests
import psycopg2
import json
import docker
import subprocess
import os
import time


class DockerConnect:

    def __init__(self, inifile, port=5432):
        '''
        The docker connect class takes in a list of filenames to construct a docker container communication platform.
        The filename is an initialization file that consists of the image names for each container.
        :param inifile: contains the names of the docker images that contain the postgres servers in question.
        '''

        client = docker.from_env()

        bashCommand = "docker run -d -p %d:5432 %s"

        for line in open(inifile):
            # starts containers while distributing monotonically increasing ports.
            # Opted to bash launch the containers then scrape the containers off the daemon
            # Made because original documentation was given for bash.

            # client.containers.run(line, ports={'%d/tcp' % port : 5432}, detach=True)
            process = subprocess.run((bashCommand % (port, line)).split())
            port += 1

        print(client.containers.list())
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




