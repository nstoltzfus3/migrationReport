from PsycoConnection import PsycoConnection

class PSQLServer:
    '''
    Plain python class that holds information for a Postgres Server running on docker.
    '''
    def __init__(self, user, dbname, password, port):
        self.user = user
        self.dbname = dbname
        self.password = password
        self.port = port

