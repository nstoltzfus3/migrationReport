from PsycoConnection import PsycoConnection

class PSQLServer:

    def __init__(self, user, dbname, password, port):
        self.user = user
        self.dbname = dbname
        self.password = password
        self.port = port

