

class dbConfig:
    """
      db config
    """

    def __init__(self, user, password, host, port,database):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database

    def get_table_name(self):
        return "job_status"
