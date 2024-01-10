class PGConnectionInfo:
    """Keeps Postgres connection info."""

    def __init__(self, host, port, user, password, dbname):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self.set_dbname(dbname)

    def host(self):
        return self._host

    def port(self):
        return self._port

    def user(self):
        return self._user

    def password(self):
        return self._password

    def set_dbname(self, dbname):
        if dbname.strip().lower() == "postgres":
            raise Exception("database name must be different from 'postgres'")
        self._dbname = dbname

    def dbname(self):
        return self._dbname
