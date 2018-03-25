from collections import namedtuple
import psycopg2
import yaml
import os


# Easily store creds from `config.yaml`
DB = namedtuple('DB', [
    'dbname',
    'host',
    'port',
    'user',
    'password'
])


def _read_config():
    """Reads DB Creds From Config.yaml

    :returns: DB(**kwargs), namedtuple

    Example
    -------

    >>> config = _read_config()
    >>> config.dbname
    >>> 'mytestdb'
    >>> config.port
    >>> 5439
    """
    current_dir = os.path.abspath(os.curdir)
    file_path = os.path.join(current_dir, 'config.yaml')

    try:
        with open(file_path, mode='r', encoding='UTF-8') as file:
            config = yaml.load(file)
            return DB(**config)

    except IOError as e:
        print("""
        Unable to find `config.yaml` in current directory.

        Default format is:
        ==================
        dbname: 'dbname'
        host: host
        port: 5439
        user: 'user'
        password: 'password'
        """)
        raise


def _pg_connect():
    """Automatically Connect to Postgres

    Uses DB creds from `_read_config()` namedtuple
    to construct the DSN. From there, two variables are
    returned: connect and cursor. Connect is a connection
    class from Psycopg2 that handles connecting to a Postgres
    instance. Cursor is an object that uses the connection
    in order to send queries to Postgres.

    :returns: connect, connection object
              cursor,  cursor object

    Example
    -------

    >>> connect, cursor = _pg_connect()
    """
    config = _read_config()
    connect = _psyco_connect(config.dbname, config.host,
                            config.user, config.port,
                            password=config.password)
    cursor = connect.cursor()
    return connect, cursor


def _psyco_connect(dbname, host, user, port, **kwargs):
    """Connect To Postgres Database Instance

    :dbname: database name, str
    :host: database host address, str
    :port: connection port number, int
    :user: username used to authenticate, str
    :password: password used to authenticate, str

    :returns: postgres connection, object
    """
    return psycopg2.connect(dbname=dbname, host=host, port=port,
                            user=user, **kwargs)
