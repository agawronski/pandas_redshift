from collections import namedtuple
import yaml
import os

from .exceptions import ConfigReadYamlError


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
    except ConfigReadYamlError:
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
