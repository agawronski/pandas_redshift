from setuptools import setup

setup(
    name = 'pandas_redshift',
    packages = ['pandas_redshift'],
    version = '0.1',
    description = 'Load data from redshift into a pandas DataFrame and vice versa.',
    author = 'Aidan Gawronski',
    author_email = 'aidangawronski@gmail.com',
    url = 'https://github.com/agawronski/pandas_redshift',
    download_url = 'https://github.com/agawronski/pandas_redshift/archive/1.0.tar.gz', # I'll explain this in a second
    install_requires = ['traceback',
                        'psycopg2',
                        'pandas',
                        'boto3',
                        'sys',
                        'os',
                        'io']
)
