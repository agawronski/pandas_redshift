## pandas_redshift

This package is designed to make it easier to get data from redshift into a pandas DataFrame and vice versa.
The pandas_redshift package only supports python3.

## Installation

```python
pip install pandas-redshift
```

## Example
```python
import pandas_redshift as pr
```

Connect to redshift. If port is not supplied it will be set to amazon default 5439.

As of release 1.1.2 you can exclude the password if you are using a .pgpass file.

```python
pr.connect_to_redshift(dbname = <dbname>,
                        host = <host>,
                        port = <port>,
                        user = <user>,
                        password = <password>)
```

Query redshift and return a pandas DataFrame.

```python
data = pr.redshift_to_pandas('select * from gawronski.nba_shots_log')

data.ix[0:5,0:5]

GAME_ID1                   MATCHUP LOCATION  W  FINAL_MARGIN
0  21400899  MAR 4, 2015 - CHA @ BKN        A  W            24
1  21400899  MAR 4, 2015 - CHA @ BKN        A  W            24
2  21400899  MAR 4, 2015 - CHA @ BKN        A  W            24
3  21400899  MAR 4, 2015 - CHA @ BKN        A  W            24
4  21400899  MAR 4, 2015 - CHA @ BKN        A  W            24
5  21400899  MAR 4, 2015 - CHA @ BKN        A  W            24
```

Write a pandas DataFrame to redshift. Requires access to an S3 bucket and previously running pr.connect_to_redshift.

If the table currently exists **IT WILL BE DROPPED** and then the pandas DataFrame will be put in it's place.

If you set append = True the table will be appended to (if it exists).

```python
# Connect to S3
pr.connect_to_s3(aws_access_key_id = <aws_access_key_id>,
                aws_secret_access_key = <aws_secret_access_key>,
                bucket = <bucket>,
                subdirectory = <subdirectory>
                # As of release 1.1.1 you are able to specify an aws_session_token (if necessary):
                # aws_session_token = <aws_session_token>
                )

# Write the DataFrame to S3 and then to redshift
pr.pandas_to_redshift(data_frame = data,
                        redshift_table_name = 'gawronski.nba_shots_log')

```

Other options:

As of v1.1.2 you can specify the region (necessary if the S3 bucket is in a different location than Redshift).

```python
pr.pandas_to_redshift(data_frame,
                        redshift_table_name,
                        # Defaults:
                        column_data_types = None, # A list of column data types. As of 2.0.0 If not supplied the data types will be inferred from the DataFrame dtypes
                        index = False,
                        save_local = False, # If set to True a csv from the data frame will save in the current directory
                        delimiter = ',',
                        quotechar = '"',
                        dateformat = 'auto',
                        timeformat = 'auto',
                        region = '',
                        append = False)

```
Redshift data types: http://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html

Finally close the cursor, commit and close the database connection, and remove variables from the environment.

```python
pr.close_up_shop()
```


As this package is largely a layer over psycopg2 a convenience function has been added to execute and commit sql queries that don't have anything to do with your local machine (for example creating a new table).

```python
pr.exec_commit("""
create table combined_table as
select *
from table1
union
select *
from table2;
""")
```

If you encounter the error:
psycopg2.InternalError: current transaction is aborted, commands ignored until end of transaction block

you can access the pyscopg2 internals with the following:

```python
pr.core.connect.commit()
pr.core.connect.rollback()
```

Adjust logging [levels](https://github.com/agawronski/pandas_redshift/pull/50):

```python
pr.set_log_level('info')                     # print the copy statement, but hide keys
pr.set_log_level('info', mask_secrets=False) # display the keys
pr.set_log_level('error')                    # only log errors
```


