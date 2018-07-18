#!/usr/bin/env python3
# from pandas.core.dtypes.missing import isna
# import pandas._libs.lib as lib

from io import StringIO
import pandas as pd
import traceback
import psycopg2
import boto3
import csv
import sys
import os
import io

def connect_to_redshift(dbname, host, user, port = 5439, **kwargs):
    # connect to redshift
    global connect, cursor
    connect = psycopg2.connect(dbname = dbname,
                                        host = host,
                                        port = port,
                                        user = user,
                                        **kwargs)
    cursor = connect.cursor()


def connect_to_s3(aws_access_key_id, aws_secret_access_key, bucket, subdirectory = None, **kwargs):
    global s3, s3_bucket_var, s3_subdirectory_var, aws_1, aws_2, aws_token
    s3 = boto3.resource('s3',
                        aws_access_key_id = aws_access_key_id,
                        aws_secret_access_key = aws_secret_access_key,
                        **kwargs)
    s3_bucket_var = bucket
    if subdirectory is None:
        s3_subdirectory_var = ''
    else:
        s3_subdirectory_var = subdirectory + '/'
    aws_1 = aws_access_key_id
    aws_2 = aws_secret_access_key
    if kwargs.get('aws_session_token'):
        aws_token = kwargs.get('aws_session_token')
    else:
        aws_token = ''


# def redshift_to_pandas(sql_query, infer_dtypes = False):
#     # pass a sql query and return a pandas dataframe
#     cursor.execute(sql_query)
#     columns_list = [desc[0] for desc in cursor.description]
#     data = pd.DataFrame(cursor.fetchall(), columns = columns_list)
#     # try to coerce dtypes
#     def __coerce_col_dtype__(input_col):
#         """
#         Infer datatype of a pandas column, process only if the column dtype is object.
#         input:   col: a pandas Series representing a df column.
#         this function is adapted from: pandas.io.sql.SQLTable._get_notna_col_dtype
#             Infer datatype of the Series col.  In case the dtype of col is 'object'
#             and it contains NA values, this infers the datatype of the not-NA
#             values.  Needed for inserting typed data containing NULLs, GH8778.
#         """
#         # sample the first 10 000  rows to determine type.
#         col = input_col.dropna().unique()[:10000]
#         if col.dtype =="object":
#             # try numeric
#             try:
#                 col_new = pd.to_datetime(col)
#                 return col_new.dtype
#             except:
#                 try:
#                     col_new = pd.to_numeric(col)
#                     return col_new.dtype
#                 except:
#                     try:
#                         col_new = pd.to_timedelta(col)
#                         return col_new.dtype
#                     except:
#                         return "object"
#         else:
#             output = lib.infer_dtype(col)
#             # translate to numpy dtypes
#             to_numpy_dtypes = {
#                     "integer":"int64",
#                     "floating":"float64",
#                     "datetime64":"datetime64[ns]",
#                     "string":"object"
#              }
#             return to_numpy_dtypes.get(output, output)
#     if infer_dtypes:
#         dtypes = { x : __coerce_col_dtype__(data[x]) for x in data.columns }
#         data.astype(dtype = dtypes)
#     return data

def redshift_to_pandas(sql_query, infer_dtypes = False):
    # pass a sql query and return a pandas dataframe
    cursor.execute(sql_query)
    columns_list = [desc[0] for desc in cursor.description]
    if infer_dtypes:
        si = StringIO()
        cw = csv.writer(si)
        fetch_records = cursor.fetchall()
        for record in fetch_records:
            cw.writerow(record)
        si.seek(0)
        data = pd.read_csv(si, header = None, names = columns_list)
        return data
    else:
        data = pd.DataFrame(cursor.fetchall(), columns = columns_list)
        return data


def validate_column_names(data_frame):
    """Validate the column names to ensure no reserved words are used.

    Arguments:
        dataframe pd.data_frame -- data to validate
    """
    rrwords = open(os.path.join(os.path.dirname(__file__),
                                'redshift_reserve_words.txt'), 'r').readlines()
    rrwords = [r.strip().lower() for r in rrwords]

    data_frame.columns = [x.lower() for x in data_frame.columns]

    for col in data_frame.columns:
        try:
            assert col not in rrwords
        except:
            raise ValueError(
                'DataFrame column name {0} is a reserve word in redshift'
                .format(col))

    return data_frame


def df_to_s3(data_frame, csv_name, index, save_local, delimiter):
    """Write a dataframe to S3

    Arguments:
        dataframe pd.data_frame -- data to upload
        csv_name str -- name of the file to upload
        save_local bool -- save a local copy
        delimiter str -- delimiter for csv file
    """
    # create local backup
    if save_local == True:
        data_frame.to_csv(csv_name, index=index, sep=delimiter)
        print('saved file {0} in {1}'.format(csv_name, os.getcwd()))
    #
    csv_buffer = StringIO()
    data_frame.to_csv(csv_buffer, index=index, sep=delimiter)
    s3.Bucket(s3_bucket_var).put_object(
        Key=s3_subdirectory_var + csv_name, Body=csv_buffer.getvalue())
    print('saved file {0} in bucket {1}'.format(
        csv_name, s3_subdirectory_var + csv_name))


def create_redshift_table(data_frame,
                          redshift_table_name,
                          column_data_types=None,
                          index=False,
                          append=False):
    """Create an empty RedShift Table

    """
    if index == True:
        columns = list(data_frame.columns)
        if data_frame.index.name:
            columns.insert(0, data_frame.index.name)
        else:
            columns.insert(0, "index")
    else:
        columns = list(data_frame.columns)
    if column_data_types is None:
        column_data_types = ['varchar(256)'] * len(columns)
    columns_and_data_type = ', '.join(
        ['{0} {1}'.format(x, y) for x, y in zip(columns, column_data_types)])
    
    create_table_query = 'create table {0} ({1})'.format(
        redshift_table_name, columns_and_data_type)
    print(create_table_query)
    print('CREATING A TABLE IN REDSHIFT')
    cursor.execute('drop table if exists {0}'.format(redshift_table_name))
    cursor.execute(create_table_query)
    connect.commit()


def s3_to_redshift(redshift_table_name, delimiter=',', quotechar='"',
                   dateformat='auto', timeformat='auto', region=''):

    bucket_name = 's3://{0}/{1}.csv'.format(
                        s3_bucket_var, s3_subdirectory_var + redshift_table_name)
    s3_to_sql = """
    copy {0}
    from '{1}'
    delimiter '{2}'
    ignoreheader 1
    csv quote as '{3}'
    dateformat '{4}'
    timeformat '{5}'
    access_key_id '{6}'
    secret_access_key '{7}'
    """.format(redshift_table_name, bucket_name, delimiter, quotechar, dateformat,
               timeformat, aws_1, aws_2)
    if region:
        s3_to_sql = s3_to_sql + "region '{0}'".format(region)
    if aws_token != '':
        s3_to_sql = s3_to_sql + "\n\tsession_token '{0}'".format(aws_token)
    s3_to_sql = s3_to_sql + ';'
    print(s3_to_sql)
    # send the file
    print('FILLING THE TABLE IN REDSHIFT')
    try:
        cursor.execute(s3_to_sql)
        connect.commit()
    except Exception as e:
        print(e)
        traceback.print_exc(file=sys.stdout)
        connect.rollback()
        raise


def pandas_to_redshift(data_frame,
                       redshift_table_name,
                       column_data_types=None,
                       index=False,
                       save_local=False,
                       delimiter=',',
                       quotechar='"',
                       dateformat='auto',
                       timeformat='auto',
                       region='',
                       append=False):

    # Validate column names.
    data_frame = validate_column_names(data_frame)
    # Send data to S3
    csv_name = redshift_table_name + '.csv'
    df_to_s3(data_frame, csv_name, index, save_local, delimiter)

    # CREATE AN EMPTY TABLE IN REDSHIFT
    if append is False:
        create_redshift_table(data_frame, redshift_table_name,
                              column_data_types, index, append)
    # CREATE THE COPY STATEMENT TO SEND FROM S3 TO THE TABLE IN REDSHIFT
    s3_to_redshift(redshift_table_name, delimiter, quotechar,
                   dateformat, timeformat, region)



def exec_commit(sql_query):
    cursor.execute(sql_query)
    connect.commit()


def close_up_shop():
    global connect, cursor, s3, s3_bucket_var, s3_subdirectory_var, aws_1, aws_2, aws_token
    cursor.close()
    connect.commit()
    connect.close()
    try:
        del connect, cursor
    except:
        pass
    try:
        del s3, s3_bucket_var, s3_subdirectory_var, aws_1, aws_2, aws_token
    except:
        pass

#-------------------------------------------------------------------------------
