#!/usr/bin/env python3
from io import StringIO
import pandas as pd
import traceback
import psycopg2
import boto3
import sys
import os
import re
import uuid
import logging

S3_ACCEPTED_KWARGS = [
    'ACL', 'Body', 'CacheControl ',  'ContentDisposition', 'ContentEncoding', 'ContentLanguage',
    'ContentLength', 'ContentMD5', 'ContentType', 'Expires', 'GrantFullControl', 'GrantRead',
    'GrantReadACP', 'GrantWriteACP', 'Metadata', 'ServerSideEncryption', 'StorageClass',
    'WebsiteRedirectLocation', 'SSECustomerAlgorithm', 'SSECustomerKey', 'SSECustomerKeyMD5',
    'SSEKMSKeyId', 'RequestPayer', 'Tagging'
]  # Available parameters for service: https://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Client.put_object



logging_config = {
    'logger_level': logging.INFO,
    'mask_secrets': True
}
log_format = 'Pandas Redshift | %(asctime)s | %(name)s | %(levelname)s | %(message)s'
logging.basicConfig(level=logging_config['logger_level'], format=log_format)
logger = logging.getLogger(__name__)


def set_log_level(level, mask_secrets=True):
    log_level_map = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warn': logging.WARN,
        'error': logging.ERROR
    }
    logging_config['logger_level'] = log_level_map[level]
    logger = logging.getLogger(__name__)
    logger.setLevel(logging_config['logger_level'])
    logging_config['mask_secrets'] = mask_secrets


def mask_aws_credentials(s):
    if logging_config['mask_secrets']:
        import re
        s = re.sub('(?<=access_key_id \')(.*)(?=\')', '*'*8, s)
        s = re.sub('(?<=secret_access_key \')(.*)(?=\')', '*'*8, s)
    return s


def connect_to_redshift(dbname, host, user, port=5439, **kwargs):
    global connect, cursor
    connect = psycopg2.connect(dbname=dbname,
                               host=host,
                               port=port,
                               user=user,
                               **kwargs)

    cursor = connect.cursor()


def connect_to_s3(aws_access_key_id, aws_secret_access_key, bucket, subdirectory=None, aws_iam_role=None, **kwargs):
    global s3, s3_bucket_var, s3_subdirectory_var, aws_1, aws_2, aws_token, aws_role
    s3 = boto3.resource('s3',
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        **kwargs)
    s3_bucket_var = bucket
    if subdirectory is None:
        s3_subdirectory_var = ''
    else:
        s3_subdirectory_var = subdirectory + '/'
    aws_1 = aws_access_key_id
    aws_2 = aws_secret_access_key
    aws_role = aws_iam_role
    if kwargs.get('aws_session_token'):
        aws_token = kwargs.get('aws_session_token')
    else:
        aws_token = ''


def redshift_to_pandas(sql_query, query_params=None):
    # pass a sql query and return a pandas dataframe
    cursor.execute(sql_query, query_params)
    columns_list = [desc[0] for desc in cursor.description]
    data = pd.DataFrame(cursor.fetchall(), columns=columns_list)
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
        except AssertionError:
            raise ValueError(
                'DataFrame column name {0} is a reserve word in redshift'
                .format(col))

    # check for spaces in the column names
    there_are_spaces = sum(
        [re.search('\s', x) is not None for x in data_frame.columns]) > 0
    # delimit them if there are
    if there_are_spaces:
        col_names_dict = {x: '"{0}"'.format(x) for x in data_frame.columns}
        data_frame.rename(columns=col_names_dict, inplace=True)
    return data_frame


def df_to_s3(data_frame, csv_name, index, save_local, delimiter, verbose=True, **kwargs):
    """Write a dataframe to S3

    Arguments:
        dataframe pd.data_frame -- data to upload
        csv_name str -- name of the file to upload
        save_local bool -- save a local copy
        delimiter str -- delimiter for csv file
    """
    extra_kwargs = {k: v for k, v in kwargs.items(
    ) if k in S3_ACCEPTED_KWARGS and v is not None}
    # create local backup
    if save_local:
        data_frame.to_csv(csv_name, index=index, sep=delimiter)
        if verbose:
            logger.info('saved file {0} in {1}'.format(csv_name, os.getcwd()))
    #
    csv_buffer = StringIO()
    data_frame.to_csv(csv_buffer, index=index, sep=delimiter)
    s3.Bucket(s3_bucket_var).put_object(
        Key=s3_subdirectory_var + csv_name, Body=csv_buffer.getvalue(),
        **extra_kwargs)
    if verbose:
        logger.info('saved file {0} in bucket {1}'.format(
            csv_name, s3_subdirectory_var + csv_name))


def pd_dtype_to_redshift_dtype(dtype):
    if dtype.startswith('int64'):
        return 'BIGINT'
    elif dtype.startswith('int'):
        return 'INTEGER'
    elif dtype.startswith('float'):
        return 'REAL'
    elif dtype.startswith('datetime'):
        return 'TIMESTAMP'
    elif dtype == 'bool':
        return 'BOOLEAN'
    else:
        return 'VARCHAR(256)'


def get_column_data_types(data_frame, index=False):
    column_data_types = [pd_dtype_to_redshift_dtype(dtype.name)
                         for dtype in data_frame.dtypes.values]
    if index:
        column_data_types.insert(
            0, pd_dtype_to_redshift_dtype(data_frame.index.dtype.name))
    return column_data_types


def create_redshift_table(data_frame,
                          redshift_table_name,
                          column_data_types=None,
                          index=False,
                          append=False,
                          diststyle='even',
                          distkey='',
                          sort_interleaved=False,
                          sortkey='',
                          verbose=True):
    """Create an empty RedShift Table

    """
    if index:
        columns = list(data_frame.columns)
        if data_frame.index.name:
            columns.insert(0, data_frame.index.name)
        else:
            columns.insert(0, "index")
    else:
        columns = list(data_frame.columns)
    if column_data_types is None:
        column_data_types = get_column_data_types(data_frame, index)
    columns_and_data_type = ', '.join(
        ['{0} {1}'.format(x, y) for x, y in zip(columns, column_data_types)])

    create_table_query = 'create table {0} ({1})'.format(
        redshift_table_name, columns_and_data_type)
    if not distkey:
        # Without a distkey, we can set a diststyle
        if diststyle not in ['even', 'all']:
            raise ValueError("diststyle must be either 'even' or 'all'")
        else:
            create_table_query += ' diststyle {0}'.format(diststyle)
    else:
        # otherwise, override diststyle with distkey
        create_table_query += ' distkey({0})'.format(distkey)
    if len(sortkey) > 0:
        if sort_interleaved:
            create_table_query += ' interleaved'
        create_table_query += ' sortkey({0})'.format(sortkey)
    if verbose:
        logger.info(create_table_query)
        logger.info('CREATING A TABLE IN REDSHIFT')
    cursor.execute('drop table if exists {0}'.format(redshift_table_name))
    cursor.execute(create_table_query)
    connect.commit()


def s3_to_redshift(redshift_table_name, csv_name, delimiter=',', quotechar='"',
                   dateformat='auto', timeformat='auto', region='', parameters='', verbose=True):

    bucket_name = 's3://{0}/{1}'.format(
        s3_bucket_var, s3_subdirectory_var + csv_name)

    if aws_1 and aws_2:
        authorization = """
        access_key_id '{0}'
        secret_access_key '{1}'
        """.format(aws_1, aws_2)
    elif aws_role:
        authorization = """
        iam_role '{0}'
        """.format(aws_role)
    else:
        authorization = ""

    s3_to_sql = """
    copy {0}
    from '{1}'
    delimiter '{2}'
    ignoreheader 1
    csv quote as '{3}'
    dateformat '{4}'
    timeformat '{5}'
    {6}
    {7}
    """.format(redshift_table_name, bucket_name, delimiter, quotechar, dateformat,
               timeformat, authorization, parameters)
    if region:
        s3_to_sql = s3_to_sql + "region '{0}'".format(region)
    if aws_token != '':
        s3_to_sql = s3_to_sql + "\n\tsession_token '{0}'".format(aws_token)
    s3_to_sql = s3_to_sql + ';'
    if verbose:
        logger.info(mask_aws_credentials(s3_to_sql))
        # send the file
        logger.info('FILLING THE TABLE IN REDSHIFT')
    try:
        cursor.execute(s3_to_sql)
        connect.commit()
    except Exception as e:
        logger.error(e)
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
                       append=False,
                       diststyle='even',
                       distkey='',
                       sort_interleaved=False,
                       sortkey='',
                       parameters='',
                       verbose=True,
                       **kwargs):

    # Validate column names.
    data_frame = validate_column_names(data_frame)
    # Send data to S3
    csv_name = '{}-{}.csv'.format(redshift_table_name, uuid.uuid4())
    s3_kwargs = {k: v for k, v in kwargs.items()
        if k in S3_ACCEPTED_KWARGS and v is not None}
    df_to_s3(data_frame, csv_name, index, save_local, delimiter, verbose=verbose, **s3_kwargs)

    # CREATE AN EMPTY TABLE IN REDSHIFT
    if not append:
        create_redshift_table(data_frame, redshift_table_name,
                              column_data_types, index, append,
                              diststyle, distkey, sort_interleaved, sortkey, verbose=verbose)

    # CREATE THE COPY STATEMENT TO SEND FROM S3 TO THE TABLE IN REDSHIFT
    s3_to_redshift(redshift_table_name, csv_name, delimiter, quotechar,
                   dateformat, timeformat, region, parameters, verbose=verbose)


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

# -------------------------------------------------------------------------------
