import pandas as pd
# import ibm_db
# import ibm_db_dbi as dbi
# import ibm_db_sa
from datetime import datetime
from datetime import timedelta
import math
import re
#import sqlalchemy
import jaydebeapi
#from sqlalchemy import types
from sqlalchemy import create_engine
from IPython.core.ultratb import AutoFormattedTB
# from boxsdk import JWTAuth
# from boxsdk import Client
import os
import sys
import glob



class MasterConnector:
    '''
    adding a comment 
    An object to connecto to any database using jaydebeapi
    You need to provide a dictionary like this for the dbDictionary variable
    {'uid': 'gcognos',
    'pwd': 'XXXXXXX@T1me#4%Ever',
    'host': '6bc4d3XXXXXXXXX7b092.bpe60pbd01oinge4psd0.databases.appdomain.cloud',
    'port': '31912', 
    'db': 'BLUDB',
    'd_source': 'db2',
    'ssl': 'True',
    'truststore': 'False'}
    '''

    def __init__(self, dbDictionary, jarFilesPath, certPath=None, jksPath=None):
        self.uid = dbDictionary['uid']
        self.pwd = dbDictionary['pwd']
        self.db = dbDictionary['db']
        self.host = dbDictionary['host']
        self.port = dbDictionary['port']
        self.d_source = dbDictionary['d_source']
        self.ssl = dbDictionary['ssl']
        self.using_cert_file = dbDictionary['using_cert_file']
        self.using_jks_file = dbDictionary['using_jks_file']
        self.jks_password = dbDictionary['jks_password']
        self.cert_path = certPath
        self.jksPath = jksPath
        self.jarFilesPath = jarFilesPath

    def _connectDb(self):
        '''
        for any other current parameters 
        '''
        # define string connection
        if self.d_source == 'db2':
            jdbc = 'jdbc:db2:'
            driver = 'com.ibm.db2.jcc.DB2Driver'
        elif self.d_source == 'netezza':
            jdbc = 'jdbc:netezza:'
            driver = "org.netezza.Driver"

        if self.ssl == 'True':
            string = jdbc + \
                f'//{self.host}:{self.port}/{self.db}:sslConnection=true;useJDBC4ColumnNameAndLabelSemantics=false;'
        else:
            string = jdbc + \
                f'//{self.host}:{self.port}/{self.db}:useJDBC4ColumnNameAndLabelSemantics=false;'

        if self.using_cert_file == 'True':
            string = string + f'sslCertLocation={self.cert_path};'
        
        if self.using_jks_file == 'True':
            string = string + f'sslTrustStoreLocation={self.jksPath};sslTrustStorePassword={self.jks_password};'

        # create connection to database
        # jar files
        jars = glob.glob(self.jarFilesPath+'*.jar', recursive=False)
        # jars = glob.glob('jars.zip', recursive=False)
        #jars.append('sqlj4.zip')
        jars = [os.path.abspath(x) for x in jars]
        # print(jars)
        try:
            self.conn = jaydebeapi.connect(
                driver, string, [self.uid, self.pwd], jars=jars)
            self.curs = self.conn.cursor()
        except Exception as e:
            print('Error in connection: ', e)
        else:
            print(f'Connection to {self.db} established: {self.uid} ')

    def getAllGcognosOptions(self):
        'Only use with gcognos daat database connectiong'
        self._connectDb()
        sql = 'select * from GCOGNOS.PYTHON_PW_DB'
        data = self.sql_to_df(sql=sql)
        self._close_conn()
        return data

    def sql_to_df(self, sql):
        '''Execute SQL and export to Dataframe'''
        self._connectDb()
        START = datetime.now()
        df = pd.read_sql(sql, self.conn)
        END = datetime.now()
        print('SQL Run Time: ', END-START)
        self._close_conn()
        return df

    def exec_statement(self, sql):
        '''Execute an SQL Statement. 
        ej1: DELETE FROM SCHEMA.TABLE
        ej2: GRANT ALL PRIVILEGES ON SCHEMA.TABLE WHERE TO user '''
        self._connectDb()
        self.curs.execute(sql)
        self.conn.commit()
        self._close_conn()

    def save_df(self, df, table, schema=None, how='append', chunksize=1000, show=0, nonAscii=False):
        '''INSERT a df: DataFrame into the table: TABLE. 
        how='replace' or 'append'. If replace, first we execute a DROP. 
        If table does not exist, we use string_to_create_table and date_converter functions to create the table and adapt dates to DATE DB2 format does not fail.
        schema: default is set to the user use to make the connection (schema = self.uid.upper())
        chunksize: to set the size of chunks it will upload at the same time
        show: if you want to print the n 'INSERT INTO schema.table...' string characters to see values or execute in QMF.
        nonAscii: set to True if want to remove assean characters as Chinesse, Japanese or Korean'''
        self._connectDb()
        table = table.upper()
        if schema == None:
            schema = self.uid.upper()

        row = 0
        field_names = '("' + '","'.join(df.columns)+'")'
        if how == 'replace':
            try:
                delete_table = f'DELETE FROM {schema}.{table}'
                self.curs.execute(delete_table)
                self.conn.commit()
            except:
                print('Creating table')
                create = string_to_create_table(df, f'{schema}.{table}')
                self.curs.execute(create)
                self.conn.commit()
                df = date_converter(df)
                pass

        while row < len(df):
            beginrow = row
            if row + chunksize > len(df):
                endrow = len(df)
            else:
                endrow = row + chunksize

            tuples = [tuple(x) for x in df.values[beginrow:endrow]]

            temp_list1 = []

            for t in tuples:

                temp_list2 = []

                for v in t:
                    if str(type(v)) == "<class 'jpype._jclass.java.lang.Long'>":
                        temp_list2.append(v.value)
                    elif isinstance(v, str) and ("'" in v or '"' in v):
                        temp_list2.append(v.replace("'", "").replace('"', "”"))
                    elif nonAscii and isinstance(v, str) and re.search("[\uac00-\ud7a3\u3040-\u30ff\u4e00-\u9FFF]", v):
                        v = re.sub(
                            "[\uac00-\ud7a3\u3040-\u30ff\u4e00-\u9FFF]", " ", v)
                        v = re.sub(' +', ' ', v)
                        v = v + '*'
                        temp_list2.append(v)
                    elif ('datetime' in str(type(v))) or ('Timestamp' in str(type(v))):
                        temp_list2.append(str(v))
                    elif v == '' or v is None:
                        temp_list2.append('')
                    elif v != v:
                        temp_list2.append(None)
                    else:
                        temp_list2.append(v)

                temp_list1.append(tuple(temp_list2))

            tuples = temp_list1

            values_params = '('+",".join('?' for i in df.columns)+')'

            sql = f'''INSERT INTO {schema}.{table} {field_names} VALUES {values_params}'''

            if show > 0:
                print(sql[:show])
                print(tuples)
            self.curs.executemany(sql, tuples)
            self.conn.commit()
            print(str(endrow), 'out of', str(len(df)), ' loaded to table.')
            row = endrow
        # closing connection
        self._close_conn()

    def _close_conn(self):
        '''Close connection to DB'''
        self.conn.close()
        self.curs.close()
        print('Connection closed')


class HiddenPrints:
    '''Hide print when not necessary '''

    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout


def date_converter(df):
    '''Raplace nans to Nats in datelike columns. Used combined with string_to_create_table in save_df when how=replace and table does not exists '''
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    for col in df.columns:
        len_values = df[col].astype(str).apply(len)
        if df[col].dtypes == 'object':
            if (len_values.max() == 10 or len_values.max() == 19 or len_values.max() == 26) and (df[col].astype(str).str.count(r'(\d+(/|-){1,4}\d+(/|-){1,2}\d{1,4})').sum() / len(df) > 0.05):
                df[col].fillna(value=pd.NaT, inplace=True)
                print(col, ' -> NaNs convert to NaT')
    return df


def string_to_create_table(df, table):
    '''Return a 'CREATE TABLE...' string based on df columns python formats 
    df: dataFame object
    table: string with table name to create: 'SCHEMA.TABLE'   '''
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    empty = df.columns[df.isna().all()].to_list()
    df[empty] = df[empty].fillna(value='', axis=1)
    string = 'CREATE TABLE '
    for col in df.columns:
        if '_ID' in col:
            df[col] = df[col].astype('str')
    string = string + table + ' ( '
    for col in df.columns:
        # print(col, str(df[col].dtypes))
        len_values = df[col].astype(str).apply(len)
        if 'IDENTIFIER_COUNTER' in col:
            string = string + f'"{col}"' + ' DECIMAL(15,9)' + ', '
        elif 'float' in str(df[col].dtypes):
            int_len = int(len_values.max())
            string = string + f'"{col}"' + \
                ' DECIMAL({},2)'.format(int_len+2) + ', '
            # print('DECIMAL')
        elif 'int' in str(df[col].dtypes):
            string = string + f'"{col}"' + ' INTEGER' + ', '
            # print('INTEGER')
        elif 'ns' in str(df[col].dtypes):
            string = string + f'"{col}"' + ' TIMESTAMP' + ', '
            # print('ORIGINAL DATE')
        elif df[col].dtypes == 'object':
            leng = int(len_values.max() + 3 * math.sqrt(len_values.var()))
            if (len_values.max() == 10) and (df[col].astype(str).str.count(r'(\d+(/|-){1,4}\d+(/|-){1,2}\d{1,4})').sum() / len(df) > 0.05):
                string = string + f'"{col}"' + ' DATE' + ', '
                # print('DATE')
            elif (len_values.max() == 19 or len_values.max() == 26) and (df[col].astype(str).str.count(r'(\d+(/|-){1,4}\d+(/|-){1,2}\d{1,4})').sum() / len(df) > 0.05):
                string = string + f'"{col}"' + ' TIMESTAMP' + ', '
                # print('TIMESTAMP')
            else:
                string = string + f'"{col}"' + \
                    ' VARCHAR({})'.format(leng + 15) + ', '
                # print('VARCHAR')
        else:
            print('EXCEPTION IN  ', df[col].dtypes)
    string = string[:-2] + ');'
    print(string)
    return string
