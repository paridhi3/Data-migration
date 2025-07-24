import pyodbc as pyo
import warnings

warnings.filterwarnings("ignore")
import pandas as pd
from tabulate import tabulate
from sqlalchemy import create_engine
# from sqlalchemy.dialects import registry
from snowflake.connector.pandas_tools import pd_writer
# registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')
import snowflake
import psycopg2
from datetime import datetime
import re
import string
# import helper
import pandas as pd
import numpy as np
import datetime
import urllib.parse
import snowflake.connector
import asyncio
from flask import Flask
import threading

app = Flask(__name__)
timestamps = []
length = 0
current_progress = 0
t_End = 0
total_entries = 0

# Global list to store start_time_str
global_start_time_list = []

call_count = 0


def write_in_db(sql, cur1):
    cur1.execute(sql)


def datatype_comparison(table_name, schema_name, in_database, server, user, password, account, warehouse, role,
                              cnn, cur, cur1):
    sql = """SELECT UPPER(COLUMN_NAME) as COLUMN_NAME, DATA_TYPE,ISNULL(ISNULL(CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION),DATETIME_PRECISION) LENGTH,NUMERIC_SCALE FROM {2}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{0}' and TABLE_SCHEMA='{1}';""".format(
        table_name, schema_name, in_database)
    df3 = pd.read_sql(sql, cnn)
    df3 = pd.DataFrame(df3)
    print(sql)
    print(df3)
    exit = 0
    datatype = ['BIGINT', 'BIT', 'DECIMAL', 'INT', 'MONEY', 'NUMERIC', 'SMALLINT', 'SMALLMONEY', 'TINYINT', 'FLOAT',
                'REAL', 'DATE', 'DATETIME2', 'DATETIME', 'DATETIMEOFFSET', 'SMALLDATETIME', 'TIME', 'CHAR', 'TEXT',
                'VARCHAR', 'NCHAR', 'NTEXT', 'NVARCHAR', 'UNIQUEIDENTIFIER', 'XML', 'HIERARCHYID', 'BINARY',
                'VARBINARY','GEOGRAPHY']
    datatype = [x.lower() for x in datatype]
    dtmap = ['NUMBER', 'BOOLEAN', 'NUMBER', 'NUMBER', 'FLOAT', 'NUMBER', 'NUMBER', 'FLOAT', 'NUMBER', 'FLOAT',
             'NUMERIC', 'DATE', 'TIMESTAMP_NTZ', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'DATETIME', 'TIME', 'VARCHAR(1)',
             'VARCHAR', 'VARCHAR', 'VARCHAR', 'VARCHAR', 'VARCHAR', 'VARCHAR(16777216)', 'VARIANT', 'VARCHAR',
             'BINARY', 'VARCHAR','VARCHAR']
    df3['COMPATIBLE_COLOUMN'] = 0
    for j in range(0, len(df3['DATA_TYPE'])):
        # print(df3['DATA_TYPE'][j])
        # print(datatype)
        for i in range(0, len(datatype)):
            # print(datatype[i])
            if df3['DATA_TYPE'][j] == datatype[i]:
                df3['COMPATIBLE_COLOUMN'][j] = dtmap[i]
            else:
                if df3['COMPATIBLE_COLOUMN'][j] == 0:
                    df3['COMPATIBLE_COLOUMN'][j] = df3['DATA_TYPE'][j]
                else:
                    pass
    # print(df3['Data_types'])
    # print(df3['COMPATIBLE_COLOUMN'])
    df3['Data_types'] = 0
    #     print(df3['COMPATIBLE_COLOUMN'][0])
    for i in range(0, len(df3['COMPATIBLE_COLOUMN'])):
        if (df3['COMPATIBLE_COLOUMN'][i] == 'NUMBER'):
            df3["LENGTH"][i] = str(df3["LENGTH"][i])
            df3["NUMERIC_SCALE"][i] = str(df3["NUMERIC_SCALE"][i])
            df3["LENGTH"][i] = df3["LENGTH"][i].replace('.0', '')
            df3["NUMERIC_SCALE"][i] = df3["NUMERIC_SCALE"][i].replace('.0', '')
            df3['Data_types'][i] = df3['COMPATIBLE_COLOUMN'][i] + '(' + df3["LENGTH"][i] + ',' + \
                                   df3["NUMERIC_SCALE"][i] + ')'
        elif (df3['COMPATIBLE_COLOUMN'][i] == 'FLOAT'):
            df3['Data_types'][i] = df3['COMPATIBLE_COLOUMN'][i]
        elif (df3['COMPATIBLE_COLOUMN'][i] == 'VARCHAR'):
            df3["LENGTH"][i] = str(df3["LENGTH"][i])
            df3["LENGTH"][i] = df3["LENGTH"][i].replace('.0', '')
            df3['Data_types'][i] = df3['COMPATIBLE_COLOUMN'][i] + '(' + df3["LENGTH"][i] + ')'
        elif (df3['COMPATIBLE_COLOUMN'][i] == 'TIMESTAMP_NTZ'):
            df3["LENGTH"][i] = str(df3["LENGTH"][i])
            df3["LENGTH"][i] = df3["LENGTH"][i].replace('.0', '')
            df3['Data_types'][i] = df3['COMPATIBLE_COLOUMN'][i] + '(' + df3["LENGTH"][i] + ')'
        elif (df3['COMPATIBLE_COLOUMN'][i] == 'DATE'):
            df3['Data_types'][i] = df3['COMPATIBLE_COLOUMN'][i]
        elif (df3['COMPATIBLE_COLOUMN'][i] == 'BOOLEAN'):
            df3['Data_types'][i] = 'NUMBER(1,0)'
        elif (df3['COMPATIBLE_COLOUMN'][i] == 'VARIANT'):
            df3['Data_types'][i] = 'VARIANT'
        elif (df3['COMPATIBLE_COLOUMN'][i] == 'VARCHAR(16777216)'):
            df3['Data_types'][i] = 'VARCHAR(16777216)'
        elif (df3['COMPATIBLE_COLOUMN'][i] == 'BINARY'):
            df3['Data_types'][i] = 'BINARY(8388608)'
        elif (df3['COMPATIBLE_COLOUMN'][i] == 'VARCHAR(-1)'):
            df3['Data_types'][i] = 'VARCHAR(16777216)'
        elif (df3['COMPATIBLE_COLOUMN'][i] == 'geography'):
            df3['Data_types'][i] = 'GEOGRAPHY'
        elif (df3['COMPATIBLE_COLOUMN'][i] == 'NUMBER(19,4)'):
            df3['Data_types'][i] = 'NUMBER(19,4)'
        elif (df3['COMPATIBLE_COLOUMN'][i] == 'NUMBER(10,4)'):
            df3['Data_types'][i] = 'NUMBER(10,4)'
        elif (df3['COMPATIBLE_COLOUMN'][i] == 'NUMBER(9,4)'):
            df3['Data_types'][i] = 'NUMBER(9,4)'
        else:
            df3["LENGTH"][i] = str(df3["LENGTH"][i])
            df3['Data_types'][i] = df3['COMPATIBLE_COLOUMN'][i] + '(' + df3["LENGTH"][i] + ')'
        if (df3['Data_types'][i] == 'VARCHAR(-1)'):
            df3['Data_types'][i] = 'VARCHAR(16777216)'
    # if df3['Data_types'][j]==0:
    #        df3['Data_types']=df3['COMPATIBLE_COLOUMN'].astype(str)+'(' + df3["LENGTH"].astype(str)+')'
    #     print("SQL Server Datatypes details")
    #     print('\n')
    #     print(df3)
    #     print('\n')
    cur1.execute("use warehouse "+warehouse)
    try:
        sql = """DESC TABLE {2}.{1}.{0};""".format(table_name, schema_name, in_database)
        cur1.execute(sql)
        sql = """SET DESC_QUERY_ID = LAST_QUERY_ID();"""
        cur1.execute(sql)
        sql = """SELECT '{2}.{1}.{0}',"name","type" from table(result_scan($desc_query_id));""".format(
            table_name, schema_name, in_database)
        d = cur1.execute(sql)
        df4 = pd.DataFrame(d)
        df4.columns = ['Table_name', 'column_name', 'Data_type']
    except Exception as e:

        # Get the current timestamp as the start time
        start_time = datetime.datetime.now()

        # Convert the start time to a string in the desired format (if needed)
        start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')

        # Append the string to the global list
        global_start_time_list.append(start_time_str)

        sql = """insert into  GMigrate_Report_DB.QA.datatypevalidation_error_report(Source_DBMS,Target_DBMS,TABLE_NAME,RESULT_STATUS,CREATED_ON,CREATED_BY)
            values('SQL_SERVER','SNOWFLAKE','{2}.{1}.{0}','Not-valid',TO_TIMESTAMP('{3}', 'YYYY-MM-DD HH24:MI:SS'),'{4}')""".format(table_name, schema_name, in_database, start_time_str, user)
        write_in_db(sql, cur1)
        print("Log For date type validation updated ", table_name)
        exit = 1
    #     print("Snowflake Datatypes Details")
    #     print('\n')
    #     print(df4)
    #     print('\n')
    # comparing the datatypes
    try:
        print("Comparing the datatypes")
        df5 = pd.DataFrame()
        df5['Table_name'] = df4['Table_name']
        df5['column_name'] = df4['column_name'].str.upper()
        df5['SQL_Datatype'] = df3['DATA_TYPE']
        df5['Snowflake_Datatype'] = df4['Data_type']
        # df5['SQL_Datatype_compartible']=df3['COMPATIBLE_COLOUMN']
        df5['Final_Data_types'] = df3['Data_types']
        df5['COLUMN_COMPAR'] = np.where(df3['COLUMN_NAME'].str.upper() == df4['column_name'].str.upper(), 'True',
                                        'False')
        df5['COLUMN_AND_LEN_COMPAR'] = np.where(df3['Data_types'] == df4['Data_type'], 'True', 'False')
        # df5['result_LENGTH']=np.where(df3['LENGTH']==df4['max_length'],'True','False')
        print('\n')
        # print(df5['COLUMN_COMPAR'])
        print(df5)
        print('\n')
    except:
        if (exit != 1):

            # Get the current timestamp as the start time
            start_time = datetime.datetime.now()

            # Convert the start time to a string in the desired format (if needed)
            start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')

            # Append the string to the global list
            global_start_time_list.append(start_time_str)

            sql = """insert into  GMigrate_Report_DB.QA.datatypevalidation_error_report(Source_DBMS,Target_DBMS,TABLE_NAME,RESULT_STATUS,CREATED_ON,CREATED_BY)
                values('SQL_SERVER','SNOWFLAKE','{2}.{1}.{0}','Error while comparing Datatypes',TO_TIMESTAMP('{3}', 'YYYY-MM-DD HH24:MI:SS'),'{4}')""".format(table_name,
                                                                                                  schema_name,
                                                                                                  in_database, start_time_str , user)
            write_in_db(sql, cur1)
            print("Log For date type validation updated ", table_name)
            exit = 1
        else:
            pass

    #     account_identifier = account
    #     user = user
    #     # password = password
    #     encoded_password = urllib.parse.quote_plus(password)
    #     database_name = 'validation_db'
    #     schema_name_s = 'QA'
    #     # conn_string = f"snowflake://{user}:{password}@{account_identifier}/{database_name}/{schema_name_s}"
    #     conn_string = f"snowflake://{user}:{encoded_password}@{account_identifier}/{database_name}/{schema_name_s}"
    #     engine = create_engine(conn_string)

    # df5['COLUMN_LEN_COMPAR'][0]='True'
    # df5['COLUMN_LEN_COMPAR'][3]='True'
    # print(df5['COLUMN_LEN_COMPAR'][0])
    # print(df5['COLUMN_LEN_COMPAR'][3])
    # print(df5['COLUMN_LEN_COMPAR']).eq(True).all()
    # entering the log table
    try:
        if (df5['COLUMN_AND_LEN_COMPAR'] == "True").all():
            if (df5['COLUMN_COMPAR'] == "True").all():

                # Get the current timestamp as the start time
                start_time = datetime.datetime.now()

                # Convert the start time to a string in the desired format (if needed)
                start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')

                # Append the string to the global list
                global_start_time_list.append(start_time_str)

                sql = """insert into  GMigrate_Report_DB.QA.datatypevalidation_error_report(Source_DBMS,Target_DBMS,TABLE_NAME,RESULT_STATUS,CREATED_ON,CREATED_BY)
            values('SQL_SERVER','SNOWFLAKE','{2}.{1}.{0}','valid',TO_TIMESTAMP('{3}', 'YYYY-MM-DD HH24:MI:SS'),'{4}')""".format(table_name, schema_name, in_database, start_time_str , user)
                write_in_db(sql, cur1)
                print("Log For date type validation updated ", table_name)
            else:

                # Get the current timestamp as the start time
                start_time = datetime.datetime.now()

                # Convert the start time to a string in the desired format (if needed)
                start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')

                # Append the string to the global list
                global_start_time_list.append(start_time_str)

                sql = """insert into GMigrate_Report_DB.QA.datatypevalidation_error_report(Source_DBMS,Target_DBMS,TABLE_NAME,RESULT_STATUS,CREATED_ON,CREATED_BY)
            values('SQL_SERVER','SNOWFLAKE','{2}.{1}.{0}','Not-valid',TO_TIMESTAMP('{3}', 'YYYY-MM-DD HH24:MI:SS'),'{4}')""".format(table_name, schema_name, in_database, start_time_str , user)
                write_in_db(sql, cur1)
                # writing the result to snowflake
                account_identifier = account
                user = user
                encoded_password = urllib.parse.quote_plus(password)
                database_name = 'GMigrate_Report_DB'
                schema_name_s = 'QA'
                engine = create_engine(
                    'snowflake://{user}:{password}@{account_identifier}/{database_name}/{schema_name}'.format(
                        user=user,
                        password=encoded_password,
                        account_identifier=account_identifier,
                        database_name=database_name,
                        schema_name=schema_name_s
                    )
                )
                con = engine.connect()
                con.execute("use warehouse "+warehouse)
                df5.to_sql('Datatype_mismatch_resultset', con=con, if_exists='append', index=False)
                print("Log For date type validation updated for table ", table_name)

        else:
            if (exit != 1):
                # Get the current timestamp as the start time
                start_time = datetime.datetime.now()

                # Convert the start time to a string in the desired format (if needed)
                start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')

                # Append the string to the global list
                global_start_time_list.append(start_time_str)

                sql = """insert into  GMigrate_Report_DB.QA.datatypevalidation_error_report(Source_DBMS,Target_DBMS,TABLE_NAME,RESULT_STATUS,CREATED_ON,CREATED_BY)
                values('SQL_SERVER','SNOWFLAKE','{2}.{1}.{0}','Not-valid',TO_TIMESTAMP('{3}', 'YYYY-MM-DD HH24:MI:SS'),'{4}')""".format(table_name, schema_name, in_database, start_time_str, user)
                write_in_db(sql, cur1)
                # writing the result to snowflake
                account_identifier = account
                user = user
                encoded_password = urllib.parse.quote_plus(password)
                database_name = 'GMigrate_Report_DB'
                schema_name_s = 'QA'
                engine = create_engine(
                    'snowflake://{user}:{password}@{account_identifier}/{database_name}/{schema_name}'.format(
                        user=user,
                        password=encoded_password,
                        account_identifier=account_identifier,
                        database_name=database_name,
                        schema_name=schema_name_s
                    )
                )
                con = engine.connect()
                con.execute("use warehouse "+warehouse)
                df5.to_sql('Datatype_mismatch_resultset', con=con, if_exists='append', index=False)
                print("Log For date type validation updated for table", table_name)
    except Exception as e:
        if (exit != 1):

            # Get the current timestamp as the start time
            start_time = datetime.datetime.now()

            # Convert the start time to a string in the desired format (if needed)
            start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')

            # Append the string to the global list
            global_start_time_list.append(start_time_str)


            sql = """insert into  GMigrate_Report_DB.QA.datatypevalidation_error_report(Source_DBMS,Target_DBMS,TABLE_NAME,RESULT_STATUS,CREATED_ON,CREATED_BY)
            values('SQL_SERVER','SNOWFLAKE','{2}.{1}.{0}','Error in the datatype comparison block',TO_TIMESTAMP('{3}', 'YYYY-MM-DD HH24:MI:SS'),'{4}');""".format(table_name,
                                                                                                      schema_name,
                                                                                                      in_database, start_time_str, user)
            write_in_db(sql, cur1)
            print("Log Updated in table for ", table_name)
            print("Error is the datatype comparison block, error raised is ---> " + str(e))


def data_comparison(table_name, schema_name, in_database, server, user, password, account, warehouse, role, cnn,
                          cur, cur1):
    set_xml_flag = ''
    # if table_name=='Employee':
    #     pass
    # else:
    #     return
    src_cnt = 0
    tar_cnt = 0
    try:
        cur.execute(f"select count(*) from {in_database}.{schema_name}.{table_name}")
        source_cnt = cur.fetchall()[0]
        src_cnt = int(source_cnt[0])
        print("datatype for src count")
        print(src_cnt)
        print(type(src_cnt))
        cur1.execute("use warehouse " + warehouse)
        # --------------------check if table exists in snowflake------------------------
        try:
            sql = """select 1 from {2}.{1}.{0} limit 1""".format(table_name, schema_name,
                                                                 in_database)

            cur1.execute(sql)
            print("Table " + table_name + " exists in snowflake")
        except:

            # Get the current timestamp as the start time
            start_time = datetime.datetime.now()

            # Convert the start time to a string in the desired format (if needed)
            start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')

            # Append the string to the global list
            global_start_time_list.append(start_time_str)

            sql = """insert into GMigrate_Report_DB.QA.datavalidation_error_report(Source_DBMS,Target_DBMS,TABLE_NAME,RESULT_STATUS,SOURCE_RECORD_COUNT,TARGET_RECORD_COUNT,CREATED_ON,CREATED_BY) values('SQL_SERVER','SNOWFLAKE','{2}.{1}.{0}','Table does not exist in snowflake',{5},0,TO_TIMESTAMP('{3}', 'YYYY-MM-DD HH24:MI:SS'),'{4}');""".format(
                table_name, schema_name, in_database, start_time_str ,user,src_cnt)
            write_in_db(sql, cur1)
            print("Log Updated for table " + table_name)
            print("Table " + table_name + " does not exist in snowflake")
            return
        cur1.execute(f"select count(*) from {in_database}.{schema_name}.{table_name}")
        target_cnt = cur1.fetchall()[0]
        tar_cnt = int(target_cnt[0])
        print("datatype for tar count")
        print(tar_cnt)
        print(type(tar_cnt))

        # --------------------Datatype Comparison--------------------------------------
        print("Datatype comparison started for table " + table_name)
        try:
            datatype_comparison(table_name, schema_name, in_database, server, user, password, account, warehouse,
                                      role, cnn, cur, cur1)
            print("Datatype comparison done for table " + table_name)
        except Exception as e:
            print("Datatype comparison errored for table " + table_name + " with error ---> " + str(e))

        # --------------------Fetching primary keys from SQL server-------------------------------------
        try:
            sql_for_pk = "select UPPER(COLUMN_NAME) as PK_COLUMN_NAME from {2}.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE WHERE TABLE_NAME='{0}' AND TABLE_SCHEMA='{1}' and (CONSTRAINT_NAME like 'PK%' or CONSTRAINT_NAME LIKE 'pk%');".format(
                table_name, schema_name, in_database)
            df_cols = pd.read_sql(sql_for_pk, cnn)
            df_cols = pd.DataFrame(df_cols)
            pk_cols_res = df_cols.empty
            order_by_cols = ''
            if pk_cols_res == True:
                order_by_cols = ''
            else:
                for index, row in df_cols.iterrows():
                    # print(row['PK_COLUMN_NAME'])
                    order_by_cols = order_by_cols + ',' + str(row['PK_COLUMN_NAME'])
                order_by_cols = order_by_cols.removeprefix(',')
            print('primary keys: ' + order_by_cols)
        except Exception as e:

            # Get the current timestamp as the start time
            start_time = datetime.datetime.now()

            # Convert the start time to a string in the desired format (if needed)
            start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')

            # Append the string to the global list
            global_start_time_list.append(start_time_str)

            order_by_cols = ''
            sql = """insert into  GMigrate_Report_DB.QA.datavalidation_error_report(Source_DBMS,Target_DBMS,TABLE_NAME,RESULT_STATUS,SOURCE_RECORD_COUNT,TARGET_RECORD_COUNT,CREATED_ON,CREATED_BY)
            values('SQL_SERVER','SNOWFLAKE','{2}.{1}.{0}','Error while fetching Primary keys, validation on first few column order',{5},{6},TO_TIMESTAMP('{3}', 'YYYY-MM-DD HH24:MI:SS'),'{4}');""".format(
                table_name, schema_name, in_database, start_time_str , user,src_cnt,tar_cnt)
            write_in_db(sql, cur1)
            print("Log Updated for table " + table_name)
            print("Error while fetching Primary keys from SQL server, error raised is ---> " + str(e))
            # --------------------Fetching data from snowflake------------------------------
        try:
            if order_by_cols == '':
                # sql = """select * from {2}.{1}.{0} order by 1,2,3 ASC""".format(table_name, schema_name,
                #                                                                 in_database)
                sql = """select * from {2}.{1}.{0}""".format(table_name, schema_name,
                                                                                in_database)
                b = cur1.execute(sql)
                print("Fetching data from snowflake")
                df1 = pd.DataFrame(b)
                print(df1)
            else:
                """sql = select * from {2}.{1}.{0} order by {3} ASC.format(table_name, schema_name,
                                                                              in_database, order_by_cols)"""
                sql = "select * from {2}.{1}.{0} ;".format(table_name, schema_name, in_database)

                print(sql)
                b = cur1.execute(sql)
                print("Fetching data from snowflake")
                df1 = pd.DataFrame(b)
                print(df1)
        except Exception as e:

            # Get the current timestamp as the start time
            start_time = datetime.datetime.now()

            # Convert the start time to a string in the desired format (if needed)
            start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')

            # Append the string to the global list
            global_start_time_list.append(start_time_str)

            sql = """insert into  GMigrate_Report_DB.QA.datavalidation_error_report(Source_DBMS,Target_DBMS,TABLE_NAME,RESULT_STATUS,SOURCE_RECORD_COUNT,TARGET_RECORD_COUNT,CREATED_ON,CREATED_BY)
            values('SQL_SERVER','SNOWFLAKE','{2}.{1}.{0}','Error while fetching data from snowflake, validation failed',{5},0,TO_TIMESTAMP('{3}', 'YYYY-MM-DD HH24:MI:SS'),'{4}');""".format(
                table_name, schema_name, in_database, start_time_str, user,src_cnt)
            write_in_db(sql, cur1)
            print("Log Updated for table " + table_name)
            print(order_by_cols)
            print("Error while fetching data from snowflake, error raised is ---> " + str(e))
            return

        # -----------------Checking if table has data------------------------------------
        result = df1.empty
        if result == True:

            # Get the current timestamp as the start time
            start_time = datetime.datetime.now()

            # Convert the start time to a string in the desired format (if needed)
            start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')

            # Append the string to the global list
            global_start_time_list.append(start_time_str)

            sql = """insert into GMigrate_Report_DB.QA.datavalidation_error_report(Source_DBMS,Target_DBMS,TABLE_NAME,RESULT_STATUS,SOURCE_RECORD_COUNT,TARGET_RECORD_COUNT,CREATED_ON,CREATED_BY) values('SQL_SERVER','SNOWFLAKE','{2}.{1}.{0}','Table has no data',{5},{6},TO_TIMESTAMP('{3}', 'YYYY-MM-DD HH24:MI:SS'),'{4}');""".format(
                table_name, schema_name, in_database, start_time_str, user,src_cnt,tar_cnt)
            write_in_db(sql, cur1)
            print("Log Updated for table " + table_name)
            print("Table " + table_name + " has no data in snowflake")
            return
        else:
            df1 = df1.replace(np.nan, 'Na', regex=True)
            df1 = df1.fillna(np.nan).replace([np.nan], [None])
            df1.fillna("", inplace=True)
            print("Table " + table_name + " has data in snowflake...Data validation started...")

        # --------------------fetching data from sql server-------------------------------
        try:
            print("Fetching data from SQL server")
            try:
                if order_by_cols == '':
                    # sql = "select * from {2}.{1}.{0} order by 1,2,3 ASC;".format(table_name, schema_name, in_database)
                    sql = "select * from {2}.{1}.{0};".format(table_name, schema_name, in_database)
                    df = pd.read_sql(sql, cnn)
                    df = pd.DataFrame(df)
                    print('&&&&&&&&&&&&&&&&')
                    print(df)
                    # df.fillna('',inplace=True)
                    df = df.replace(np.nan, 'Na', regex=True)
                    df = df.fillna(np.nan).replace([np.nan], [None])
                    df = df.replace('', 'Na', regex=True)
                else:
                    """sql = "select * from {2}.{1}.{0} order by {3} ASC;".format(table_name, schema_name, in_database,
                                                                               order_by_cols)"""
                    sql = "select * from {2}.{1}.{0} ;".format(table_name, schema_name, in_database)

                    print(sql)
                    df = pd.read_sql(sql, cnn)
                    df = pd.DataFrame(df)
                    ptint('CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC')
                    print(df)
                    # df.fillna('',inplace=True)
                    df = df.replace(np.nan, 'Na', regex=True)
                    df = df.fillna(np.nan).replace([np.nan], [None])
                    df = df.replace('', 'Na', regex=True)
            except:
                #sql = "select * from {1}.{0} order by 1 ASC;".format(table_name, schema_name)
                sql = "select * from {1}.{0} ;".format(table_name, schema_name)
                df = pd.read_sql(sql, cnn)
                df = pd.DataFrame(df)
                # df.fillna('',inplace=True)
                df = df.replace(np.nan, 'Na', regex=True)
                df = df.fillna(np.nan).replace([np.nan], [None])
                df = df.replace('', 'Na', regex=True)
            df.columns = map(str.upper, df.columns)
            print(df)
        except Exception as e:

            # Get the current timestamp as the start time
            start_time = datetime.datetime.now()

            # Convert the start time to a string in the desired format (if needed)
            start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')

            # Append the string to the global list
            global_start_time_list.append(start_time_str)

            sql = """insert into  GMigrate_Report_DB.QA.datavalidation_error_report(Source_DBMS,Target_DBMS,TABLE_NAME,RESULT_STATUS,SOURCE_RECORD_COUNT,TARGET_RECORD_COUNT,CREATED_ON,CREATED_BY)
            values('SQL_SERVER','SNOWFLAKE','{2}.{1}.{0}','Error while fetching data from SQL server, validation failed',0,{5},TO_TIMESTAMP('{3}', 'YYYY-MM-DD HH24:MI:SS'),'{4}');""".format(
                table_name, schema_name, in_database, start_time_str,user,tar_cnt)
            write_in_db(sql, cur1)
            print("Log Updated for table " + table_name)
            print("Error while fetching data from SQL server, error raised is ---> " + str(e))
            return
        #             sql = "select * from {1}.{0} order by 1 ASC;".format(table_name, schema_name)
        #             df = pd.read_sql(sql, cnn)
        #             print("Fetching data from SQL server")
        #             df = pd.DataFrame(df)
        #             # df.fillna('',inplace=True)
        #             df = df.replace(np.nan, 'Na', regex=True)
        #             df = df.fillna(np.nan).replace([np.nan], [None])
        # --------------------handling XML cases------------------------------------------
        try:
            print("Handling XML cases for Table " + table_name)
            sql1 = """Desc table {2}.{1}.{0}""".format(table_name, schema_name, in_database)
            c = cur1.execute(sql1)
            dfd = pd.DataFrame(c)
            counter = -1
            for d in range(0, len(dfd)):
                counter = counter + 1
                if (dfd[1][d] == "DATE"):
                    a = dfd[0][d]
                    #                     print(df[a])
                    df[a] = df[a].replace('Na', '1700-01-01')
                    df[a] = pd.to_datetime(df[a], format="%Y-%m-%d",errors="coerce").dt.date
                    df = df.replace(np.nan, '', regex=True)
                    df = df.fillna(np.nan).replace([np.nan], [None])
                    # df[a] = df[a].replace('1700-01-01', 'Na')
                    df[a]=df[a].apply(lambda x: 'Na' if x == datetime.date(1700, 1, 1) else x)

                    df1[counter] = pd.to_datetime(df1[counter], format="%Y-%m-%d",errors="coerce").dt.date
                    df1[counter] = df1[counter].fillna('Na')
                    # for ij in range(0, len(df[a])):
                #                         print(df[a][ij])
                # if (df[a][ij] != 'Na'):
                # df[a][ij] = pd.to_datetime(df[a][ij], format="%Y-%m-%d")
                # df = df.replace(np.nan, '', regex=True)
                # df = df.fillna(np.nan).replace([np.nan], [None])
                elif "TIME(" in dfd[1][d]:
                    a = dfd[0][d]
                    df[a] = df[a].replace('Na', '00:00:00')
                    time = df[a].str.partition('.')
                    df[a] = time[0]
                    print(df[a])
                    # df['flag']=time[2]
                    df[a] = pd.to_datetime(df[a], format="%H:%M:%S", errors="coerce").dt.time
                    # df.loc[df['flag'] == '99', a] = 'Na'
                    df[a] = df[a].replace('00:00:00', 'Na')
                    df = df.replace(np.nan, '', regex=True)
                    df = df.fillna(np.nan).replace([np.nan], [None])
                    #######################################################################
                    df1[counter] = df1[counter].astype(str).str.split('.',expand=True)[0]
                    df1[counter] = pd.to_datetime(df1[counter], format="%H:%M:%S", errors="coerce").dt.time
                    # df[a]=pd.to_datetime(df[a], format="%H:%M:%S").dt.time
                    # df.drop(['flag'],axis=1, inplace=True)
                    # ---
                    # for ij in range(0, len(df[a])):
                    #     if (df[a][ij] != 'Na'):
                    #         time,dot,sec=df[a][ij].partition('.')
                    #         df[a][ij]=time
                    #         df[a][ij] = pd.to_datetime(df[a][ij], format="%H:%M:%S")
                    #         df = df.replace(np.nan, '', regex=True)
                    #         df = df.fillna(np.nan).replace([np.nan], [None])
                    # df[a]=pd.to_datetime(df[a], format="%H:%M:%S").dt.time
                elif re.search("^NUMBER\(\d+.[1-9]+\)", dfd[1][d]):
                    # a = dfd[0][d]
                    df1 = df1.replace(np.nan, '', regex=True)
                    df1 = df1.fillna(np.nan).replace([np.nan], [None])
                    df1[counter] = df1[counter].replace('Na', -9999.9999)
                    df1[counter] = pd.to_numeric(df1[counter])
                    df1[counter] = df1[counter].replace(-9999.9999, 'Na')
                    # for ij in range(0, len(df1[counter])):
                    #     if (df1[counter][ij] != 'Na'):
                    #         df1[counter][ij]=pd.to_numeric(df1[counter][ij])
                    #         df1 = df1.replace(np.nan, '', regex=True)
                    #         df1 = df1.fillna(np.nan).replace([np.nan], [None])
            print("Handled XML cases for Table " + table_name)
        except Exception as e:

            # Get the current timestamp as the start time
            start_time = datetime.datetime.now()

            # Convert the start time to a string in the desired format (if needed)
            start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')

            # Append the string to the global list
            global_start_time_list.append(start_time_str)

            sql = """insert into GMigrate_Report_DB.QA.datavalidation_error_report(Source_DBMS,Target_DBMS,TABLE_NAME,RESULT_STATUS,SOURCE_RECORD_COUNT,TARGET_RECORD_COUNT,CREATED_ON,CREATED_BY) values('SQL_SERVER','SNOWFLAKE','{2}.{1}.{0}','Error while handling XML cases',{5},{6},TO_TIMESTAMP('{3}', 'YYYY-MM-DD HH24:MI:SS'),'{4}');""".format(
                table_name, schema_name, in_database, start_time_str,user,src_cnt,tar_cnt)
            write_in_db(sql, cur1)
            print("Log Updated for table " + table_name)
            print("Error while handling XML cases, error raised is ---> " + str(e))
        # ---------------------Handling Boolean data---------------------------
        try:
            print("Handling boolean data for Table " + table_name)
            sql1 = """SELECT UPPER(COLUMN_NAME), DATA_TYPE FROM {2}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{0}' and TABLE_SCHEMA='{1}';""".format(
                table_name, schema_name, in_database)
            dfc = pd.read_sql(sql1, cnn)
            dfc = pd.DataFrame(dfc)
            #             print(dfc)
            for dc in range(0, len(dfc)):
                if (dfc["DATA_TYPE"][dc] == "BIT"):
                    re1 = dfc["COLUMN_NAME"][dc]
                    # print(re)
                    df[re1].replace([True, False], [1, 0], inplace=True)
            print("Handled boolean data for Table " + table_name)
        except Exception as e:

            # Get the current timestamp as the start time
            start_time = datetime.datetime.now()

            # Convert the start time to a string in the desired format (if needed)
            start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')

            # Append the string to the global list
            global_start_time_list.append(start_time_str)

            sql = """insert into  GMigrate_Report_DB.QA.datavalidation_error_report(Source_DBMS,Target_DBMS,TABLE_NAME,RESULT_STATUS,SOURCE_RECORD_COUNT,TARGET_RECORD_COUNT,CREATED_ON,CREATED_BY) values('SQL_SERVER','SNOWFLAKE','{2}.{1}.{0}','Error while handling Boolean data',{5},{6},TO_TIMESTAMP('{3}', 'YYYY-MM-DD HH24:MI:SS'),'{4}');""".format(
                table_name, schema_name, in_database, start_time_str, user,src_cnt,tar_cnt)
            write_in_db(sql, cur1)
            print("Log Updated for table " + table_name)
            print("Error while handling Boolean data, error raised is ---> " + str(e))
        # ---------------------Checking validity of XML data---------------------------
        try:
            print("Checking validity of XML data for " + table_name)
            sql = """SELECT UPPER(COLUMN_NAME) as COLUMN_NAME, DATA_TYPE,ISNULL(ISNULL(CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION),
            DATETIME_PRECISION) LENGTH,NUMERIC_SCALE FROM {2}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{0}' AND TABLE_SCHEMA='{1}';""".format(
                table_name, schema_name, in_database)
            df_xml = pd.read_sql(sql, cnn)
            df_xml = pd.DataFrame(df_xml)
            for d1_x in range(0, len(df_xml)):
                if df_xml["DATA_TYPE"][d1_x] == "xml":
                    a1 = df_xml["COLUMN_NAME"][d1_x]
                    a1 = d1_x
                    sql = """select count(*) from(select check_xml("{0}") as result from {3}.{2}.{1}) where result!=Null""".format(
                        df_xml["COLUMN_NAME"][d1_x], table_name, schema_name, in_database)
                    cur1.execute("use warehouse "+warehouse)
                    b = cur1.execute(sql)
                    b = pd.DataFrame(b)
                    if (b[0][0] == 0):
                        # print([df3["COLUMN_NAME"][d1]])
                        # print(df[df3["COLUMN_NAME"][d1]])
                        df[df_xml["COLUMN_NAME"][
                            d1_x]] = "VALID"  # we are changing to valid because in check_xml its valid for passing true we are changing to valid, if not valid it will take the xml itself and automatically it will fail.
                        df1[a1] = "VALID"
            print("Checked validity of XML data for " + table_name)
        except Exception as e:

            # Get the current timestamp as the start time
            start_time = datetime.datetime.now()

            # Convert the start time to a string in the desired format (if needed)
            start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')

            # Append the string to the global list
            global_start_time_list.append(start_time_str)

            sql = """insert into  GMigrate_Report_DB.QA.datavalidation_error_report(Source_DBMS,Target_DBMS,TABLE_NAME,RESULT_STATUS,SOURCE_RECORD_COUNT,TARGET_RECORD_COUNT,CREATED_ON,CREATED_BY)
            values('SQL_SERVER','SNOWFLAKE','{2}.{1}.{0}','Error while checking XML validity',{5},{6},TO_TIMESTAMP('{3}', 'YYYY-MM-DD HH24:MI:SS'),'{4}');""".format(table_name,
                                                                                                 schema_name,
                                                                                                 in_database, start_time_str, user,src_cnt,tar_cnt)
            write_in_db(sql, cur1)
            print("Log Updated in table for" + table_name)
            print("Error while checking XML validity, error raised is ---> " + str(e))

        # ----------------------Comparing Data between snowflake and sql server---------------------------
        try:
            print("Comparison between data started for Table " + table_name)
            # for index1, rows1 in df.iterrows():
            #     for column1, value in rows1.items():
            #         if isinstance(value, str):
            #             df.at[index1, column1] = re.sub(r'\\+', '', value)
            df.replace('','Na',inplace=True)
            df.replace('NULL', 'Na', inplace=True)
            print(df)
            print(df1)
            ou = ((df.values == df1.values))
            # print(type(ou))
            # ou=np.all(ou)
            try:
                cdf = pd.DataFrame(ou)
                list_of_cols = df.columns.values.tolist()
                dict_of_cols = dict(enumerate(list_of_cols))
                cdf.rename(columns=dict_of_cols, inplace=True)
                print(cdf)
                print("inserting the error records to summary table if exists...\n")
                for cols in list_of_cols:
                    li = cdf[cdf[cols] == False].index.tolist()
                    final_list = [x + 1 for x in li]
                    if li:
                        # Get the current timestamp as the start time
                        start_time = datetime.datetime.now()

                        # Convert the start time to a string in the desired format (if needed)
                        start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')

                        # Append the string to the global list
                        global_start_time_list.append(start_time_str)

                        sql1 = """insert into GMigrate_Report_DB.QA.DATAVALIDATION_DETAIL_REPORT(Source_DBMS,Target_DBMS,TABLE_NAME,COLUMN_NAME,ROW_NUMBERS,CREATED_ON,CREATED_BY) values('SQL_SERVER','SNOWFLAKE','{4}.{2}.{3}','{0}','{1}',TO_TIMESTAMP('{5}', 'YYYY-MM-DD HH24:MI:SS'),'{6}')""".format(
                            str(cols), str(final_list), schema_name, table_name, in_database, start_time_str, user)
                        li = []
                        final_list = []
                        write_in_db(sql1, cur1)
            #                 sql1 = """Desc table {2}.{1}.{0} """.format(table_name, schema_name, in_database)
            #                 b11 = cur1.execute(sql1)
            #                 b11 = pd.DataFrame(b11)
            #                 print("inserting the error records to summary table if exists...\n")
            #                 for s in range(0, len(ou)):
            #                     for js in range(0, len(b11)):
            #                         # print(js)
            #                         if (ou[s][js] == False):
            #                             #print(s, js)
            #                             a_su = b11[0][js]
            #                             #print(a_su)
            #                             b_su = s
            #                             sql1 = """insert into validation_db.QA.DATAVALIDATION_ERROR_SUMMARY values('{2}.{3}','{0}','{1}',sysdate())""".format(
            #                                 a_su, b_su, schema_name, table_name)
            #                             print(str(a_su)+" "+str(b_su))
            #                             write_in_db(sql1,cur1)
            except Exception as e:

                # Get the current timestamp as the start time
                start_time = datetime.datetime.now()

                # Convert the start time to a string in the desired format (if needed)
                start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')

                # Append the string to the global list
                global_start_time_list.append(start_time_str)

                sql = """insert into  GMigrate_Report_DB.QA.datavalidation_error_report(Source_DBMS,Target_DBMS,TABLE_NAME,RESULT_STATUS,SOURCE_RECORD_COUNT,TARGET_RECORD_COUNT,CREATED_ON,CREATED_BY)
                values('SQL_SERVER','SNOWFLAKE','{2}.{1}.{0}','Failed to enter in Error Summary table',{5},{6},TO_TIMESTAMP('{3}', 'YYYY-MM-DD HH24:MI:SS'),'{4}');""".format(table_name,
                                                                                                          schema_name,
                                                                                                          in_database,start_time_str,
                                                                                                          user,src_cnt,tar_cnt)
                write_in_db(sql, cur1)
                print("Log Updated for table " + table_name)
                print("Error while comparing data, error raised is ---> " + str(e))

            # print(ou)
            if ou.all() == True:

                # Get the current timestamp as the start time
                start_time = datetime.datetime.now()

                # Convert the start time to a string in the desired format (if needed)
                start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')

                # Append the string to the global list
                global_start_time_list.append(start_time_str)

                sql = """insert into  GMigrate_Report_DB.QA.datavalidation_error_report(Source_DBMS,Target_DBMS,TABLE_NAME,RESULT_STATUS,SOURCE_RECORD_COUNT,TARGET_RECORD_COUNT,CREATED_ON,CREATED_BY)
            values('SQL_SERVER','SNOWFLAKE','{2}.{1}.{0}','Matched',{5},{6},TO_TIMESTAMP('{3}', 'YYYY-MM-DD HH24:MI:SS'),'{4}');
            """.format(table_name, schema_name, in_database, start_time_str,user,src_cnt,tar_cnt)
                write_in_db(sql, cur1)
            else:

                # Get the current timestamp as the start time
                start_time = datetime.datetime.now()

                # Convert the start time to a string in the desired format (if needed)
                start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')

                # Append the string to the global list
                global_start_time_list.append(start_time_str)

                sql = """insert into  GMigrate_Report_DB.QA.datavalidation_error_report(Source_DBMS,Target_DBMS,TABLE_NAME,RESULT_STATUS,SOURCE_RECORD_COUNT,TARGET_RECORD_COUNT,CREATED_ON,CREATED_BY)
            values('SQL_SERVER','SNOWFLAKE','{2}.{1}.{0}','Not-Matched',{5},{6},TO_TIMESTAMP('{3}', 'YYYY-MM-DD HH24:MI:SS'),'{4}');
            """.format(table_name, schema_name, in_database, start_time_str,user,src_cnt,tar_cnt)
                write_in_db(sql, cur1)
            print("Data Comparison log Updated for table ", table_name)
            print("Comparison between data done for Table " + table_name)
        except Exception as e:

            # Get the current timestamp as the start time
            start_time = datetime.datetime.now()

            # Convert the start time to a string in the desired format (if needed)
            start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')

            # Append the string to the global list
            global_start_time_list.append(start_time_str)

            sql = """insert into  GMigrate_Report_DB.QA.datavalidation_error_report(Source_DBMS,Target_DBMS,TABLE_NAME,RESULT_STATUS,SOURCE_RECORD_COUNT,TARGET_RECORD_COUNT,CREATED_ON,CREATED_BY)
            values('SQL_SERVER','SNOWFLAKE','{2}.{1}.{0}','Data Comparison failed due to some error',{5},{6},TO_TIMESTAMP('{3}', 'YYYY-MM-DD HH24:MI:SS'),'{4}');""".format(table_name,
                                                                                                        schema_name,
                                                                                                        in_database,start_time_str,
                                                                                                        user,src_cnt,tar_cnt)
            write_in_db(sql, cur1)
            print("Log Updated for table " + table_name)
            print("Error in the data comparison block, error raised is ---> " + str(e))
    except Exception as e:

        # Get the current timestamp as the start time
        start_time = datetime.datetime.now()

        # Convert the start time to a string in the desired format (if needed)
        start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')

        # Append the string to the global list
        global_start_time_list.append(start_time_str)

        sql = """insert into  GMigrate_Report_DB.QA.datavalidation_error_report(Source_DBMS,Target_DBMS,TABLE_NAME,RESULT_STATUS,SOURCE_RECORD_COUNT,TARGET_RECORD_COUNT,CREATED_ON,CREATED_BY)
            values('SQL_SERVER','SNOWFLAKE','{2}.{1}.{0}','Data Comparison failed due to some internal error...please check logs!',{5},{6},TO_TIMESTAMP('{3}', 'YYYY-MM-DD HH24:MI:SS'),'{4}');""".format(
            table_name, schema_name, in_database, start_time_str,user,src_cnt,tar_cnt)
        write_in_db(sql, cur1)
        print("Log Updated for " + table_name)
        print("Data comparison function errored out, error raised --> " + str(e))


def validation(server, user, password, account, warehouse, role, dfs, source_username, source_password):
    global current_progress
    global length
    global t_End
    global total_entries
    global global_start_time_list
    dfs = dfs.reset_index()
    length = len(dfs)
    global_start_time_list.clear()
    # ----establishing connections----------------------------
    try:
        cnn_sql = ('Driver={SQL Server};'
                   f'Server={server};'
                   f'UID={source_username};'
                   f'PWD={source_password};')
        cnn = pyo.connect(cnn_sql)
        cur = cnn.cursor()
        print('opened...')
    except Exception as e:
        print("Couldn't connect to SQL database. Error raised is ---> " + str(e))
        return
    try:
        # config=helper.read_config()
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            role=role

        )
        cur1 = conn.cursor()
        print('validation STARTED...')
    except Exception as e:
        print("Couldn't connect to snowflake database. Error raised is ---> " + str(e))
        return
    # ------------Creating VALIDATION_DB database-------------
    sql = "CREATE DATABASE IF NOT EXISTS GMigrate_Report_DB;"
    write_in_db(sql, cur1)

        # ------------Use VALIDATION_DB database-------------
    sql = "USE DATABASE GMigrate_Report_DB;"
    write_in_db(sql, cur1)

    # ------------Creating QA schema--------------------------
    sql = "CREATE SCHEMA IF NOT EXISTS QA;"
    write_in_db(sql, cur1)

    # ------------Use QA schema-------------
    sql = "USE SCHEMA QA;"
    write_in_db(sql, cur1)

    # ------creation of sequences------------------------
    sql = "CREATE SEQUENCE IF NOT EXISTS seq_datatypeval START = 1 INCREMENT = 1;"
    write_in_db(sql, cur1)
    sql = "CREATE SEQUENCE IF NOT EXISTS seq_dataval START = 1 INCREMENT = 1;"
    write_in_db(sql, cur1)
    sql = "CREATE SEQUENCE IF NOT EXISTS seq_datavaldetail START = 1 INCREMENT = 1;"
    write_in_db(sql, cur1)

    sql ="CREATE TABLE IF NOT EXISTS DATATYPEVALIDATION_ERROR_REPORT(SEQ_ORDER_ID number(10,0) DEFAULT seq_datatypeval.nextval,Source_DBMS varchar,Target_DBMS varchar,TABLE_NAME varchar(300),RESULT_STATUS VARCHAR(300),CREATED_ON TIMESTAMP, CREATED_BY VARCHAR(300));"
    write_in_db(sql,cur1)

    sql = "CREATE TABLE IF NOT EXISTS DATAVALIDATION_ERROR_REPORT(SEQ_ORDER_ID number(10,0) DEFAULT seq_dataval.nextval,Source_DBMS varchar,Target_DBMS varchar,TABLE_NAME varchar(300),RESULT_STATUS VARCHAR(300),SOURCE_RECORD_COUNT int,TARGET_RECORD_COUNT int,CREATED_ON TIMESTAMP, CREATED_BY VARCHAR(300));"
    write_in_db(sql, cur1)

    sql = "CREATE TABLE IF NOT EXISTS DATAVALIDATION_DETAIL_REPORT(SEQ_ORDER_ID number(10,0) DEFAULT seq_datavaldetail.nextval,Source_DBMS varchar,Target_DBMS varchar,TABLE_NAME varchar(300),COLUMN_NAME VARCHAR(300),ROW_NUMBERS VARCHAR(16777216),CREATED_ON TIMESTAMP, CREATED_BY VARCHAR(300));"
    write_in_db(sql, cur1)
    #     print("List of databases\n")
    #     sql = "select name from sys.databases;"
    #     dfsd = pd.read_sql(sql, cnn)
    #     dfsd = pd.DataFrame(dfsd)
    #     print(dfsd)
    for i in range(0, len(dfs)):
        database = dfs['TABLE_CATALOG'][i]
        schema = dfs['TABLE_SCHEMA'][i]
        table_name = dfs['TABLE_NAME'][i]
        print("Database under validation is " + database)
        sql = "USE {0}".format(database)
        cnn.execute(sql)
        #     sql = "SELECT Distinct TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES;"  # -- WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA='HumanResources';"
        #     dfs = pd.read_sql(sql, cnn)
        #     dfs = pd.DataFrame(dfs)
        #     print(dfs)
        print("Schema under validation is " + schema)
        # sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA='{0}';".format(
        #     schema)
        # dfs = pd.read_sql(sql, cnn)
        # dfs = pd.DataFrame(dfs)
        # print("Displaying table names under schema "+schema)
        # print(dfs)
        t_start = datetime.datetime.now()
        print(t_start)
        # for i in range(0,len(dfs)):
        # print(dfs['TABLE_CATALOG'][i])
        t_1 = datetime.datetime.now()
        timestamps.append(t_1)
        data_comparison(table_name, schema, database, server, user, password, account, warehouse, role, cnn, cur,
                              cur1)
        t_2 = datetime.datetime.now()
        timestamps.append(t_2)
    print("END...")
    cur1.execute('DROP TABLE GMIGRATE_REPORT_DB.QA.DATATYPEVALIDATION_ERROR_REPORT')
    t_end = datetime.datetime.now()
    print(t_end)
    print("Time b/w Code Start & end:", t_end - t_start)


# df_db = {'TABLE_CATALOG':['AdventureWorks2019'],
#        'TABLE_SCHEMA':['Sales'],
#        'TABLE_NAME':['SalesPersonQuotaHistory'],
#        'TABLE_TYPE':['Table']}
# dfs=pd.DataFrame(df_db)
# validation('IXPF1CEZAP\SQLEXPRESS19','Thufail','Aa@1Aa@1','gj55794.ap-southeast-1','compute_wh','accountadmin',dfs)
print('done')


def start_time_function():
    # Access the global variables
    global call_count
    global global_start_time_list

    # Increment the call_count on each call
    call_count += 1

    # If it's the second call, return the start_time_list and reset the count and the list
    if call_count == 18:
        call_count = 0
        start_time_list = global_start_time_list.copy()
        global_start_time_list.clear()
        return start_time_list
    else:
        return global_start_time_list


def progress_vd():
    global length
    global timestamps
    global current_progress
    global t_End
    total_entries = (length * 2) + 2

    if total_entries == 2:
       total_entries = 1

    if round(current_progress) <= 100:
        progress_per_entry = 100 / total_entries
        current_progress = 0
        migration_completed = False
    else:
        progress_per_entry = 0
        current_progress = 100
        migration_completed = True

    if len(timestamps) <= total_entries and migration_completed == False:
        current_progress = (len(timestamps) + 1) * progress_per_entry
        if round(current_progress) >= 100:
            migration_completed = True
            timestamps = []
            length = 0
            current_progress = 0
            t_End = 0
            cnt = 0
            total_entries = 1000
        return round(current_progress)


# @app.before_first_request
# def validation_async(server, user, password, account, warehouse, role, database, schema):
#     t = threading.Thread(target=validation(),
#                          args=(server, user, password, account, warehouse, role, database, schema))
#     t.start()
