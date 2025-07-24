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
import re
import string
import decimal
# import helper
import pandas as pd
import numpy as np
import datetime
import urllib.parse
import snowflake.connector
import asyncio
from flask import Flask
import threading

global_start_time_list = []
call_count = 0

def validate_cases_sql_server_snf(server,user,password,account,warehouse,role,database,schema,df_test, source_username, source_password):

    global global_start_time_list
    global_start_time_list.clear()

    def extract_test_cases(df_test):
        #df_test = pd.read_csv(file_path)
        tables_list = df_test[0].tolist()
        test_case_list = df_test[1].tolist()
        return tables_list, test_case_list

    def validate_cases(table):
        tables_list, test_case_list = extract_test_cases(df_test)
        test_cases_table = [test_case_list[i] for i in range(len(tables_list)) if tables_list[i] == table]
        print(tables_list)
        print(test_cases_table)
        try:
            cnn_sql = ('Driver={SQL Server};'
                       f'Server={server};'
                       f'UID={source_username};'
                       f'PWD={source_password};')
            cnn = pyo.connect(cnn_sql)
            sql_cur = cnn.cursor()
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
            snow_cur = conn.cursor()
            print('validation STARTED...')
        except Exception as e:
            print("Couldn't connect to snowflake database. Error raised is ---> " + str(e))
        try:
            snow_cur.execute('CREATE DATABASE IF NOT EXISTS GMIGRATE_REPORT_DB')
            snow_cur.execute('USE DATABASE GMIGRATE_REPORT_DB')
            snow_cur.execute('CREATE SCHEMA IF NOT EXISTS QA')
            snow_cur.execute(
                'CREATE TABLE IF NOT EXISTS GMIGRATE_REPORT_DB.QA.VALIDATION_TEST_RESULTS (SOURCE_TABLE_NAME STRING,TARGET_TABLE_NAME STRING, TEST_CASE STRING, SOURCE_TABLE_RESULT STRING,TARGET_TABLE_RESULT STRING, STATUS STRING, EXECUTION_START_TIME TIMESTAMP,EXECUTION_END_TIME TIMESTAMP)')



            for test_case in test_cases_table:
                try:
                    test_case=test_case.upper()
                    if '\r' in test_case:
                        test_case = test_case.replace('\r','')
                    if test_case == 'ROW_COUNT':
                        s_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        global_start_time_list.append(s_time)
                        src_count = sql_cur.execute(f'SELECT COUNT(*) FROM {database}.{schema}.{table}')
                        src_count = src_count.fetchone()[0]
                        tar_count = snow_cur.execute(f'SELECT COUNT(*) FROM {database}.{schema}.{table}')
                        tar_count = tar_count.fetchone()[0]
                        if src_count == tar_count:
                            status = 'PASS'
                        else:
                            status = 'FAIL'
                        e_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                        snow_cur.execute(
                            f"INSERT INTO gmigrate_report_db.QA.VALIDATION_TEST_RESULTS VALUES ('{database}.{schema}.{table}','{database.upper()}.{schema.upper()}.{table.upper()}','{test_case}','{src_count}','{tar_count}','{status}','{s_time}' ,'{e_time}')")
                        print(test_case)
                        print(src_count, tar_count)


                    else:
                        s_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        global_start_time_list.append(s_time)
                        src_query = test_case
                        sql_cur.execute('USE '+database)
                        src_query = test_case.replace(' FROM ', f' FROM {schema}.')
                        src_query= src_query.replace('JOIN ',f'JOIN {schema}.')
                        # sql_cur.execute('USE SCHEMA '+schema)
                        sql_cur.execute(src_query)
                        #src_count = sql_cur.fetchone()[0]
                        src_res=sql_cur.fetchall()
                        src_res=[[round(float(col),2) if isinstance(col,decimal.Decimal) else col for col in row] for row in src_res]
                        #src_count = int(src_count * 10**6)/10**6
                        #print(src_count)
                        tar_query = test_case
                        snow_cur.execute('USE DATABASE '+database)
                        snow_cur.execute('USE SCHEMA '+schema)
                        print('########################################')
                        print(tar_query)
                        print(src_query)
                        snow_cur.execute(tar_query)
                        #tar_count = snow_cur.fetchone()[0]
                        tar_res=snow_cur.fetchall()
                        tar_res = [[round(float(col),2) if isinstance(col, decimal.Decimal) else col for col in row] for row in
                                   tar_res]
                        #tar_count = int(tar_count * 10**6)/10**6
                        #print(tar_count)
                        """if src_count == tar_count:
                            status = 'PASS'
                        else:
                            status = 'FAIL'"""
                        print('SOURCE RESULT')
                        print(src_res)
                        print(tar_res)
                        if src_res==tar_res:
                            status= 'PASS'
                        else:
                            status= 'FAIL'
                        e_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        test_case1 = test_case.replace("'", "''")
                        snow_cur.execute(f"INSERT INTO gmigrate_report_db.QA.VALIDATION_TEST_RESULTS VALUES ('{database}.{schema}.{table}','{database.upper()}.{schema.upper()}.{table.upper()}','{test_case1}','{src_res}','{tar_res}','{status}','{s_time}','{e_time}')")
                        print(test_case)
                        #print(src_count, tar_count)

                except Exception as e:
                    print(e)
        except Exception as e:
            print(e)


    tables_list, test_case_list = extract_test_cases(df_test)
    new_tables_list = list(set(tables_list))
    for table in new_tables_list:
        validate_cases(table)


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
