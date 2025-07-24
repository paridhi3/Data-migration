from getpass import getpass
from collections import defaultdict
import pandas as pd
import snowflake.connector as sf
from snowflake.connector.pandas_tools import write_pandas
import pyodbc as pyo
import sys
import re
import os
import subprocess
import boto3
from botocore.exceptions import NoCredentialsError
import datetime
import time
import contextlib
import threading
from flask import Flask, render_template, request, redirect, url_for, session

original_stdout = sys.stdout
sys.stdout = open(r'C:\migration_files\log.txt', 'w')
test1=0

app = Flask(__name__)
timestamps = []
length = 0
current_progress = 0
t_End = 0
# Global list to store start_time_str
global_start_time_list = []

call_count = 0

def migration_function(server, user, password, account, warehouse, role, df_db,source_user, source_password):
    # --------------------------------------------Establish Connection---------------------------------------------------------
    print(df_db)
    global current_progress
    global length
    global t_End
    global global_start_time_list
    global_start_time_list.clear()
    length = len(df_db)
    # connection_string = "Driver={SQL Server};Server=" + server + ";Trusted_Connection=yes;"
    connection_string = "Driver={SQL Server};Server=" + server + ";UID=" + source_user + ";PWD=" + source_password + ";"

    # connection_string = "Driver={SQL Server};Server=" + server + ";Database=demo_db;Uid=sqladmin;Pwd=test@123;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    cnn = pyo.connect(connection_string,timeout=30)

    sctx = sf.connect(user=user,
                      password=password,
                      account=account,
                      warehouse=warehouse,
                      role=role)

    def execute_query(query):
        cs = sctx.cursor()
        cs.execute(query)
        cs.close()

    def fetch_pandas_old(sctx, sql):
        cs = sctx.cursor()
        cs.execute(sql)
        rows = 0
        while True:
            dat = cs.fetchmany(50000)
            if not dat:
                break
            df1 = pd.DataFrame(dat)
            rows += df1.shape[0]
            return (df1)
        return 1

    cur = cnn.cursor()
    # ------------------------------------------------INTERNAL STAGE CODE------------------------------------------------------
    print('Migration STARTED')
    t_start = datetime.datetime.now()
    timestamps.append(t_start)
    print('t_start')
    # yield 1
    print(t_start)
    print(current_progress)
    # ---------------------------------------READ THE ROWS AS PER MIGRATION TABLE-----------------------------------------
    for i, rows in df_db.iterrows():

        try:
            # -------------------------------------CREATION OF DB IN SNOWFLAKE-------------------------------------------
            print("DATABASE CREATION")
            database = "create database if not exists {}".format(rows[0])
            print(database)
            execute_query(database)
            db1 = "use {}".format(rows[0])
            execute_query(db1)
            print(db1)

            # -------------------------------------------------CREATE AUDIT TABLE------------------------------------------
            database_GmigrateDB ="create database if not exists GMigrate_Report_DB"
            print(database_GmigrateDB)
            execute_query(database_GmigrateDB)
            schema_Audit = "create schema if not exists GMigrate_Report_DB.Audit;"
            print(schema_Audit)
            # return(schema_Audit)
            print("\n")
            execute_query(schema_Audit)
            #sql_audit = "create table if not exists GMigrate_Report_DB.Audit.AUDIT_TABLE(Database_Name varchar,  SCHEMA_NAME varchar, TABLE_NAME varchar, STATUS varchar, START_TIME timestamp DEFAULT current_timestamp(), END_TIME timestamp  DEFAULT current_timestamp(), CREATED_BY varchar DEFAULT current_user(), UPDATED_BY varchar DEFAULT current_user() ); "
            # sql_audit = "create table if not exists GMigrate_Report_DB.AUDIT_TABLE(Database_Name varchar,  SCHEMA_NAME varchar, TABLE_NAME varchar, STATUS varchar, START_TIME timestamp DEFAULT current_timestamp(), END_TIME timestamp  DEFAULT current_timestamp(), CREATED_BY varchar DEFAULT current_user(), UPDATED_BY varchar DEFAULT current_user() ); ".format(rows[0])
            sql_audit = "create table if not exists GMigrate_Report_DB.Audit.AUDIT_TABLE(Source_DBMS varchar,Target_DBMS varchar,Database_Name varchar,  SCHEMA_NAME varchar, TABLE_NAME varchar, STATUS varchar, START_TIME timestamp DEFAULT current_timestamp(), END_TIME timestamp  DEFAULT current_timestamp(), CREATED_BY varchar DEFAULT current_user(), UPDATED_BY varchar DEFAULT current_user() ); "

            print(sql_audit)
            # return(sql_audit)
            print("\n")
            execute_query(sql_audit)

            # Get the current timestamp as the start time
            start_time = datetime.datetime.now()

            # Convert the start time to a string in the desired format (if needed)
            start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')

            # Append the string to the global list
            global_start_time_list.append(start_time_str)

            sql_insert_audit = '''INSERT INTO GMigrate_Report_DB.Audit.Audit_table VALUES('SQL_SERVER','SNOWFLAKE','{}', '{}', '{}', 'IN PROGRESS', TO_TIMESTAMP('{}', 'YYYY-MM-DD HH:MI:SS'), NULL, CURRENT_USER(), CURRENT_USER());'''.format(
                 rows[0], rows[1], rows[2],start_time_str)
            print(sql_insert_audit)
            # return(sql_insert_audit)
            print("\n")
            execute_query(sql_insert_audit)

            print("\nExecution COMPLETED")
            print(
                "===================================================================================================================")

            table_df = pd.read_sql_query('select * from {}.{}.{}'.format(rows[0], rows[1], rows[2]), cnn)
            print('read data from SQL SERVER')
            t_read = datetime.datetime.now()
            timestamps.append(t_read)
            print(current_progress)
            print(t_read)
            print("Time b/w Code Start & Schema creation:", t_read - t_start)
            table_name = rows[2]
            print(table_name)
            info_sql1 = "SELECT COLUMN_NAME, DATA_TYPE,isnull(isnull(CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION),DATETIME_PRECISION) length, IS_NULLABLE, COLUMN_DEFAULT,NUMERIC_SCALE FROM [{}].INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '{}'".format(
                rows[0], table_name)
            info_sql1_query = pd.read_sql_query(info_sql1,
                                                cnn)  # here, the 'cnn' is the variable that contains your database connection information from step 2
            info_df = pd.DataFrame(info_sql1_query)
            xml_list_name = info_df.loc[info_df['DATA_TYPE'] == 'xml', 'COLUMN_NAME'].tolist()
            xml_list_index = info_df.loc[info_df['DATA_TYPE'] == 'xml', 'COLUMN_NAME'].index.values.tolist()
            if xml_list_name:
                for col in xml_list_name:
                    y = 0
                    m = xml_list_index[y]
                    k = len(table_df[col])
                    for i in range(k):
                        if not pd.isna(table_df.iloc[i, m]) and table_df.at[i, col] != None:
                            table_df.at[i, col] = table_df[col][i].replace("&#x0D;", " ")
                    y = y + 1

                    # -------------------------------------CREATION OF SCH IN SNOWFLAKE-------------------------------------------

            print('CREATION OF SCH IN SNOWFLAKE STARTED')
            t1 = datetime.datetime.now()
            timestamps.append(t1)
            print(t1)
            print("Time b/w Code Start & Schema creation:", t1 - t_start)
            print(
                "===================================================================================================================")
            print("\nSCHEMA CREATION")
            db1 = "use {}".format(rows[0])
            execute_query(db1)
            print(db1)
            schema1 = "create schema if not exists {}".format(rows[1])
            # print("\n")
            print(schema1)
            print("\n")
            execute_query(schema1)
            sc1 = "use schema {}".format(rows[1])
            execute_query(sc1)
            print('CREATION OF DB, SCH IN SNOWFLAKE ENDED')
            t2 = datetime.datetime.now()
            timestamps.append(t2)
            print(t2)
            print("Time to Create DB:", t2 - t1)
            # ----------------------------------------DDL MAPPING FOR TABLE---------------------------------------------------
            print("DDL MAPPING STARTED\n")
            sql_stm = "SELECT COLUMN_NAME, DATA_TYPE,isnull(isnull(CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION),DATETIME_PRECISION) length, IS_NULLABLE, COLUMN_DEFAULT,NUMERIC_SCALE FROM [{}].INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '{}' AND TABLE_SCHEMA='{}'".format(
                rows[0], table_name,rows[1])
            sql_query = pd.read_sql_query(sql_stm,
                                          cnn)  # here, the 'cnn' is the variable that contains your database connection information from step 2
            df = pd.DataFrame(sql_query)
            print(df)
            # print(df.dtypes)
            list1 = df.loc[df['DATA_TYPE'] == 'bit', 'COLUMN_NAME'].tolist()
            print(list1)
            if list1:
                for col in list1:
                    print(col)
                    table_df[col].replace([True, False], [1, 0], inplace=True)
            # print(table_df)

            df['IS_NULLABLE'].replace(['YES', 'NO'], ['NULL', 'NOT NULL'], inplace=True)
            datatypes = ['BIGINT', 'BIT', 'DECIMAL', 'INT', 'MONEY', 'NUMERIC', 'SMALLINT', 'SMALLMONEY',
                         'TINYINT',
                         'FLOAT', 'REAL', 'DATE', 'DATETIME2', 'DATETIME', 'DATETIMEOFFSET', 'SMALLDATETIME',
                         'TIME',
                         'CHAR', 'TEXT', 'VARCHAR', 'NCHAR', 'NTEXT', 'NVARCHAR', 'UNIQUEIDENTIFIER', 'XML',
                         'HIERARCHYID', 'BINARY', 'VARBINARY', 'GEOGRAPHY']
            datatypes = [x.lower() for x in datatypes]
            dtmap = ['NUMBER', 'NUMERIC(1)', 'NUMERIC', 'NUMBER', 'FLOAT', 'NUMERIC', 'NUMBER', 'FLOAT',
                     'NUMBER',
                     'FLOAT', 'NUMERIC', 'DATE', 'TIMESTAMP_NTZ', 'DATETIME', 'TIMESTAMP_LTZ', 'DATETIME',
                     'TIME',
                     'VARCHAR(1)', 'VARCHAR', 'VARCHAR', 'VARCHAR', 'VARCHAR', 'VARCHAR', 'VARCHAR', 'VARIANT',
                     'VARCHAR', 'BINARY', 'VARCHAR', 'VARCHAR']
            df['DATA_TYPE'].replace(datatypes, dtmap, inplace=True)
            s = "create or replace table {}.{}.{} (".format(rows[0], rows[1], table_name)
            stm = ""
            r = len(df.index) - 1
            for i, row in df.iterrows():
                if r != 0:
                    if row[2] > 0:
                        print(row[1])
                        if not pd.isna(df.iloc[i, 5]):
                            if row[1] == 'FLOAT':
                                stm = stm + '''"{}" {}({}) {} , '''.format(row[0].upper(), row[1], int(row[2]),
                                                                           row[3])
                            else:
                                stm = stm + '''"{}" {}({},{}) {} , '''.format(row[0].upper(), row[1],
                                                                              int(row[2]),
                                                                              int(row[5]), row[3])
                        else:
                            stm = stm + '''"{}" {}({}) {} , '''.format(row[0].upper(), row[1], int(row[2]),
                                                                       row[3])
                    else:
                        stm = stm + '''"{}" {} {} , '''.format(row[0].upper(), row[1], row[3])
                    r = r - 1
                else:
                    if row[2] > 0:
                        if not pd.isna(df.iloc[i, 5]):
                            if row[1] == 'FLOAT':
                                stm = stm + '''"{}" {}({}) {} );'''.format(row[0].upper(), row[1], int(row[2]),
                                                                           row[3])
                            else:
                                stm = stm + '''"{}" {}({},{}) {} );'''.format(row[0].upper(), row[1],
                                                                              int(row[2]),
                                                                              int(row[5]), row[3])
                        else:
                            stm = stm + '''"{}" {}({}) {} );'''.format(row[0].upper(), row[1], int(row[2]),
                                                                       row[3])
                    else:
                        stm = stm + '''"{}" {} {} );'''.format(row[0].upper(), row[1], row[3])
            sql = s + stm
            print("DDL MAPPING COMPLETED:\n" + sql)
            print()
            execute_query(sql)
            sql_ss = cur.execute('USE {};'.format(rows[0]))
            sql_pk = "select C.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS T JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C ON C.CONSTRAINT_NAME=T.CONSTRAINT_NAME WHERE C.TABLE_NAME='{}' and T.CONSTRAINT_TYPE='PRIMARY KEY'".format(
                rows[2])
            # df_pk = pd.read_sql_query(sql_pk, cnn)
            # for x,pks in df_pk.iterrows():
            # sql_sf_pk = 'alter table {} add primary key({})'.format(rows[2],pks[x])
            # execute_query(sql_sf_pk)

            print("\nWORKING ON SNOWFLAKE...")
            try:
                try:
                    sql1 = 'alter warehouse {} resume'.format(WAREHOUSE)
                    execute_query(sql1)
                except:
                    pass
                print("\nWRITING TO SNOWFLAKE...")

                print('DDL MAPPING FOR TABLE ENDED')
                t3 = datetime.datetime.now()
                timestamps.append(t3)
                print('t_3')
                # yield 1
                print(t3)
                print("Time for DDL: ", t3 - t2)
                # ------------------------------------FILE FORMAT CREATION------------------------------------------------------
                print("CREATING FILE FORMAT")
                file_format = 'csv1'

                File = '''CREATE OR REPLACE FILE FORMAT  {}.{}.{}
                                    COMPRESSION = "AUTO" FIELD_DELIMITER = "," RECORD_DELIMITER = "\\n" SKIP_HEADER = 1
                                    FIELD_OPTIONALLY_ENCLOSED_BY = "\\042" TRIM_SPACE = FALSE ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
                                    ESCAPE = "NONE" ESCAPE_UNENCLOSED_FIELD = "\\134" DATE_FORMAT = "AUTO" TIMESTAMP_FORMAT = "AUTO" NULL_IF = ('\\NULL', 'NUL','') ENCODING='UTF8';'''.format(
                    rows[0], rows[1], file_format)
                # File = 'CREATE OR REPLACE FILE FORMAT {}.{}.{} TYPE=CSV RECORD_DELIMITER="\\n" FIELD_DELIMITER="," ESCAPE_UNENCLOSED_FIELD=NONE SKIP_HEADER = 1'.format(rows[0],rows[1],file_format)
                print(File)
                execute_query(File)
                print("\nFILE FORMAT CREATED: " + File)

                print('FILE FORMAT CREATION ENDED')
                t4 = datetime.datetime.now()
                timestamps.append(t4)
                print('t_4')
                # yield 1
                print(t4)
                print("Time for File format: ", t4 - t3)
                # -----------------------------------SAVE TABLES AS CSV IN LOCAL STORAGE------------------------------------------
                print('*********************************')
                print(table_df)


                # for index1, rows1 in table_df.iterrows():
                #     for column1, value in rows1.items():
                #         if isinstance(value, str):
                #             table_df.at[index1, column1] = re.sub(r'\\+','',value)
                        # if value == '\\':
                        #     table_df[column1][index1] = ''
                table_df.to_csv(
                    r'C:\migration_files\{}.csv'.format(
                        rows[2]), index=False)
                FILE_PATH = r"file:///C:\migration_files\{}.csv".format(
                    rows[2])
                print('SAVE TABLES AS CSV IN LOCAL STORAGE ENDED')
                t5 = datetime.datetime.now()
                timestamps.append(t5)
                print('t_5')
                # yield 1
                print(t5)
                print("Time to save csv in local: ", t5 - t4)
                # ------------------------------------INTERNAL STAGE CREATION--------------------------------------------------
                internal_stage = 'create or replace STAGE INT_STAGE file_format = csv1'
                execute_query(internal_stage)
                print("\nINTERNAL STAGE CREATED")

                print('INTERNAL STAGE CREATION ENDED')
                t6 = datetime.datetime.now()
                timestamps.append(t6)
                print('t_6')
                # yield 1
                print(t6)
                print("Time to create internal stage: ", t6 - t5)
                # ---------------------------------UPLOAD THE FILE INTO STAGE--------------------------------------------------
                upload = 'put {} @INT_STAGE'.format(FILE_PATH)
                execute_query(upload)
                print("FILE UPLOADING TO STAGE...")

                print('UPLOAD THE FILE INTO STAGE ENDED')
                t7 = datetime.datetime.now()
                timestamps.append(t7)
                print('t_7')
                # yield 1
                print(t7)
                print("Time to upload file to stage: ", t7 - t6)
                # ---------------------------------COPY DATA FROM STAGE TO TABLE-----------------------------------------------
                sql2 = "copy into {}.{}.{} (".format(rows[0], rows[1], table_name)
                sql3 = ""
                r1 = len(df.index) - 1
                for i, row in df.iterrows():
                    if r1 != 0:
                        sql3 = sql3 + ''' "{}" ,'''.format(row[0].upper())
                        r1 = r1 - 1
                    else:
                        sql3 = sql3 + ''' "{}" )'''.format(row[0].upper())

                sql3 = sql3 + " from (select  "

                sql4 = ""
                r2 = len(df.index) - 1
                for i, row in df.iterrows():
                    if r2 != 0:
                        if row[1] != 'VARIANT':
                            sql4 = sql4 + "t.${}, ".format(i + 1)
                            r2 = r2 - 1
                        else:
                            sql4 = sql4 + "parse_XML(t.${},true), ".format(i + 1)
                            r2 = r2 - 1
                    else:
                        if row[1] != 'VARIANT':
                            sql4 = sql4 + "t.${} ".format(i + 1)
                        else:
                            sql4 = sql4 + "parse_XML(t.${},true) ".format(i + 1)

                sql5 = " from @INT_STAGE t)  file_format = csv1  on_error = CONTINUE ;"
                sql1 = sql2 + sql3 + sql4 + sql5
                print(sql1)
                # execute_query(sql1)
                print("\n")
                columns = ['file', 'status', 'rows_parsed', 'rows_loaded', 'error_limit', 'errors_seen',
                           'first_error',
                           'first_error_line', 'first_error_character', 'first_error_column_name']
                tab = fetch_pandas_old(sctx, sql1)
                # tab.columns = columns
                status = list(tab[1])
                print("FILE LOAD STATUS: " + status[0])
                print("\n")
                listx = tab.values.tolist()

                print('COPY DATA FROM STAGE TO TABLE ENDED')
                t8 = datetime.datetime.now()
                timestamps.append(t8)
                print('t_8')
                # yield 1
                print(t8)
                print("Time for copy data from stage to table: ", t8 - t7)
                # -----------------------------------------------------------------------------------------------------------------

                # -----------------------------------------CREATION OF ROW_VALIDATION TABLE--------------------------------------
                query_id = "select last_query_id();"
                x = fetch_pandas_old(sctx, query_id)
                x = list(x[0])
                # print(x)

                sql = " create table if not exists GMigrate_Report_DB.Audit.ROW_VALIDATION as SELECT 'SQL_SERVER' as Source_DBMS,'SNOWFLAKE' as Target_DBMS, * from table(result_scan(last_query_id(-2))) where 1=0"
                # print(sql)
                #execute_query(sql);
                print("ROW VALIDATION TABLE CREATED!")
                for i in range(len(listx)):
                    # sql_ins = '''insert into LOG_ERROR_ROW values('{}','{}',{},{},{},{},'{}','{}','{}','{}');'''.format(listx[i][0],listx[i][1],listx[i][2],listx[i][3],listx[4],listx[i][5],str(listx[i][6]).replace("'",""),listx[i][7],listx[i][8],listx[i][9])
                    sql_ins = '''insert into GMigrate_Report_DB.Audit.ROW_VALIDATION select  'SQL_SERVER' as Source_DBMS,'SNOWFLAKE' as Target_DBMS, * from table(result_scan(last_query_id(-3)));'''
                    print(sql_ins)
                    #execute_query(sql_ins);

                print('CREATION OF ROW_VALIDATION TABLE ENDED')
                t9 = datetime.datetime.now()
                timestamps.append(t9)
                print('t_9')
                # yield 1
                print(t9)
                print("Time for Row validation table: ", t9 - t8)
                # ------------------------------------------------------------------------------------------------------------------

                # ------------------------------------------CREATION OF ERROR_TABLE----------------------------------------------
                error_table = "create table if not exists {}.{}.ERROR_TABLE(ERROR varchar,FILE varchar,LINE integer,CHARACTER integer,BYTE_OFFSET integer,CATEGORY varchar,CODE integer,SQL_STATE integer,COLUMN_NAME varchar,\
                    ROW_NUMBER integer,ROW_START_LINE integer, REJECTED_RECORD varchar);".format(rows[0],rows[1])
                #execute_query(error_table)
                print("ERROR TABLE CREATED!")
                err_sql = "insert into {}.{}.ERROR_TABLE select * from table(validate({},\
                                        job_id=>'{}'));".format( rows[0],rows[1],table_name, x[0])
                print(err_sql)
                #execute_query(err_sql)

                # ------------------------------------------------------------------------------------------------------------------

                if (status[0] != 'LOAD_FAILED'):
                    sql2 = 'SELECT * FROM {}.{}.{}'.format(rows[0], rows[1], table_name)
                    df_sf = fetch_pandas_old(sctx, sql2)

                print('CREATION OF ERROR_TABLE ENDED')
                t10 = datetime.datetime.now()
                timestamps.append(t10)
                print('t_10')
                # yield 1
                print(t10)
                print("Time for error log: ", t10 - t9)
                # -----------------------------------------CREATION OF HISTORY_TABLE FOR CDC-------------------------------------

                # sql3 = 'create or replace table {}.{}.{}_history clone {}.{}.{}'.format(rows[0], rows[1],
                #                                                                         table_name,
                #                                                                         rows[0], rows[1],
                #                                                                         table_name)
                # execute_query(sql3)
                #
                # sql4 = "alter table {}.{}.{}_history add START_TIME TIMESTAMP_NTZ, END_TIME TIMESTAMP_NTZ, ACTIVE varchar default 'Y';".format(
                #     rows[0], rows[1], table_name)
                # execute_query(sql4)
                # sql4 = "update {}.{}.{}_history set START_TIME = current_timestamp()::timestamp_ntz , END_TIME = '9999-12-31 00:00:00 -0700'::timestamp_ntz;".format(
                #     rows[0], rows[1], table_name)
                # execute_query(sql4)
                #
                # print('CREATION OF HISTORY_TABLE FOR CDC ENDED')
                t11 = datetime.datetime.now()
                timestamps.append(t11)
                print('t_11')
                # yield 1
                print(t11)
                print(current_progress)
                # print("Time for CDC table: ", t11 - t10)
                # -------------------------------------------------CREATE AUDIT TABLE------------------------------------------
                # Get the current timestamp as the start time
                end_timek = datetime.datetime.now()

                # Convert the start time to a string in the desired format (if needed)
                end_time_str = end_timek.strftime('%Y-%m-%d %H:%M:%S')

                print(status[0])
                sql_update_audit = '''UPDATE  GMigrate_Report_DB.Audit.Audit_table SET STATUS = '{}', END_TIME = TO_TIMESTAMP('{}', 'YYYY-MM-DD HH:MI:SS') where audit.audit_table.start_time = TO_TIMESTAMP('{}', 'YYYY-MM-DD HH:MI:SS');'''.format(
                    status[0],end_time_str,start_time_str)
                print(sql_update_audit)
                print("\n")
                execute_query(sql_update_audit)

                print("\nExecution COMPLETED")
                print(
                    "===================================================================================================================")

            except sf.errors.ProgrammingError as e:  # Error for Snowflake
                print(e)  # default error message
                print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))  # error message

        except Exception as e:
            error = str(e)
            print(error)
            error = error.replace("'","")
            # Get the current timestamp as the start time
            end_timek = datetime.datetime.now()

            # Convert the start time to a string in the desired format (if needed)
            end_time_str = end_timek.strftime('%Y-%m-%d %H:%M:%S')
            sql_update_audit = '''UPDATE  GMigrate_Report_DB.Audit.Audit_table SET STATUS = 'Table not created due to {}', END_TIME = TO_TIMESTAMP('{}', 'YYYY-MM-DD HH:MI:SS') where audit.audit_table.start_time = TO_TIMESTAMP('{}', 'YYYY-MM-DD HH:MI:SS');'''.format(error, end_time_str,start_time_str)
            print(sql_update_audit)
            print("\n")
            execute_query(sql_update_audit)
            print(e)

    # ---------------------------------------------------CLOSING THE CONNECTIONS------------------------------------------------
    print("MIGRATION PROCESS COMPLETED!!!")
    t_End = datetime.datetime.now()
    timestamps.append(t_End)
    print('t_end')
    print(current_progress)
    # yield 1
    print(t_End)
    print("Time for Migration Code: ", t_End - t_start)
    print("CLOSING THE CONNECTIONS...")
    print(len(timestamps))
    # cnn.close()
    # sctx.close()


sys.stdout.close()
sys.stdout = original_stdout

def progress():
    global length
    global timestamps
    global current_progress
    global t_End
    total_entries = (length * 12) + 2

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



    # while len(timestamps) <= total_entries:
    #     if current_progress <= 100:
    #         current_progress = (len(timestamps) + 1) * progress_per_entry
    #         yield f"data: {current_progress:.2f}\n\n"
    #
    # if t_End in timestamps and current_progress < 100:
    #     current_progress = 100
    #     yield f"data: {current_progress:.2f}\n\n"
    #     yield "data: done\n\n"
    # elif current_progress >= 100:
    #     yield "data: done\n\n"


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
