from getpass import getpass
from collections import defaultdict
import pandas as pd
import snowflake.connector as sf
from snowflake.connector.pandas_tools import write_pandas
import pyodbc as pyo
import sys
import os
import subprocess
import boto3
from botocore.exceptions import NoCredentialsError
import datetime
import re
import sys
import json
import threading
from flask import Flask, render_template, request, redirect, url_for, session

app = Flask(__name__)
timestamps = []
length = 0
current_progress = 0
t_End = 0

global_pass_table = 0
global_failed_table = 0

call_count = 0
call_count1 = 0

global_table_dataframe = pd.DataFrame()


def tanalyzer(df_dbcx):
    global current_progress
    global length
    global t_End
    global global_pass_table
    global global_failed_table
    global global_table_dataframe

    length = len(df_dbcx)

    df_db = df_dbcx
    pass_table = 0
    failed_table = 0



    with open(
            r"config.json",
            "r") as jsonfile:
        config = json.load(jsonfile)

    server = config['poc']['sql_server_credentials']['server']
    user = config['poc']['snowflake_credentials']['user']
    password = config['poc']['snowflake_credentials']['password']
    account = config['poc']['snowflake_credentials']['account']
    warehouse = config['poc']['snowflake_credentials']['warehouse']
    role = config['poc']['snowflake_credentials']['role']
    source_username = config['poc']['sql_server_credentials']['username']
    source_password = config['poc']['sql_server_credentials']['password']
    connection_string = "Driver={SQL Server};Server=" + server + ";UID=" + source_username + ";PWD=" + source_password + ";"
    cnn = pyo.connect(connection_string)

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

    def repl(matchobj):
        if matchobj.group(1) is not None:
            return str(matchobj.group(1))
        elif matchobj.group(2) is not None:
            return str(matchobj.group(2))

    def remSp(x):
        regex = r"(?:\(\s*\((.*)\)\)|\((.*)\))"
        return re.sub(regex, repl, str(x), 0, re.MULTILINE | re.IGNORECASE)

    def Default_valueMap(x):
        DefulatValue_Dict = {
            "newid\(\)": "uuid_string()",
            "getdate\(\)": "current_date()",
            "None": ""
        }
        if x.strip() == "None":
            return None
        for key, value in DefulatValue_Dict.items():
            x = re.sub(key, value, str(x), 0, re.MULTILINE | re.IGNORECASE)
        return x

    cur = cnn.cursor()

    print('Analysis STARTED')
    print("###########TIME STAMP ADDED HERE###########")
    t_start = datetime.datetime.now()
    timestamps.append(t_start)
    print('t_start')
    # yield 1
    print(t_start)
    print(current_progress)

    df_analyze = pd.DataFrame(
        columns=['Database', 'Schema', 'Table', 'DDL_Migration_Analysis', 'Remark-Unsupported Datatypes',
                 'Remark-Unsupported Default Values'])
    # print(df_analyze)

    count = 1
    for i, rows in df_db.iterrows():
        print("###########TIME STAMP ADDED HERE###########")
        t_1 = datetime.datetime.now()
        timestamps.append(t_1)
        print('t_1')
        # yield 1
        print(t_1)
        print(count)
        db_sql = "USE {};".format(rows[0])
        cur.execute(db_sql)
        sql_stm = "SELECT COLUMN_NAME, DATA_TYPE,isnull(isnull(CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION),DATETIME_PRECISION) length, IS_NULLABLE, COLUMN_DEFAULT,NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS where table_schema = '{}' AND TABLE_NAME = '{}';".format(
            rows[1], rows[2])
        print(sql_stm)
        sql_query = pd.read_sql_query(sql_stm,
                                      cnn)  # here, the 'cnn' is the variable that contains your database connection information from step 2
        df = pd.DataFrame(sql_query)
        # print(df)
        datatypes_Standared = ['BIGINT', 'BIT', 'DECIMAL', 'INT', 'MONEY', 'NUMERIC', 'SMALLINT', 'SMALLMONEY',
                               'TINYINT',
                               'FLOAT', 'REAL', 'DATE', 'DATETIME2', 'DATETIME', 'DATETIMEOFFSET', 'SMALLDATETIME',
                               'TIME',
                               'CHAR', 'TEXT', 'VARCHAR', 'NCHAR', 'NTEXT', 'NVARCHAR', 'UNIQUEIDENTIFIER', 'XML',
                               'HIERARCHYID', 'BINARY', 'VARBINARY', 'GEOGRAPHY']
        Defaultvalue_Standared = ['newid', 'getdate', None]

        print("###########TIME STAMP ADDED HERE###########")
        t_2 = datetime.datetime.now()
        timestamps.append(t_2)
        print('t_2')
        # yield 1
        print(t_2)

        list_datatype = list(df["DATA_TYPE"])
        list_datatype = [x.upper() for x in list_datatype]
        # print(list_datatype)

        list_Defaultvalue = list(df["COLUMN_DEFAULT"])
        Defaultvalue = df["COLUMN_DEFAULT"]

        # print(list_Defaultvalue)
        a = 0

        print("###########TIME STAMP ADDED HERE###########")
        t_3 = datetime.datetime.now()
        timestamps.append(t_3)
        print('t_3')
        # yield 1
        print(t_3)
        for i in list_Defaultvalue:
            print("###########TIME STAMP ADDED HERE###########")
            t_4 = datetime.datetime.now()
            timestamps.append(t_4)
            print('t_4')
            # yield 1
            print(t_4)
            if i is None:
                pass
            else:
                # print(i)
                # print(type(i))
                list_Defaultvalue[a] = list_Defaultvalue[a].replace("(", "").replace(")", "")
                list_Defaultvalue[a] = list_Defaultvalue[a].replace(")", "")
                # print(list_Defaultvalue[a])
            a = a + 1
            # print(list_Defaultvalue)
        c = 0

        # print(list_Defaultvalue)
        for c in range(0, len(list_Defaultvalue)):
            print("###########TIME STAMP ADDED HERE###########")
            t_5 = datetime.datetime.now()
            timestamps.append(t_5)
            print('t_5')
            # yield 1
            print(t_5)
            if list_Defaultvalue[c] is None:
                pass
            else:
                try:
                    list_Defaultvalue[c] = float(list_Defaultvalue[c])
                    # print("int")
                    # print(list_Defaultvalue[c])
                except:
                    list_Defaultvalue[c] = list_Defaultvalue[c]
                    # print("string")
                    # print(list_Defaultvalue[c])
            c = c + 1

        # print(list_Defaultvalue)
        # for i in list_Defaultvalue:
        # print(type(i))

        b = 0
        print("###########TIME STAMP ADDED HERE###########")
        t_6 = datetime.datetime.now()
        timestamps.append(t_6)
        print('t_6')
        # yield 1
        print(t_6)
        list_Defaultvalue_new = []
        for b in range(0, len(list_Defaultvalue)):
            print("###########TIME STAMP ADDED HERE###########")
            t_7 = datetime.datetime.now()
            timestamps.append(t_7)
            print('t_7')
            # yield 1
            print(t_7)
            if list_Defaultvalue[b] is None:
                pass
            else:
                if isinstance(list_Defaultvalue[b], float) or isinstance(list_Defaultvalue[b], int):
                    # print('if')
                    pass
                else:
                    # print('else')
                    # print(list_Defaultvalue[b])
                    list_Defaultvalue_new.append(list_Defaultvalue[b])
                    # print("Sucess")
            # print(b)
            # print("-------------")
            b = b + 1
        # print(list_Defaultvalue_new)

        lst_unsupported_datatype = []
        lst_unsupported_defultvalue = []
        for i in list_Defaultvalue_new:
            if i in Defaultvalue_Standared:
                pass
            else:
                lst_unsupported_defultvalue.append(i)
        print("###########TIME STAMP ADDED HERE###########")
        t_8 = datetime.datetime.now()
        timestamps.append(t_8)
        print('t_8')
        # yield 1
        print(t_8)
        for i in list_datatype:
            if i in datatypes_Standared:
                pass
            else:
                lst_unsupported_datatype.append(i)

        print(lst_unsupported_datatype)
        print(lst_unsupported_defultvalue)

        unsupported_datatype = ', '.join([str(elem) for elem in lst_unsupported_datatype])

        unsupported_defultvalue = ', '.join([str(elem) for elem in lst_unsupported_defultvalue])

        print('----------------------------------------------------------------')
        print(unsupported_defultvalue)
        print('----------------------------------------------------------------')
        print(unsupported_datatype)
        print('----------------------------------------------------------------')
        check1 = all(item in datatypes_Standared for item in list_datatype)
        check2 = all(item in Defaultvalue_Standared for item in list_Defaultvalue_new)
        # print("Check1:",check1)
        # print("Check2:",check2)
        print("###########TIME STAMP ADDED HERE###########")
        t_9 = datetime.datetime.now()
        timestamps.append(t_9)
        print('t_9')
        # yield 1
        print(t_9)
        if check1 and check2 is True:
            print("Sucsess")
            # df_analyze = df_analyze.append({'Database': rows[0], 'Schema': rows[1], 'Table': rows[2], 'DDL_Migration_Analysis': 'Success','Remark-Unsupported Datatypes': unsupported_datatype,'Remark-Unsupported Default Values': unsupported_defultvalue}, ignore_index=True)
            new_row = pd.DataFrame({
                'Database': [rows[0]],
                'Schema': [rows[1]],
                'Table': [rows[2]],
                'DDL_Migration_Analysis': ['Success'],
                'Remark-Unsupported Datatypes': [unsupported_datatype],
                'Remark-Unsupported Default Values': [unsupported_defultvalue]
            })

            df_analyze = pd.concat([df_analyze, new_row], ignore_index=True)
            pass_table += 1

        else:
            print("Fail")
            # df_analyze = df_analyze.append({'Database': rows[0], 'Schema': rows[1], 'Table': rows[2], 'DDL_Migration_Analysis': 'Fail','Remark-Unsupported Datatypes': unsupported_datatype,'Remark-Unsupported Default Values': unsupported_defultvalue}, ignore_index=True)

            new_row = pd.DataFrame({
                'Database': [rows[0]],
                'Schema': [rows[1]],
                'Table': [rows[2]],
                'DDL_Migration_Analysis': ['Fail'],
                'Remark-Unsupported Datatypes': [unsupported_datatype],
                'Remark-Unsupported Default Values': [unsupported_defultvalue]
            })

            df_analyze = pd.concat([df_analyze, new_row], ignore_index=True)
            failed_table += 1

        count = count + 1
        print("###############################################################################################")
        # print(timestamps)

    print(df_analyze)
    global_table_dataframe = df_analyze.copy()
    global_pass_table = pass_table
    global_failed_table = failed_table
    # df_analyze.to_csv(r'C:\Users\danis\Downloads\New_folder\New_folder\table_analyzer\Analyze_DDL.csv', index=False)
    print("DDL Analyzing Completed")
    t_End = datetime.datetime.now()
    timestamps.append(t_End)
    print('t_end')
    print(current_progress)
    # yield 1
    print(t_End)


def progress_tanalyzer():
    global length
    global timestamps
    global current_progress
    global t_End
    global total_entries
    total_entries = (length * 9) + 2

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


def start_time_function():
    # Access the global variables
    global call_count
    global global_pass_table
    global global_failed_table

    table_status = []

    table_status.append(global_pass_table)
    table_status.append(global_failed_table)

    # Increment the call_count on each call
    call_count += 1

    # If it's the second call, return the start_time_list and reset the count and the list
    if call_count == 2:
        call_count = 0
        global_pass_table = 0
        global_failed_table = 0
        table_status_last = table_status.copy()
        table_status.clear()
        return table_status_last
    else:
        return table_status

def table_names():
    global call_count1
    global global_table_dataframe

    call_count1 += 1

    if call_count1 == 1:
        table_name = global_table_dataframe.copy()
        global_table_dataframe = pd.DataFrame()
        return table_name
    else:
        return global_table_dataframe
# @app.before_first_request
# def analyze_async(server, user, password, account, warehouse, role, df_view):
#     t = threading.Thread(target=tanalyzer,
#                          args=df_dbcx)
#     t.start()
