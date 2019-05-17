    import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from sql_queries import *

def event_data_grab_file_list(folder_nm):
    """This function is to pull all the files available
    in the 'folder_nm' mentioned in 'sql_queries.py'
    """
    filepath = os.getcwd() + folder_nm
    file_path_list = []

    for root, dirs, files in os.walk(filepath):
        data = glob.glob(os.path.join(root,'*'))
        file_path_list.append(data)
    file_path_list_final = file_path_list[0]
    return(file_path_list_final)

def event_data_filelist_data_to_df(file_list):
    """This function is to read content of csv files
    pulled from previous function and store consolidated
    content into dataframe 'result'
    """
    result = pd.DataFrame()

    for f in file_list:
        data = pd.read_csv(f)
        result = result.append(data)

    return(result)

def event_data_df_to_merged_csv(result_df, csv_file_nm):
    """This function is to create the csv file
    named 'csv_file_nm' mentioned in 'sql_queries.py'
    from the 'result' dataframe created in prev function
    """
    full_path=os.getcwd() + csv_file_nm
    result_df = result_df[result_df.artist.notnull()]
    result_df['userId']=result_df['userId'].astype(int)
    result_df.to_csv(full_path, index=False, columns = ['artist','firstName','gender','itemInSession',
                                                        'lastName','length','level','location','sessionId',
                                                        'song','userId'])
    return(full_path)

def event_data_nosql_setup():
    """Setup Apache Cassandra NoSQL cluster, session, keyspace
    """
    from cassandra.cluster import Cluster
    cluster = Cluster()

    # To establish connection and begin executing queries, need a session
    session = cluster.connect()

    # TO-DO: Create a Keyspace
    try:
        session.execute("""
        CREATE KEYSPACE IF NOT EXISTS udacity
        WITH REPLICATION =
        { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
        )

    except Exception as e:
        print(e)

    # TO-DO: Set KEYSPACE to the keyspace specified above
    try:
        session.set_keyspace('udacity')
    except Exception as e:
        print(e)

    return(session, cluster)

def ed_appHistData_query_results(session, full_path, query_lists, func1, func2):
    """Generic function to CREATE/INSERT/SELECT tables
    from the parameters passed
    """
    try:
        session.execute(query_lists[1])
    except Exception as e:
        print(e)

    with open(full_path, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            func1(session, line, query_lists[2])

        func2(session, line, query_lists[3])

def insert_query_1(session, line, query_lists_ins):
    """INSERT query for requirement 1
    """
    try:
        session.execute(query_lists_ins, (line[0], line[9], float(line[5]),
                                          int(line[8]), int(line[3])))
    except Exception as e:
        print(e)

def select_query_1(session, line, query_lists_sel):
    """SELECT query for requirement 1
    """
    try:
        ans_qry = session.execute(query_lists_sel)
    except Exception as e:
        print(e)

    for row in ans_qry:
        print(row.artist_name, row.song_title, row.song_length)

def insert_query_2(session, line, query_lists_ins):
    """INSERT query for requirement 2
    """
    try:
        session.execute(query_lists_ins, (line[0], line[9],
                                          (line[1] + ' ' + line[4]),
                                          int(line[10]), int(line[8]),
                                          int(line[3])))
    except Exception as e:
        print(e)

def select_query_2(session, line, query_lists_sel):
    """SELECT query for requirement 2
    """
    try:
        ans_qry = session.execute(query_lists_sel)
    except Exception as e:
        print(e)

    for row in ans_qry:
        print(row.artist_name, row.song_title, row.user_name)

def insert_query_3(session, line, query_lists_ins):
    """INSERT query for requirement 3
    """
    try:
        session.execute(query_lists_ins, ((line[1] + ' ' + line[4]),
                                          line[9], int(line[10]),
                                          int(line[8]), int(line[3])))
    except Exception as e:
        print(e)

def select_query_3(session, line, query_lists_sel):
    """SELECT query for requirement 3
    """
    try:
        ans_qry = session.execute(query_lists_sel)
    except Exception as e:
        print(e)

    for row in ans_qry:
        print(row.user_name, row.song_title)

def ed_appHistData_drop_close(session, cluster, music_app_hist_DROP_list):
    for i in music_app_hist_DROP_list:
        try:
            session.execute(i)
        except Exception as e:
            print(e)

    session.shutdown()
    cluster.shutdown()

def main():
    """This execution is to grab all the files as a LIST in
    folder name mentioned in sql_queries.folder_nm
    """
    file_path_list_final = event_data_grab_file_list(folder_nm)

    """This execution is to browse through files in LIST and
    store all consolidated content to 'result' dataframe
    """
    result = event_data_filelist_data_to_df(file_path_list_final)

    """This execution is write dataframe ('result') content into
    a csv file in the name sql_queries.csv_file_nm
    """
    full_path = event_data_df_to_merged_csv(result, csv_file_nm)

    """This execution is for CLUSTER creation, KEYSPACE creation,
    KEYSPACE setup for Apache Cassandra NoSql db
    """
    session, cluster = event_data_nosql_setup()

    """This execution to CREATE table, LOAD data, SELECT query data
    to select artist,song,song-length for sessionId & itemInSession
    """
    print("---------------------------------------------------------------------------------------")
    print("select artist, songtitle & songlength from music app hist for sessionId & itemInSession")
    print("---------------------------------------------------------------------------------------")
    ed_appHistData_query_results(session, full_path, music_app_hist_1_list,
                                 insert_query_1,select_query_1)
    print("\n\n\n\n\n")


    """This execution to CREATE table, LOAD data, SELECT query data
    to select artist,song,user for userId & sessionId
    """
    print("-----------------------------------------------------------------------------")
    print("select artist, song (sorted by itemInSession) and user for userid & sessionid")
    print("-----------------------------------------------------------------------------")
    ed_appHistData_query_results(session, full_path, music_app_hist_2_list,
                                 insert_query_2,select_query_2)
    print("\n\n\n\n\n")


    """This execution to CREATE table, LOAD data, SELECT query data
    to select user (first and last name) for a song listened
    """
    print("--------------------------------------------------------------------------------")
    print("select user (first and last) in my music app history who listened to  given song")
    print("--------------------------------------------------------------------------------")
    ed_appHistData_query_results(session, full_path, music_app_hist_3_list,
                                 insert_query_3,select_query_3)
    print("\n\n\n\n\n")


    """This execution to DROP tables, CLOSE sesion & cluster
    """
    ed_appHistData_drop_close(session, cluster, music_app_hist_DROP_list)


if __name__ == "__main__":
    main()
