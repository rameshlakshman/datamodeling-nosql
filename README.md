DATA MODELING WITH CASSANDRA  & ETL PIPELINE :
==============================================
Description : Sparkify needs to Analyze the songs collected & user activity on the music app. The analysis team needs below information for their analysis.

1. Find: artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4
2. Find: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
3. Find: every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

INPUTS :
========
Code is designed in such a way the changes will go only into the 'sql_queries.py'. 
This file has below variables that can be edited as per the needs.
	(1) folder_nm  		=> Where all the source csv files are located
	(2) csv_file_nm 	=> Merged content of all source-csv-files are written into this file
	(3) music_app_hist_1_drop	=> Drop query for table 1
	(4) music_app_hist_2_drop	=> Drop query for table 2
	(5) music_app_hist_3_drop	=> Drop query for table 3
	(6) music_app_hist_1_create	=> Create query for table 1
	(7) music_app_hist_1_create => Create query for table 2
	(8) music_app_hist_1_create => Create query for table 3
	(6) music_app_hist_1_insert	=> Insert query for table 1
	(7) music_app_hist_2_insert => Insert query for table 2
	(8) music_app_hist_3_insert => Insert query for table 3
	(6) music_app_hist_1_select	=> Select query for table 1
	(7) music_app_hist_2_select => Select query for table 2
	(8) music_app_hist_3_select => Select query for table 3

ETL PIPELINE:
=============
Below are the process steps -
	(1) FUNCTION """event_data_grab_file_list""" 
			This is called to get all files as LIST
	(2) FUNCTION """event_data_filelist_data_to_df"""
			This is called to read all content in files and store in 'dataframe'
	(3) FUNCTION """event_data_df_to_merged_csv"""
			This is called to move 'dataframe' content from step-2 into csv-file
	(4) FUNCTION """event_data_nosql_setup"""
			This is executed to setup CLUSTER, SESSION, KEYSPACE for NoSQL
	(5) FUNCTION """ed_appHistData_query_results"""
			This is a generic function that takes below parameters
				SESSION, CSV_FILE_NAME, QUERY_LISTS, INSERT_FUNC_NM, SELECT_FUNC_NM
			Below are executed one by one for the three requirements mentioned in description.
				=> ed_appHistData_query_results(session, full_path, music_app_hist_1_list, insert_query_1,select_query_1)
				=> ed_appHistData_query_results(session, full_path, music_app_hist_2_list, insert_query_2,select_query_2)
				=> ed_appHistData_query_results(session, full_path, music_app_hist_3_list, insert_query_3,select_query_3)				
	(6) FUNCTION """ed_appHistData_drop_close"""
			This is executed finally to DROP tables, CLOSE cluster and session.			