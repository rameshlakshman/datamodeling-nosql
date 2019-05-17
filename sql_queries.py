#
folder_nm = '/event_data'
csv_file_nm = '/event_datafile_prod.csv'

# DROP TABLES

music_app_hist_1_drop = "DROP TABLE IF EXISTS music_app_hist_1"
music_app_hist_2_drop = "DROP TABLE IF EXISTS music_app_hist_1"
music_app_hist_3_drop = "DROP TABLE IF EXISTS music_app_hist_1"

# CREATE TABLES

music_app_hist_1_create = ("""CREATE TABLE IF NOT EXISTS music_app_hist_1
                                (artist_name text, 
                                 song_title text, 
                                 song_length float,  
                                 sessionId int, 
                                 itemInSession int, 
                              PRIMARY KEY (sessionId, itemInSession))
                           """)

music_app_hist_2_create = ("""CREATE TABLE IF NOT EXISTS music_app_hist_2
                                (artist_name text, 
                                 song_title text, 
                                 user_name text, 
                                 user_id int, 
                                 session_Id int, 
                                 item_in_session int, 
                              PRIMARY KEY ((user_id, session_Id),item_in_session))
                           """)

music_app_hist_3_create = ("""CREATE TABLE IF NOT EXISTS music_app_hist_3
                                (user_name text, 
                                 song_title text, 
                                 user_id int, 
                                 session_Id int, 
                                 item_in_session int, 
                              PRIMARY KEY ((song_title),user_id))
                           """)

# INSERT RECORDS

music_app_hist_1_insert = ("""insert into music_app_hist_1 
                                    (artist_name, 
                                     song_title,  
                                     song_length, 
                                     sessionId,   
                                     itemInSession)
                              values (%s,%s,%s,%s,%s)""")

music_app_hist_2_insert = ("""insert into music_app_hist_2
                                    (artist_name, 
                                     song_title, 
                                     user_name, 
                                     user_id, 
                                     session_id, 
                                     item_in_session)
                              values (%s,%s,%s,%s,%s,%s)""")

music_app_hist_3_insert = ("""insert into music_app_hist_3
                                    (user_name, 
                                     song_title, 
                                     user_id, 
                                     session_Id, 
                                     item_in_session)
                              values (%s,%s,%s,%s,%s) """)

# SELECT RECORDS

music_app_hist_1_select = ("""SELECT * FROM music_app_hist_1  
                               WHERE SESSIONID = 338 
                                 AND ITEMINSESSION = 4
                           """)

music_app_hist_2_select = ("""SELECT * FROM music_app_hist_2 
                               WHERE user_id = 10 
                                 AND session_id = 182
                           """)

music_app_hist_3_select = ("""SELECT * FROM music_app_hist_3 
                               WHERE song_title = 'All Hands Against His Own'
                           """)


# QUERY LISTS

music_app_hist_1_list = [music_app_hist_1_drop,music_app_hist_1_create,music_app_hist_1_insert,music_app_hist_1_select]
music_app_hist_2_list = [music_app_hist_2_drop,music_app_hist_2_create,music_app_hist_2_insert,music_app_hist_2_select]
music_app_hist_3_list = [music_app_hist_3_drop,music_app_hist_3_create,music_app_hist_3_insert,music_app_hist_3_select]
music_app_hist_DROP_list = [music_app_hist_1_drop, music_app_hist_2_drop, music_app_hist_3_drop]
