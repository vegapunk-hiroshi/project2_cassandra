#DROP TABLES
    
drop_tbl_song_per_session = "DROP TABLE IF EXISTS song_per_session"
drop_tbl_user_listened = "DROP TABLE IF EXISTS user_listened"
drop_tbl_wholistened_theSong = "DROP TABLE IF EXISTS wholistened_theSong"


#CREATE TABLES

song_per_session_create = """
CREATE TABLE IF NOT EXISTS song_per_session(
sessionId int, 
itemInSession int, 
artist text,
song text,
length float, 
PRIMARY KEY((sessionId, itemInSession))
)
"""

user_listened_create = """
CREATE TABLE IF NOT EXISTS user_listened(
userId int, 
sessionId int, 
itemInSession int, 
artist text, 
song text, 
firstName text, 
lastName text, 
PRIMARY KEY((userId, sessionId), itemInSession)
)"""

wholistened_theSong_create = """
CREATE TABLE IF NOT EXISTS wholistened_theSong(
song text,
userId int,
firstName text, 
lastName text, 
PRIMARY KEY((song, userId))
)"""


# INSERT DATA
song_per_session_insert =  """
INSERT INTO song_per_session (
sessionId, itemInSession, artist, song, length)
VALUES (%s, %s, %s, %s, %s)
"""

user_listened_insert =  """
INSERT INTO user_listened (userId , sessionId, 
iteminsession, artist, song, firstname , lastname) 
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

wholistened_theSong_insert =  """
INSERT INTO wholistened_theSong (song, userId, firstname , lastname) 
VALUES (%s, %s, %s, %s)
"""

# QUERY LISTS
create_table_queries  = [song_per_session_create, user_listened_create, wholistened_theSong_create]
drop_table_queries = [drop_tbl_song_per_session, drop_tbl_user_listened , drop_tbl_wholistened_theSong]
insert_data_queries = [song_per_session_insert, user_listened_insert, wholistened_theSong_insert]

