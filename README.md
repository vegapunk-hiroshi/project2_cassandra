# Project1B
## Goals
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app and the data resides in a directory of CSV files on user activity on the app.
In order to answer their question to the data, I will create an Apache Cassandra database.


## Data modeling with NoSQL database
Their questions are three below:

1. Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4
2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

In order to answer these questions, the each queries should be like below:

1. Getting the artist and song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4

	SELECT sessionId, iteminsession, artist, song, length 
	FROM song_per_session 
	WHERE sessionId = 338 AND iteminsession = 4
 
2. Getting the name of artist, song (sorted by itemInSession) and user (first and last name)

	SELECT artist, song, firstname, lastname 
	FROM user_listened WHERE userId = 10 AND sessionId = 182

3. Getting every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

	SELECT firstname , lastname 
	FROM wholistened_theSong WHERE song = 'All Hands Against His Own' and userId = 10
	
Therefore, the schema of the tables to be created are below with some description of table schema:

1. For this query iteminsession and sessionid are considered as the composite key. Because these keys make the row unique and be able to know the specific information.'


	CREATE TABLE IF NOT EXISTS song_per_session(
	sessionId int, 
	iteminsession int, 
	artist text, 
	song text, 
	length float, 
	PRIMARY KEY((sessionId, iteminsession))
	

2. For this query userid and sessionid are considered as the composite key to know who listened the song and what song the user listned during the session. Iteminsessionid is considered as clustering key in order to sort the song's order in ascending.

	
	CREATE TABLE IF NOT EXISTS user_listened(
	userId int, 
	sessionId int, 
	iteminsession int, 
	artist text, 
	song text, 
	firstName text, 
	lastName text, 
	PRIMARY KEY((userId, sessionId), iteminsession)


3. For this query song and userid are considered as the composite key in order to know who listened to the specific song.

	CREATE TABLE IF NOT EXISTS wholistened_theSong(
	firstName text, 
	lastName text, 
	PRIMARY KEY((song, userId))
	
    
## Files included in the REPO:

1. event_data consists of files in CSV formats generated from the music app and the files are partitioned by data.
2. event_datafile_new.csv which is denormalized dataset will be created from the CSV files in even_data by running etl.py.

* create_tables.py that automatically drops the tables if they already exists and creates the tables as defined in the sql_queries module.
* etl.py that contains the main program and manages the file processing needed for reading the files in CSV formats and inserting the data to the Apache Cassandra DB tables that was defined by the create_tables.py.
* sql_queries.py that includes the NoSQL DB create & insert statements separeted in their own file for modularity.
* test.ipynb that includes the SELECT statements to verify the data inserted in the each table created.


## Steps to run the projects:

Open the 'Project_1B.ipynb' file in Jupyter Notebook and execute the cellls from the top to bottom by following the guidance.