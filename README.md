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

1.

	SELECT * FROM song_per_session 
	WHERE session_id = 338 AND iteminsession = 4
 
2.

	SELECT artist, song, firstname, lastname 
	FROM user_listened WHERE user_id = 10 AND session_id = 182


3.

	SELECT song, firstname , lastname 
	FROM wholistened_theSong WHERE song = 'All Hands Against His Own'
	
Therefore, the schema of the tables to be created are below with some description of table schema:

1.PARTITION KEY are session_id and iteminsession.

	CREATE TABLE IF NOT EXISTS song_per_session(
	session_id int, 
	iteminsession int, 
	artist text, 
	song text, 
	length float, 
	PRIMARY KEY((session_id, iteminsession))
	

2.PARTITION KEY are user_id, session_id. CLUSTER KEY is iteminsession so it will be sorted.  

	
	CREATE TABLE IF NOT EXISTS user_listened(
	user_id int, 
	session_id int, 
	iteminsession int, 
	artist text, 
	song text, 
	firstName text, 
	lastName text, 
	PRIMARY KEY((user_id, session_id), iteminsession)


3.PARTITION KEY are song.

	CREATE TABLE IF NOT EXISTS wholistened_theSong(
	song text, 
	firstName text, 
	lastName text, 
	PRIMARY KEY(song)
	
## Files included in the REPO:

1. event_data consists of files in CSV formats generated from the music app and the files are partitioned by data.
2. event_datafile_new.csv which is denormalized dataset will be created from the CSV files in even_data by running etl.py.

* create_tables.py that automatically drops the tables if they already exists and creates the tables as defined in the sql_queries module.
* etl.py that contains the main program and manages the file processing needed for reading the files in CSV formats and inserting the data to the Apache Cassandra DB tables that was defined by the create_tables.py.
* sql_queries.py that includes the NoSQL DB create & insert statements separeted in their own file for modularity.
* test.ipynb that includes the SELECT statements to verify the data inserted in the each table created.


## Steps to run the projects:

1. Execute the "python create_tables.py" file in the Terminal to create all the DB tables.
* Execute the "python etl.py" file in the Terminal to extract from all csv files to create eventdatafilenew.csv and insert all records into  the tables.
* Verify the data in the each table by each the SELECT statement written in the cell in the test.ipynb.