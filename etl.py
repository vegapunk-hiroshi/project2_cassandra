import pandas as pd
import cassandra
from cassandra.cluster import Cluster
import re
import os
import glob
import numpy as np
import json
import csv
from sql_queries import insert_data_queries

#ETL Pipeline for Pre-Processing the Files

def filepaths_list():
    """
    - Get the list of file paths from current folder and return 
    """
    # Get your current folder and subfolder event data
    filepath = os.getcwd() + '/event_data'
    
    all_filepath = []
    
    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):
        
        # join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root,'*'))
        
        all_filepath.append(file_path_list)
        
    all_filepath = all_filepath[0][:]
    
    all_filepath.sort()
    
    return all_filepath

    
def process_file():
    """
    - Use the list of paths to get the event data
    - Extract the data from the only needed columns.
    - Create the event data csv file to use.
    """
    
    # get file path list by filepaths_list()
    all_filepath = filepaths_list()
    
    song_all = []
    
    for n, f in enumerate(all_filepath):

        df = pd.read_csv(f)

        song = df[['artist','firstName','gender','itemInSession','lastName','length',
                   'level','location','sessionId','song','userId']]
        
        song = song.dropna(how='any')

        if n > 0:
            song_all = pd.concat([song_all, song], axis=0)
        else:
            song_all = song
            
    song_all = song_all.astype({'userId':int})
    
    song_all.to_csv("event_datafile_new.csv", encoding="utf-8", index=False)
    
    print(" 'event_datafile_new.csv' created.")
            
def insertion_data(session):
    """
    - Insert the data to the created tables.
    """
    file = 'event_datafile_new.csv'

    #read the csv by pandas
    dfnew = pd.read_csv(file, encoding = 'utf8')
    
    #extract the needed column for each insert statement from the csv
    df1 = dfnew[['sessionId','itemInSession','artist','song','length']]
    df2 = dfnew[['userId' , 'sessionId', 'itemInSession', 'artist', 'song', 'firstName' , 'lastName']]
    df3 = dfnew[['song', 'userId', 'firstName', 'lastName']]

    # insert records
    for i, row in df1.iterrows():
        session.execute(insert_data_queries[0], (row.sessionId, row.itemInSession, row.artist, row.song, row.length))
        if i%1000==0:
            print("*")

    print("Inserted to 1st table")
    
    # insert records
    for i, row in df2.iterrows():
        session.execute(insert_data_queries[1], (row.userId , row.sessionId, row.itemInSession, row.artist, row.song, row.firstName , row.lastName))
        if i%1000==0:
            print("*")
    print("Inserted to 2nd table")
    
    # insert records
    for i, row in df3.iterrows():
        session.execute(insert_data_queries[2], (row.song,row.userId,row.firstName, row.lastName))
        
        if i%1000==0:
            print("*")
        
    print("Inserted to 3rd table")
                
    
def main():
    """
    - Create the event data csv file.
    - Connect to the keyspace 'music_songs'.
    - Insert the data from the csv to the tables.
    - Shutdonw the connection with the keyspace.
    """
    process_file()
    
    cluster = Cluster(['127.0.0.1'])
    
    # To establish connection and begin executing queries, need a session
    session = cluster.connect()
    
    session.set_keyspace('music_songs')
    
    insertion_data(session)
    
    session.shutdown()
    cluster.shutdown()
    
if __name__ == "__main__":
    
    main()
    