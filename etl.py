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

    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):
        
        # join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root,'*'))
    
    return file_path_list

        
def process_file():
    """
    - Use the list of paths to get the event data
    - Extract the data from the only needed columns.
    - Create the event data csv file to use.
    """
    
    # initiating an empty list of rows that will be generated from each file
    full_data_rows_list = [] 

    # get file path list by filepaths_list()
    file_path_list = filepaths_list()
    for f in file_path_list:
        
        # reading csv file 
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
            
            # creating a csv reader object 
            csvreader = csv.reader(csvfile) 
            
            next(csvreader)
            
            # extracting each data row one by one and append it        
            for line in csvreader:
                
                #print(line)
                full_data_rows_list.append(line) 

    # creating a smaller event data csv file called event_datafile_full csv that will be used to 
    # insert data into the Apache Cassandra tables
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
        
        writer = csv.writer(f, dialect='myDialect')
        
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                    'level','location','sessionId','song','userId'])
        
        for row in full_data_rows_list:
            
            if (row[0] == ''):
                continue
                
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))
            
            
            
def insertion_data(session):
    
    """
    - Insert the data to the created tables.
    """
    
    file = 'event_datafile_new.csv'
    
    with open(file, encoding = 'utf8') as f:
        
        csvreader = csv.reader(f)
        
        next(csvreader)

        for line in csvreader:
            
            # Assign which column element should be assigned for each column in the INSERT statement.
            session.execute(insert_data_queries[0], (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))
            
            session.execute(insert_data_queries[1], (int(line[10]), int(line[8]), int(line[3]), line[0], line[9], line[1], line[4]))
            
            session.execute(insert_data_queries[2], (line[9], line[1], line[4]))
            
    print("insert successfull")
    
    
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
    
    