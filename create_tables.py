from cassandra.cluster import Cluster
from sql_queries import create_table_queries, drop_table_queries


def create_keyspace():
    """
    - Create keyspace 'music_songs' and set to it.
    - Returns the session and cluster.
    """
    
    cluster = Cluster(['127.0.0.1'])
    
    # To establish connection and begin executing queries, need a session
    session = cluster.connect()
    
    # Create a Keyspace
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS music_songs 
        WITH REPLICATION =
        {'class':'SimpleStrategy', 'replication_factor':1}""")
    # Set KEYSPACE to the keyspace specified above
    session.set_keyspace('music_songs')
    
    return session, cluster
    
    
def drop_tables(session):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        session.execute(query)
    
    
def create_tables(session):
    """
    Create each table using the queries in the `create_table_queries` list.
    """
    for query in create_table_queries:
        session.execute(query)
        
        
def main():
    """
    - Create keyspace and returns session cluster
    
    - Drop all tables if exists
    
    - Create all tables needed that is design after query
    
    - Finally, close the connection.
    """
    
    
    session, cluster = create_keyspace()

    drop_tables(session)
    
    create_tables(session)
    
    session.shutdown()
    cluster.shutdown()
    
if __name__ == "__main__":
    main()
    