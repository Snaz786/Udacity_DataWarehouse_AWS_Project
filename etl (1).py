import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load Staging tables from s3 log_data and song_data in Json format
    into staging_events and staging_songs tables.
    
    Keyword aruguments:
    cur -- cursor
    conn --  connection to the database.
    
    Output:
    * log_data in staging_events table.
    * song_data in staging_songs table.
    
    """
    
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    
    Insert data from staging tables (staging_events and staging_songs)
        into star schema analytics tables:
        * Fact table: songplays
        * Dimension tables: users, songs, artists, time
    
    Keyword arguments:
    cur  -- cursor
    conn -- connection to database
    
    Output:
    * log_data in staging_events table.
    * song_data in staging_songs table.
    
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()