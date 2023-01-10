import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function takes the list of copy operations for staging tables defined in sql_queries.py
    and executes them in order
    -------------------------------------------------------
    Arguments:
        cur: Database cursor
        conn: Database connection
    Returns:
        None
    """ 
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function takes the list of insert operations defined in sql_queries.py
    and executes them in order
    -------------------------------------------------------
    Arguments:
        cur: Database cursor
        conn: Database connection
    Returns:
        None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This is the main function whose aim is to read the config file,
    create the connection and cursor to the database and call 
    the other 2 functions to load the data in the Redshift database
    ---------------------------------------------------
    Arguments:
        None
    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print("Conn established")
    cur = conn.cursor()
    print("Cur created")
    load_staging_tables(cur, conn)
    print("Staging tables loaded!")
    insert_tables(cur, conn)
    print("Data from staging tables inserted into songplays, songs, users,artist and time!")

    conn.close()


if __name__ == "__main__":
    main()