import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function takes the list of dropping operations defined in sql_queries.py
    and executes them in order
        -------------------------------------------------------
    Arguments:
        cur: Database cursor
        conn: Database connection
    Returns:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function takes the list of creating table operations defined in sql_queries.py
    and executes them in order
        -------------------------------------------------------
    Arguments:
        cur: Database cursor
        conn: Database connection
    Returns:
        None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This is the main function whose aim is to read the config file,
    create the connection and cursor to the database and call 
    the other 2 functions to create empty tables in the 
    Redshift selected database
    -------------------------------------------------------------------------
    Arguments:
        None
    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print("Connection and cursor created!")

    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    print("Tables created!")

    conn.close()

    print("Connection closed!")

if __name__ == "__main__":
    main()