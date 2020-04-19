import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, create_schema_queries



def drop_tables(cur, conn):
    '''
    This function drops the tables when called.It uses SQL 
    queries list to the drop tables  defined in the sql_queries.py
    params:
    param cur: cursor to the database tables.
    param conn: conncection string to the database
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    This function creates the tables when called. It uses  a list of 
    SQL queries to create the tables in the database in the sql_queries.py
    params:
    param cur : cursor to the database
    param conn: Connection string to the redshift database.
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        
def create_schemas(cur, conn):
    """This function will call sql_queries.py to create the stage and star schemas.
    params:
    param cur -- the connected cursor
    param conn -- connection string to redshift database
    """

    for query in create_schema_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    This function connects to the Redshift Database and calls the functions to
    create the schemas, drop already existing tables and then creates new tables.
    
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    create_schemas(cur, conn)
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()