import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    This function copies the data from the S3 to the staging tables
    as a step before OLAP. 
    params:
    param cur: A cursor to the database instance
    param conn: The connection string to the Postgres databse
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    This fucntion inserts the values from the staging table into the 
    tables with Star Schema at Redshift for OLAP
    params:
    param cur: A cursor to the database instance
    param conn: The connection string to the Postgres database
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    This function calls appropriate SQL queries to load in data from S3 to staging tables, 
    performs different types of transformations, validations and then load them into fact and 
    dimensional tables.
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()