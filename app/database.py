import sqlite3
from datetime import datetime, timedelta
import config
import pandas as pd
import traceback
import helper_functions

db_path = config.DATABASE_PATH
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

def create_tables():
    """Creates all tables needed"""
    # cursor = db.get_cursor()
    with open(f'queries/create_tables.sql', 'r') as f:
        query = f.read()
    print(f"Database Operations - Creating drop/load tables in SQLite. Path of database: {db_path}")
    cursor.executescript(query)

def execute_query(query, params=None):
    try:
        if params:
            print(f'Database Operations - Executing {query} with the following parameters: {params}')
            cursor.execute(query, params)

        else:
            print(f'Database Operations - Executing {query}')
            cursor.execute(query)
        return cursor.fetchall()
    except Exception as e:
        print(f"Error executing query: {e}")
        helper_functions.log_error(e)
        return None

def table_to_df(table_name: str):
    # cursor = db.get_cursor()
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    print(f"Database Operations - Retrieving {table_name}")
    return df

def insert_data_to_db(df, table_name: str):
    # conn = db.get_connection()
    try:
        df.to_sql(table_name, conn, if_exists='append', index=False)  # Appends data to the table
        print(f"Database Operations - Data inserted into '{table_name}' successfully.")
    except Exception as e:
        helper_functions.log_error(e)
        print(f"Database Operations - Error inserting data in '{table_name}': {e}")

def create_backup_table(table_name):

    try:
        delete_query = f"DROP TABLE IF EXISTS {table_name}_backup"
        print(f"Database Operations - Executing: {delete_query}")
        cursor.execute(delete_query)

        copy_query = f"CREATE TABLE {table_name}_backup AS SELECT *, DATE('now') AS backup_date FROM {table_name}"
        print(f"Database Operations - Executing: {copy_query}")
        cursor.execute(copy_query)

        result_query = f"SELECT COUNT(*) FROM {table_name}_backup"
        result = execute_query(query=result_query)
        print(f"Database Operations - Backup table '{table_name}_backup' contains {result[0][0]} rows.")

    except Exception as e:
        helper_functions.log_error(e)
        print(f"Database Operations - Error creating backup table '{table_name}_backup': {e}")
    conn.commit()


def get_max_date_for_season(table_name, season):
    """Get the max date for a given season."""
    query = f"SELECT MAX(report_date) FROM {table_name} WHERE season = ?"
    params = (season,)
    result = execute_query(query=query, params=params)
    print(f"Database Operations - Retrieving the max date from {table_name}")
    if result == None:
        return None
    else:
     return result[0][0]
    
def incremental_load_delete(table_name, season, delete_date):
    count_query = f"SELECT COUNT(*) FROM {table_name} WHERE season = ? AND report_date >= ?"
    count_params = (season, delete_date)
    
    result = execute_query(count_query, count_params)
    
    if result and result[0][0] > 0:
        delete_query = f"DELETE FROM {table_name} WHERE season = ? AND report_date >= ?"
        delete_params = (season, delete_date)
        
        execute_query(delete_query, delete_params)
        cursor.connection.commit()  # Make sure to commit after delete
        print(f"Database Operations - Deleted {result[0][0]} records on or after {delete_date} from '{table_name}' for the {season} season to ensure no duplicate data is inserted during incremental load")
    else:
        print(f"Database Operations - No records on or after {delete_date} from '{table_name}' for the {season} season found to delete")
    


