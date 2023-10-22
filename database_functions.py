import pymysql
import pandas as pd
from datetime import datetime
import time
import json
import logging

from config import *  # Importing configurations

# conexión a la base de datos desde el exterior de Hostinger
def connect_to_db():
    try:
        connection = pymysql.connect(host=db_config['host'],
                                     port=int(db_config.get('port', 3306)),  # Default port is 3306
                                     user=db_config['user'],
                                     password=db_config['password'],
                                     db=db_config['database'])
        return connection
    except pymysql.MySQLError as e:
        print(f"Error connecting to the database: {e}")
        return None

def insert_data_into_db(df, table, date_to_insert, incremental=False):
    max_retries = 3
    retries = 0

    while retries < max_retries:
        connection = connect_to_db()
        if connection:
            break
        print(f"Retrying connection ({retries + 1}/{max_retries})")
        retries += 1
        time.sleep(5)  # Wait for 5 seconds before retrying

    if connection is None:
        print("Failed to establish a connection after several attempts.")
        return

    cursor = connection.cursor()
    try:
        # Obtiene los nombres de las columnas de la tabla destino
        cursor.execute(f"DESCRIBE {table};")
        columns = [column[0] for column in cursor.fetchall()]
        
        # Asegurarse de que 'date' está en la lista de columnas
        if 'date' not in columns:
            print(f"La tabla {table} debe tener una columna 'date'")
            return

        # Delete existing records if incremental is True
        if incremental:
            cursor.execute(f"DELETE FROM {table} WHERE date = %s", (date_to_insert,))
            connection.commit()
        
        # inicializa los datos a insertar en la tabla
        data_to_insert = []

        for index, row in df.iterrows():
            
            row_data = [date_to_insert] + [row[col] for col in df.columns]

            # el numero de peregrinos siempre es mayor que cero porque la
            # query a PBI lo utiliza como filtro
            data_to_insert.append(tuple(row_data))

            if len(data_to_insert) >= 500:
                all_columns = ', '.join(columns)
                update_columns = ', '.join([f"{col}=VALUES({col})" for col in columns])
                placeholders = ', '.join(['%s'] * len(row_data))
                insert_query = f"INSERT INTO {table} ({all_columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_columns}"

                cursor.executemany(insert_query, data_to_insert)
                connection.commit()
                data_to_insert = []

        if len(data_to_insert) > 0:
            all_columns = ', '.join(columns)
            update_columns = ', '.join([f"{col}=VALUES({col})" for col in columns])
            placeholders = ', '.join(['%s'] * len(row_data))
            insert_query = f"INSERT INTO {table} ({all_columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_columns}"

            cursor.executemany(insert_query, data_to_insert)
            connection.commit()

    except Exception as e:
        print(f"Error during data insertion: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

def insert_data_into_db_last_day(data_dict):
    # Guarda los datos de resumen de pilgrims sencillos

    # Establish a connection to the MySQL database
    connection = pymysql.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        database=db_config['database']
    )
    
    try:
        # Create a cursor object
        cursor = connection.cursor()
        
        # SQL query to insert the data into the table
        sql_query = """INSERT IGNORE INTO stats_pilgrims_last_day (date, pilgrims)
                       VALUES (%s, %s)"""
        
        # Values to insert into the table
        values = (data_dict.get('date', None), data_dict.get('pilgrims', None))
        
        # Execute the SQL query
        cursor.execute(sql_query, values)
        
        # Commit the transaction
        connection.commit()
        
    except pymysql.MySQLError as e:
        print(f"An error occurred: {e}")
        
    finally:
        # Close the database connection
        connection.close()

def get_queries_from_db():

    max_retries = 3
    retries = 0

    while retries < max_retries:
        connection = connect_to_db()
        if connection:
            break
        logging.warning(f"Retrying connection ({retries + 1}/{max_retries})")
        retries += 1
        time.sleep(15)  # Wait for 15 seconds before retrying

    if connection is None:
        logging.error("Failed to establish a connection after several attempts.")
        return None, None, None

    cursor = connection.cursor()

    # Buscar en la tabla db_queries_stats
    cursor.execute("SELECT template, query FROM db_queries_stats")

    # Inicializar la variable
    query_date_last_day = None
    template_query_year_month = None
    template_query_date = None

    # Iterar sobre los resultados
    for row in cursor:
        if row[0] == "query_date_last_day":
            query_date_last_day = row[1]
        if row[0] == "query_template_all_columns_year_month":
            template_query_year_month = row[1]
        if row[0] == "query_template_all_columns_any_day":
            template_query_date = row[1]

    # Cerrar cursor y conexión
    cursor.close()
    connection.close()

    return json.loads(query_date_last_day), json.loads(template_query_year_month), json.loads(template_query_date)

def get_sum_pilgrims_last_day():
    """
    This function reads data from the "db_caminos" table for the last recorded day
    and returns the sum of the "pilgrims" column.
    """
    # Establish a connection to the database
    connection = connect_to_db()
    
    if connection is None:
        print("Failed to establish a connection.")
        return None

    cursor = connection.cursor()

    # Step 1: Determine the last recorded date
    cursor.execute(f"SELECT MAX(date) FROM {tabla_check};")
    last_date = cursor.fetchone()[0]
    if not last_date:
        print("No records found in the table.")
        cursor.close()
        connection.close()
        return None, None
    last_date_str = last_date.strftime('%Y-%m-%d')

    # Step 2: Query the data for the last date and sum the "pilgrims" column
    sql_query = f"SELECT SUM(pilgrims) FROM {tabla_check} WHERE date = '{last_date_str}';"
    cursor.execute(sql_query)
    total_pilgrims = cursor.fetchone()[0]

    # Close the cursor and the connection
    cursor.close()
    connection.close()

    return last_date, total_pilgrims

def get_pilgrims_last_day():
    """
    This function reads data from the "db_caminos" table for the last recorded day
    and returns the sum of the "pilgrims" column.
    """
    # Establish a connection to the database
    connection = connect_to_db()
    
    if connection is None:
        print("Failed to establish a connection.")
        return None

    cursor = connection.cursor()

    # Step 1: Determine the last recorded date
    cursor.execute(f"SELECT MAX(date) FROM {tabla_pilgrims_last_day};")
    last_date = cursor.fetchone()[0]
    if not last_date:
        print("No records found in the table.")
        cursor.close()
        connection.close()
        return None, None
    last_date_str = last_date.strftime('%Y-%m-%d')

    # Step 2: Query the data for the last date and sum the "pilgrims" column
    sql_query = f"SELECT pilgrims FROM {tabla_pilgrims_last_day} WHERE date = '{last_date_str}';"
    cursor.execute(sql_query)
    total_pilgrims = cursor.fetchone()[0]

    # Close the cursor and the connection
    cursor.close()
    connection.close()

    return last_date_str, total_pilgrims