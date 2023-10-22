import json
import requests
from config import *
from data_transformer import *
from database_functions import *
import time
from datetime import datetime
import math
from bs4 import BeautifulSoup
import re
import logging


def fetch_data_day(query):
    # Recuperar la información del servicio PBI
    response = requests.post(URL_OP_PBI, headers=HEADERS, json=query)
    return json.loads(response.text)


def update_stats_last_day(query_last_day, query_year_month):

    response_data = fetch_data_day(query_last_day)
    info_last_day = extract_date_last_day(response_data)

    # guarda la respuesta en la db
    insert_data_into_db_last_day(info_last_day)

    for item in queries_tables:
        dimensions = item['dimensions']
        tabla_db = item['tabla'] + 'last_day'
        # primero ajusto la plantilla con cada una de las queries
        query_pbi = adjust_query(query_year_month, dimensions)

        # lanzo la petición a PBI
        response_data = fetch_data_day(query_pbi)

        # extraigo los datos de cada una de las respuestas
        columns, dm0_data = extract(response_data)

        # Transformar los datos para obtener un pandas que poder guardar en una base de datos
        df = convert_to_dataframe(columns, dm0_data, dimensions)

        # Pasar directamente a base de datos
        insert_data_into_db(df, tabla_db, info_last_day['date'])

        # Limpieza de variables
        dimensions = None
        tabla_db = None
        query_pbi = None
        response_data = None
        columns = None
        dm0_data = None
        df = None

        # espera 1s antes de seguir con el bucle
        time.sleep(1)

    return info_last_day['pilgrims']


def update_stats_year_month(query_year_month, start_year=2003, incremental=True):

    # Mapeo de nombres de meses en inglés a español
    month_map = {
        'January': 'enero',
        'February': 'febrero',
        'March': 'marzo',
        'April': 'abril',
        'May': 'mayo',
        'June': 'junio',
        'July': 'julio',
        'August': 'agosto',
        'September': 'septiembre',
        'October': 'octubre',
        'November': 'noviembre',
        'December': 'diciembre'
    }

    # Mapeo inverso de nombres de meses en español a números de mes
    month_number_map = {
        'enero': 1,
        'febrero': 2,
        'marzo': 3,
        'abril': 4,
        'mayo': 5,
        'junio': 6,
        'julio': 7,
        'agosto': 8,
        'septiembre': 9,
        'octubre': 10,
        'noviembre': 11,
        'diciembre': 12
    }

    # Obtener el mes actual en inglés y convertirlo a español
    now = datetime.now()
    current_year = now.year
    # Devuelve el mes en inglés, por ejemplo, "January"
    current_month_english = now.strftime("%B")
    # Convierte a español
    current_month = month_map[current_month_english.lower().capitalize()]

    # Definir el rango de fechas a iterar
    if incremental:
        years = [current_year]
        months = [current_month]
    else:
        # Desde 2003 hasta el año actual
        years = range(start_year, current_year + 1)
        months = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio',
                  'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre']

    # Calcula el número total de conexiones requeridas
    total_connections = 0
    for year in years:
        if year == current_year:
            total_connections += len(months[:months.index(
                current_month) + 1]) * len(queries_tables)
        else:
            total_connections += len(months) * len(queries_tables)

    # Calcula el tiempo de espera para no exceder las 500 conexiones por hora
    if incremental:
        if total_connections <= 500:
            wait_time = 3600 / 500
        else:
            wait_time = 3600 / total_connections
    else:
        wait_time = 1

    wait_time = math.ceil(wait_time)

    # Variable para almacenar el último valor de df['M0'].sum()
    last_value = None

    for year in years:
        if year == current_year:
            months_to_iterate = months[:months.index(
                current_month) + 1]  # Solo hasta el mes actual
        else:
            months_to_iterate = months  # Todos los meses

        for month in months_to_iterate:
            for item in queries_tables:
                dimensions = item['dimensions']
                tabla_db = item['tabla'] + 'monthly'

                # Carga y ajusta la plantilla de consulta
                query_pbi = adjust_query(
                    query_year_month, dimensions, year, month)

                # Obtiene y procesa los datos
                response_data = fetch_data_day(query_pbi)
                columns, dm0_data = extract(response_data)

                df = convert_to_dataframe(columns, dm0_data, dimensions)
                # Almacenar el último valor de df['M0'].sum()
                last_value = df['M0'].sum()

                # Inserta los datos en la base de datos
                # primero hay que construir la fecha de las estadisticas en función del año y del mes del buclo

                current_month_number = month_number_map[current_month]
                stats_date = datetime(
                    year, month_number_map[month], 1).strftime('%Y-%m-%d')

                insert_data_into_db(df, tabla_db, stats_date, incremental)

                # Limpieza de variables
                dimensions = None
                tabla_db = None
                query_pbi = None
                response_data = None
                columns = None
                dm0_data = None
                df = None

                # Espera 2 segundos antes de la próxima iteración
                time.sleep(wait_time)

    return int(last_value)


def update_stats_date(query_date, dates):

    # Calcula el número total de conexiones requeridas
    total_connections = len(dates)

    wait_time = 1  # 1s - calcular cuando haya muchas fechas

    sum_values = []  # Variable para almacenar el valor de df['M0'].sum() para cada fecha

    for date in dates:
        for item in queries_tables:
            dimensions = item['dimensions']
            tabla_db = item['tabla'] + 'last_day'

            # Carga y ajusta la plantilla de consulta
            query_pbi = adjust_query_per_day(query_date, dimensions, date)

            # Obtiene y procesa los datos
            response_data = fetch_data_day(query_pbi)
            columns, dm0_data = extract(response_data)

            df = convert_to_dataframe(columns, dm0_data, dimensions)
            # Almacenar el último valor de df['M0'].sum()
            date_sum = int(df['M0'].sum())

            # cambiar el formato de la fecha para guardarlo en base de datos
            # Convert the string to a datetime object using strptime
            date_obj = datetime.strptime(date, "%d/%m/%Y")

            # Convert the datetime object back to a string in "YYYY-MM-dd" format using strftime
            new_date_str = date_obj.strftime("%Y-%m-%d")

            insert_data_into_db(df, tabla_db, new_date_str)

            # Limpieza de variables
            dimensions = None
            tabla_db = None
            query_pbi = None
            response_data = None
            columns = None
            dm0_data = None
            df = None

            # Espera x segundos antes de la próxima iteración
            time.sleep(wait_time)
        sum_values.append(date_sum)

    return sum_values


def get_pilgrims_now():
    """
    Description:
    The function `get_pilgrims_now` aims to extract a specific numeric value from a webpage, which ostensibly
    represents the current count of pilgrims. It sends an HTTP GET request to a designated URL (`URL_OP_WEBSITE`),
    processes the HTML content of the webpage using BeautifulSoup, and scans for an HTML `h1` element within a `div`
    with the class `wpb_wrapper`. It uses regex to find numeric values within this `h1` element's text. If a numeric
    value is found, it returns the first numeric value as an integer. If an HTTP request error occurs or no numeric
    value is found within the specified criteria, it logs the error or warning respectively and returns `None`.

    Inputs:
    - No direct inputs are required by the user as the URL is presumably defined elsewhere in the code (`URL_OP_WEBSITE`).
    Outputs:
    - On Success: Returns the first numeric value found within the specified `h1` element as an integer.
    - On Failure: 
      - Logs an error and returns `None` if there's an HTTP request error.
      - Logs a warning and returns `None` if no numeric value is found within the specified criteria.
    Exceptions:
    - `requests.RequestException`: Handles any exceptions that are thrown during the HTTP request process,
      logs an error message, and returns `None`.
    """

    try:
        response = requests.get(URL_OP_WEBSITE)
        response.raise_for_status()  # Check if the request was successful
    except requests.RequestException as e:
        logging.error(f"An error occurred: {e}")
        return None

    soup = BeautifulSoup(response.content, 'html.parser')
    h1_elements = soup.select("div.wpb_wrapper > h1")

    for h1 in h1_elements:
        text = h1.text
        numbers = re.findall(r"\d+", text)
        if numbers:
            return int(numbers[0])  # return the first number found

    logging.warning("No number found with the specified criteria.")
    return None


def log_pilgrims_count():
    # Get the current count of pilgrims
    pilgrims_count = get_pilgrims_now()
    if pilgrims_count is None:
        logging.error("Failed to retrieve pilgrims count.")
        return None

    # Connect to the database
    connection = connect_to_db()

    # Insert the data into the database or update if the date already exists
    date = datetime.now().strftime('%Y-%m-%d')
    try:
        with connection.cursor() as cursor:
            sql_query = f"""
                INSERT INTO {tabla_pilgrims_last_day} (date, pilgrims)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE pilgrims = VALUES(pilgrims);
            """
            cursor.execute(sql_query, (date, pilgrims_count))
        connection.commit()
    except pymysql.MySQLError as e:
        logging.error(f"Error inserting or updating data in the database: {e}")
        return None
    finally:
        connection.close()
        return pilgrims_count, date
