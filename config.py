from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# API Configurations
URL_OP_PBI = os.getenv('URL_OP_PBI')
URL_OP_WEBSITE = os.getenv('URL_OP_WEBSITE')

# Other secrets
api_key_hash = os.getenv('API_KEY_HASH')

HEADERS = {
    'X-PowerBI-ResourceKey': os.getenv('X_POWERBI_RESOURCE_KEY'),
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'Content-Type': 'application/json'
}

# Database Configurations
db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_DATABASE')
}

queries_tables = [
    {"dimensions": ['d1'], "tabla": "stats_camino_"},
    {"dimensions": ['d2'], "tabla": "stats_means_"},
    {"dimensions": ['d3'], "tabla": "stats_gender_"},
    {"dimensions": ['d4'], "tabla": "stats_origin_"},
    {"dimensions": ['d5'], "tabla": "stats_country_"},
    {"dimensions": ['d6'], "tabla": "stats_motivo_"},
    {"dimensions": ['d7'], "tabla": "stats_age_"},
    {"dimensions": ['d1', 'd5', 'd4'], "tabla": "stats_camino_country_origin_"}
]

tabla_check = 'stats_camino_country_origin_last_day'
tabla_pilgrims_last_day = 'stats_pilgrims_last_day'
