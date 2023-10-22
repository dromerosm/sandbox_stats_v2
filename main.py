from flask import Flask, jsonify, request, abort
from functools import wraps
import pytz
from datetime import datetime
import logging
import hashlib

from data_fetcher import *
from database_functions import *
from config import *

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)

# recupera las templates para todas las funciones
query_last_day_template, query_year_month_template, query_any_date_template = get_queries_from_db()
logging.info("Templates retrieved...")

def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        # Check the "Auth" header
        request_api_key = request.headers.get('Auth')
        if request_api_key is None:
            logging.warning("Unauthorized access attempt - missing Authorization header")
            abort(401)  # Unauthorized
        
        # Hash the request_api_key
        request_api_key_hash = hashlib.sha256(request_api_key.encode()).hexdigest()
        
        # Compare the hashes
        if request_api_key_hash != api_key_hash:
            logging.warning(f"Unauthorized access attempt - incorrect API key: {request_api_key}")
            abort(401)  # Unauthorized
        
        return f(*args, **kwargs)
    
    return decorated



@app.route('/')
@require_auth
def home():

    madrid_tz = pytz.timezone('Europe/Madrid')
    current_time = datetime.now(madrid_tz).strftime('%Y-%m-%d %H:%M:%S %Z%z')
    # connect to mySQL to get the last day and total number of pilgrims
    last_day, total_pilgrims = get_sum_pilgrims_last_day()
    last_day_formatted = last_day.strftime('%Y-%m-%d')
    logging.info("Connection completed")
    return jsonify({"status": "success", "message": "Statistics API is running", "last_day": last_day_formatted, "total_pilgrims": total_pilgrims, "current_time": current_time})


@app.route('/update_stats_last_day')
@require_auth
def update_last_day():

    if not query_last_day_template or not query_year_month_template:
        logging.warning("Template(s) are empty, aborting update")
        return jsonify({"status": "error", "message": "Template(s) are empty, unable to update statistics"})

    try:
        logging.info("Starting LAST DAY update")
        pilgrims = update_stats_last_day(
            query_last_day_template,
            query_year_month_template)
        logging.info("Finishing LAST_DAY update")
        return jsonify({"status": "success", "message": "Last day's statistics updated", "pilgrims": pilgrims})
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return jsonify({"status": "error", "message": f"An error occurred: {str(e)}"})


@app.route('/update_stats_current_month')
@require_auth
def update_year_month():

    if not query_year_month_template:
        logging.warning("Template(s) are empty, aborting update")
        return jsonify({"status": "error", "message": "Template(s) are empty, unable to update statistics"})


    try:
        logging.info("Starting current MONTH update")
        pilgrims = update_stats_year_month(
            query_year_month=query_year_month_template, incremental=True)
        logging.info("Finishing YEAR_MONTH update")
        return jsonify({"status": "success", "message": "Statistics for the current month updated", "pilgrims": pilgrims})
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return jsonify({"status": "error", "message": f"An error occurred: {str(e)}"})


@app.route('/update_stats_by_day/', defaults={'dates': None})
@app.route('/update_stats_by_day/<dates>')
@require_auth
def update_date(dates):
    if not dates:
        return jsonify({"status": "success", "message": "No dates provided, no action taken"})
    
    if not query_last_day_template or not query_year_month_template:
        logging.warning("Template(s) are empty, aborting update")
        return jsonify({"status": "error", "message": "Template(s) are empty, unable to update statistics"})

    try:
        # Split the dates string into a list of date strings
        date_list = dates.split(',')

        formatted_dates = []
        # Optionally, validate the date format
        for date_str in date_list:
            try:
                date_obj = datetime.strptime(date_str, '%d-%m-%Y')
                formatted_date_str = date_obj.strftime('%d/%m/%Y')
                formatted_dates.append(formatted_date_str)
            except ValueError:
                return jsonify({"status": "error", "message": f"Invalid date format: {date_str}"})

        pilgrims = update_stats_date(query_any_date_template, formatted_dates)
        return jsonify({"status": "success", "message": "Statistics for the list of dates updated", "pilgrims": pilgrims})
    except Exception as e:
        return jsonify({"status": "error", "message": f"An error occurred: {str(e)}"})


@app.route('/update_now')
@require_auth
def update_now():

    # serverless function to update the stats
    try:
        logging.info("Gathering pilgrims NOW...")
        pilgrims, now = log_pilgrims_count()
        logging.info("... done!")
        return jsonify({"status": "success", "message": "Pilgrims count updated", "pilgrims": pilgrims, "now": now})
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return jsonify({"status": "error", "message": f"An error occurred: {str(e)}"})


@app.route('/now')
@require_auth
def pilgrims_now():
    try:
        last_day, total_pilgrims = get_pilgrims_last_day()
        logging.info("Connection completed")
        return jsonify({"status": "success", "message": "Number of pilgrims recorded", "date": last_day, "pilgrims": total_pilgrims})
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return jsonify({"status": "error", "message": f"An error occurred: {str(e)}"})


if __name__ == "__main__":
    app.run()
