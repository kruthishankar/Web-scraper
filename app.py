import asyncio
from mongo_database.mongo import MongoDBManagement
from tools.tool import rand_proxies
from scrapers.scraper import Amazon
import time
import pandas as pd
from sqlalchemy import create_engine
import sqlite3
import csv
from sqllite_database.sqllite import createTable, runQueries, process_query_frontend
import os
import glob
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import traceback
from main import Scraper

csv_directory = os.path.dirname(__file__)
app = Flask(__name__)
CORS(app)
mongo_manager = MongoDBManagement()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/scrape', methods=['POST'])
def scrape():
    try:
        # Extract URL from the POST request
        data = request.get_json()
        url = data['url']
        print('Starting scrape for URL:', url)

        # Instantiate the scraper
        scraper = Scraper(url)

        # Since scraping is an async function, use asyncio to run it
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(scraper.scraping())

        return jsonify({"message": "Scraping completed successfully."})
    except Exception as e:
        print(f"An error occurred during scraping: {e}")
        return jsonify({"error": str(e)}), 500
        


@app.route('/evaluateQuery', methods=['POST'])
def evaluate_query():
    try:
        data = request.get_json()
        query = data.get('query', '')
        parameters = data.get('parameters', {})
 
        mongo_data = mongo_manager.process_query_frontend(query, parameters) or []
        mysql_data = process_query_frontend(query, parameters) or []

        # Combine the data from MongoDB and MySQL
        combined_data = {
            'mongoData': mongo_data,
            'mysqlData': mysql_data
        }
        # Return the combined data as a JSON response
        return jsonify(combined_data)
    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()
        return jsonify({'error': 'An error occurred during query evaluation'}), 500
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)