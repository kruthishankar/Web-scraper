import asyncio
from mongo_database.mongo import MongoDBManagement
from tools.tool import rand_proxies
from scrapers.scraper import Amazon
import time
import pandas as pd
from sqlalchemy import create_engine
import sqlite3
import csv
from mysql_database.mysql import createTable, runQueries, process_query_frontend
import os
import glob

from flask_cors import CORS
import traceback
mongo_manager = MongoDBManagement()
csv_directory = os.path.dirname(__file__)

def combine_stats_tables(csv_directory):
    # Find all CSV files with a pattern matching your filenames
    mongo_csv_files = glob.glob(os.path.join(csv_directory, 'mongodb_stats_*.csv'))
    mysql_csv_files = glob.glob(os.path.join(csv_directory, 'mysql_stats_*.csv'))

    # Sort the files by timestamp (assumes filenames end with a timestamp)
    mongo_csv_files.sort()
    mysql_csv_files.sort()

    # Check if the latest MongoDB CSV file exists
    if mongo_csv_files:
        latest_mongo_csv = mongo_csv_files[-1]
        mongo_df = pd.read_csv(latest_mongo_csv)
    else:
        # If MongoDB CSV doesn't exist, create an empty DataFrame
        mongo_df = pd.DataFrame(columns=['Indexing Type', 'Columns', 'Execution Time(s)', 'Rows Returned', 'Space Utilization'])

    # Check if the latest MySQL CSV file exists
    if mysql_csv_files:
        latest_mysql_csv = mysql_csv_files[-1]
        mysql_df = pd.read_csv(latest_mysql_csv)
    else:
        # If MySQL CSV doesn't exist, create an empty DataFrame
        mysql_df = pd.DataFrame(columns=['Indexing Type', 'Columns', 'Execution Time(s)', 'Rows Returned', 'Space Utilization'])

    # Add header rows to separate the tables
    mongo_df = pd.concat([pd.Series(['MongoDB Stats', '', '', '', '']), mongo_df])
    mysql_df = pd.concat([pd.Series(['MySQL Stats', '', '', '', '']), mysql_df])

    # Combine MongoDB and MySQL DataFrames
    combined_df = pd.concat([mongo_df, mysql_df], axis=0, ignore_index=True)
    
    return combined_df


class Scraper:
    def __init__(self, base_url):
        self.base_url = base_url

    async def scraping(self):
        start_scraping_time = time.time()
        status = await Amazon(self.base_url, None).status()

        if status == 503:
            print("503 response. Please try again later.")
            return
                # Type True if you want to export to CSV and avoid MongoDB
                
                # Type True if you want to use proxy:
        proxy = False
        if proxy:
            proxy_url = f"http://{rand_proxies()}"
        else:
            proxy_url = None
        try:
            amazon = Amazon(self.base_url, proxy_url)
            df = await amazon.export_csv()
                
            if df.empty:
                print("Sorry, couldn't scrape any data into sql data.")
            else:
                createTable(df)

            await mongo_manager.export_to_mongo(self.base_url, proxy_url)
        except Exception as e:
            print(f"An error occurred: {e}")
        end_scraping_time = time.time()
        print(f"Scraping completed in {end_scraping_time - start_scraping_time} seconds.")
        print(f"Scraping {self.base_url}")


if __name__ == '__main__':
    

    async def main():
        
        
        

        print("--------------Welcome to the Amazon scraper and DB interface!--------------")

        # Ask the user for the base Amazon URL
        base_url = input("Enter the base Amazon URL (or press Enter for default): ")

        # Use the default URL if the user didn't provide one
        if not base_url:
            base_url = "https://www.amazon.it/s?rh=n%3A20904366031&fs=true&ref=lp_20904366031_sar"

        print("The base url is ",base_url)

        scraper = Scraper(base_url)

        # Call the scraping method using await
        await scraper.scraping()
      
            
        while True:     
            #nosql exporting
            print("Which query do you want to execute ? ")
            print("1: Search products between price range x and y ")
            print("2: Search by name and description")
            print("3: Search by rating grater than equal to x and order them in descending")
            print("4: Search by you saved grater than equal to x and order them in descending order")
            print("5: Search highest in stock values and order them by the product count in descending order")
            print("6. Exit")
            user_input = input("Enter your choice (1-6): ")

            if user_input == '6':
                print("Exiting the program.")
                break    
                
            mongo_manager.create_and_query(user_input)
            mongo_manager.close_connection()
            
        #sql exporting

            runQueries(user_input)
            # Read SQLite statistics CSV file
            # Call the function to generate the combined table
            combined_table = combine_stats_tables(csv_directory)

            # Display the combined table

    asyncio.run(main())
   
