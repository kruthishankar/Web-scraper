import pymongo
import time
import csv
import os
from tools.tool import flat, export_sheet
from scrapers.scraper import Amazon
import sys
import datetime


class MongoDBManagement:
    def __init__(self, db_uri='mongodb://localhost:27017/', db_name='amazon', collection_name='Giochi, console e accessori per Xbox Series X e S'):
        self.client = pymongo.MongoClient(db_uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    async def export_to_mongo(self, url, proxy):
        amazon = Amazon(url, proxy)
        datas = await amazon.concurrent_scraping()
        result = self.collection.insert_many(flat(datas))
        print("Data exported to MongoDB.")
        return result

    async def mongo_to_sheet(self):
        datas = list(self.collection.find({}))
        await export_sheet(datas, self.collection.name)

    async def data_by_asin(self, asin):
        query = {"ASIN": asin}
        return [doc for doc in self.collection.find(query)]
   

    # def create_index(self, collection_name, index_fields):
    #     collection = self.db[collection_name]
    #     for field in index_fields:
    #         collection.create_index([(field, pymongo.ASCENDING)])

    # def create_compound_index(self, collection_name, compound_fields):
    #     collection = self.db[collection_name]
    #     collection.create_index(compound_fields)
    def create_index(self, collection_name, index_fields):
        collection = self.db[collection_name]
        for field, order in index_fields:
            collection.create_index([(field, order)])

    def create_compound_index(self, collection_name, compound_fields):
        collection = self.db[collection_name]
        collection.create_index(compound_fields)


    def query_collection(self, collection_name, query, index_info):
        collection = self.db[collection_name]
        start_time = time.time()
        cursor = collection.find(query)
        results = list(cursor)
        execution_time = time.time() - start_time
        self.record_stats('Query Execution', index_info, execution_time, len(results))
        return results, execution_time

    def record_stats(self, indexing_type, column, query_type, execution_time, rows_returned, result_size, filename):
        data=[]
        
        with open(filename, 'a', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            # Check if the file is newly created and needs a header
            if os.path.getsize(filename) == 0:
                csvwriter.writerow(query_type)
                csvwriter.writerow(['Indexing Type', 'Columns', 'Execution Time(s)', 'Rows Returned', 'Space Utilization'])
            csvwriter.writerow([indexing_type, column, execution_time, rows_returned, result_size])
            data.append([indexing_type, column, execution_time, rows_returned, result_size])
        return [indexing_type, column, execution_time, rows_returned, result_size]




    def process_query_frontend(self, user_input, parameters):
        indexing_fields = ['Name', 'Description', 'Price', 'Rating', 'Availability']
        compound_indexes = [('Name', 'Description'), ('Price', 'Rating')]
        collection_name="Giochi, console e accessori per Xbox Series X e S"
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        # Include the timestamp in the filename
        stats_filename = f'mongodb_stats_{timestamp}.csv'
        # if not self.db[self.collection]:
        #     print("No collection found in the database.")
        #     return
        query_map = {
            '1': 'Price Range',
            '2': 'Name and Description',
            '3': 'Rating',
            '4': 'You saved',
            '5': 'In Stock'
        }
        if user_input == 'query1':
            lower_bound = float(parameters.get("lowerBound", 0))
            upper_bound = float(parameters.get("upperBound", 1000))
            query_function = lambda: self.find_products_in_price_range(lower_bound, upper_bound)
        elif user_input == 'query2':
            keyword = parameters.get("stringValue", "")
            query_function = lambda: self.search_by_name_and_description(keyword)
        elif user_input == 'query3':
            rating = float(parameters.get("numericValue", 0))
            query_function = lambda: self.search_by_rating(rating)
        elif user_input == 'query4':
            quantity = float(parameters.get("numericValue", 0))
            query_function = lambda: self.find_products_with_you_saved(quantity)
        elif user_input == 'query5':
            query_function = lambda: self.count_products_in_stock()

        start_time = time.time()
        results_no_index = query_function()
        exec_time_no_index = time.time() - start_time
        print(f"Query on without indexing took {exec_time_no_index} seconds. Results: {results_no_index}")
        self.record_stats("N/A", "N/A", query_map.get(user_input, "Unknown Query"), exec_time_no_index, len(results_no_index), sys.getsizeof(results_no_index), stats_filename)


        simple_query_data = []
        compound_query_data = []

        for field in indexing_fields:
            # Create index and query again
            self.create_index(collection_name, [(field, pymongo.ASCENDING)])
            start_time = time.time()
            results_with_index = query_function()
            exec_time_with_index = time.time() - start_time
            print(f"Query on {field} with index took {exec_time_with_index} seconds. Results: {results_with_index}")
            simple_query_data.append(self.record_stats("Simple", field, query_map.get(user_input, "Unknown Query"), exec_time_with_index, len(results_with_index), sys.getsizeof(results_with_index), stats_filename))


            
        for compound_field in compound_indexes:
            self.create_compound_index(collection_name, [(field, pymongo.ASCENDING) for field in compound_field])
            start_time = time.time()
            results_with_complexindex = query_function()
            exec_time_with_complexindex = time.time() - start_time
            compound_field_str = ', '.join(compound_field)
            print(f"Query on {compound_field} with index took {exec_time_with_complexindex} seconds. Results: {results_with_complexindex}")
            compound_query_data.append(self.record_stats("Compound", compound_field_str, query_map.get(user_input, "Unknown Query"), exec_time_with_complexindex, len(results_with_complexindex), sys.getsizeof(results_with_complexindex), stats_filename))


        combined_data = simple_query_data + compound_query_data

        return combined_data


    def create_and_query(self, user_input):
        indexing_fields = ['Name', 'Description', 'Price', 'Rating', 'Availability']
        compound_indexes = [('Name', 'Description'), ('Price', 'Rating')]
        collection_name="Giochi, console e accessori per Xbox Series X e S"
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        # Include the timestamp in the filename
        stats_filename = f'mongodb_stats_{timestamp}.csv'
        # if not self.db[self.collection]:
        #     print("No collection found in the database.")
        #     return
        query_map = {
            '1': 'Price Range',
            '2': 'Name and Description',
            '3': 'Rating',
            # Map other user inputs to their respective query descriptions
        }
        if user_input == '1':
            lower_bound = float(input("Enter the lower bound for Price: "))
            upper_bound = float(input("Enter the upper bound for Price: "))
            query_function = lambda: self.find_products_in_price_range(lower_bound, upper_bound)
        elif user_input == '2':
            keyword = input("Enter keyword for search in Name and Description: ")
            query_function = lambda: self.search_by_name_and_description(keyword)
        elif user_input == '3':
            rating = float(input("Enter minimum rating: "))
            query_function = lambda: self.search_by_rating(rating)
        elif user_input == '4':
            quantity = float(input("Enter your saved item quantity: "))
            query_function = lambda: self.find_products_with_you_saved(quantity)
        elif user_input == '5':
            query_function = lambda: self.count_products_in_stock()

        start_time = time.time()
        results_no_index = query_function()
        exec_time_no_index = time.time() - start_time
        print(f"Query on without indexing took {exec_time_no_index} seconds. Results: {results_no_index}")
        self.record_stats("N/A", "N/A", query_map.get(user_input, "Unknown Query"), exec_time_no_index, len(results_no_index), sys.getsizeof(results_no_index), stats_filename)


        

        for field in indexing_fields:
            # Create index and query again
            self.create_index(collection_name, [(field, pymongo.ASCENDING)])
            start_time = time.time()
            results_with_index = query_function()
            exec_time_with_index = time.time() - start_time
            print(f"Query on {field} with index took {exec_time_with_index} seconds. Results: {results_with_index}")
            self.record_stats("Simple", field, query_map.get(user_input, "Unknown Query"), exec_time_with_index, len(results_with_index), sys.getsizeof(results_with_index), stats_filename)


            
        for compound_field in compound_indexes:
            self.create_compound_index(collection_name, [(field, pymongo.ASCENDING) for field in compound_field])
            start_time = time.time()
            results_with_complexindex = query_function()
            exec_time_with_complexindex = time.time() - start_time
            compound_field_str = ', '.join(compound_field)
            print(f"Query on {compound_field} with index took {exec_time_with_complexindex} seconds. Results: {results_with_complexindex}")
            self.record_stats("Compound", compound_field_str, query_map.get(user_input, "Unknown Query"), exec_time_with_complexindex, len(results_with_complexindex), sys.getsizeof(results_with_complexindex), stats_filename)




    
  
    # def build_query(self, user_input, field_info):
    #     if user_input == '1':
    #         x = float(input("Enter the lower bound for Price: "))
    #         y = float(input("Enter the upper bound for Price: "))
    #         return {'Price': {'$gte': x, '$lte': y}}
    #     elif user_input == '2':
    #         keyword = input("Enter keyword for search in Name and Description: ")
    #         return {'$or': [{'Name': {'$regex': keyword, '$options': 'i'}}, {'Description': {'$regex': keyword, '$options': 'i'}}]}
    #     elif user_input == '3':
    #         x = float(input("Enter minimum rating: "))
    #         return {'Rating': {'$gte': x}}
    #     elif user_input == '4':
    #         x = float(input("Enter minimum 'You saved' value: "))
    #         return {'You saved': {'$gte': x}}
    #     elif user_input == '5':
    #         return {'Availability': 'In Stock'}
    #     else:
    #         return {}
        

    def find_products_in_price_range(self, lower_bound, upper_bound):
        """
        Finds products within a specified price range.

        Args:
        lower_bound (float): The lower bound of the price range.
        upper_bound (float): The upper bound of the price range.

        Returns:
        list: A list of products within the specified price range.
        """
        # Assuming 'Price' field is a string with a comma as a decimal separator
       
        pipeline = [
            {
                "$addFields": {
                    "convertedPrice": {
                        "$convert": {
                            "input": { "$replaceAll": { "input": "$Price", "find": ",", "replacement": "." } },
                            "to": "double",
                            "onError": "Error",
                            "onNull": 0
                        }
                    }
                }
            },
            {
                "$match": {
                    "convertedPrice": { "$gte": lower_bound, "$lte": upper_bound }
                }
            }
        ]

        results = self.collection.aggregate(pipeline)
        return list(results)
    
    def search_by_name_and_description(self, keyword):
       
        query = {'$or': [{'Name': {'$regex': keyword, '$options': 'i'}}, {'Description': {'$regex': keyword, '$options': 'i'}}]}
        results = self.collection.find(query)
        return list(results)
    
    def search_by_rating(self, rating):
        """
        Search for products with a rating greater than or equal to 'x',
        order the results in descending order, and return the first 'limit' entries.
        """
        
        query = {"Rating": {"$gte": rating}}
        sort_order = [("Rating", pymongo.DESCENDING)]

        try:
            results = list(self.collection.find(query).sort(sort_order).limit(10))
            return results
        except Exception as e:
            print(f"An error occurred: {e}")
            return []
        
    def find_products_with_you_saved(self, you_saved):
        """
        Find products with 'You saved' greater than or equal to a specified quantity.

        Args:
            you_saved (float): The minimum 'You saved' quantity to filter by.

        Returns:
            list: A list of products with 'You saved' greater than or equal to the specified quantity.
        """
        query = {"You saved": {"$gte": you_saved}}
        results = self.collection.find(query).sort([("You saved", pymongo.DESCENDING)])
        return list(results)
    
    def count_products_in_stock(self):
        """
        Count the number of products with 'Availability' equal to 'In Stock'.

        Returns:
            int: The count of products with 'Availability' equal to 'In Stock'.
        """
        query = {"Availability": "In Stock"}
        count = self.collection.count_documents(query)
        return count



    def close_connection(self):
        self.client.close()