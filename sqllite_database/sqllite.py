from sqlalchemy import create_engine
import sqlite3
import csv
import time
import datetime
import pandas as pd

table_name = 'amazon_sql'
indexing_columns = ['Name', 'Description', 'Price', 'Rating', 'Availability']
compound_indexes = [('Name', 'Description'),
                    ('Price', 'Rating'), ('Availability', 'Store')]


def createTable(df):
    database_type = 'sqlite'  # or 'mysql', 'postgresql', etc.
    user = 'your_username'
    password = 'your_password'
    host = 'your_host'
    port = 'your_port'
    database_name = 'your_database_name'

    if database_type == 'sqlite':
        engine = create_engine(f'sqlite:///{database_name}.db')
    else:
        engine = create_engine(
            f'{database_type}://{user}:{password}@{host}:{port}/{database_name}')

    df.to_sql(table_name, engine, index=False, if_exists='replace')


def runQueries(user_input):
    connection = sqlite3.connect('your_database_name.db')
    cursor = connection.cursor()
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    csv_file = open(f'mysql_stats_{timestamp}.csv', 'w', newline='')
    csv_writer = csv.writer(csv_file)

    def create_index(columns):
        index_name = "_".join(columns)
        cursor.execute(
            f"CREATE INDEX IF NOT EXISTS {index_name}_index ON {table_name} ({', '.join(columns)});")
        cursor.execute("PRAGMA page_count;")
        total_pages = cursor.fetchone()[0]
        cursor.execute("PRAGMA page_size;")
        page_size = cursor.fetchone()[0]
        space_utilization_kb = total_pages * (page_size / 1024.0)
        return space_utilization_kb

    if user_input == '1':
        lower_bound = float(input("Enter the lower bound for Price: "))
        upper_bound = float(input("Enter the upper bound for Price: "))
        query = f"SELECT * FROM {table_name} WHERE Price BETWEEN {lower_bound} AND {upper_bound};"
    elif user_input == '2':
        keyword = input("Enter keyword for search in Name and Description: ")
        query = f"SELECT * FROM {table_name} WHERE name LIKE '%{keyword}%'"
    elif user_input == '3':
        rating = float(input("Enter minimum rating: "))
        query = f"SELECT * FROM {table_name} WHERE 'Rating count' > {rating};"
    elif user_input == '4':
        you_saved = float(input("Enter your saved item quantity: "))
        query = f"SELECT * FROM {table_name} WHERE 'You saved' >= {you_saved} ORDER BY 'You saved' DESC;"
    elif user_input == '5':
        query = f"SELECT COUNT(*) AS ProductCount FROM {table_name} WHERE Availability = 'In Stock';"

    csv_writer.writerow([query, ' ',
                        ' ', ' ', ' '])
    csv_writer.writerow(['Indexing Type', 'Columns',
                        'Execution Time (s)', 'Rows Returned', 'Space Utilization'])
    # For base case
    start_time = time.time()
    cursor.execute(query)
    execution_time = time.time() - start_time
    rows = cursor.fetchall()
    cursor.execute("PRAGMA page_count;")
    total_pages = cursor.fetchone()[0]
    cursor.execute("PRAGMA page_size;")
    page_size = cursor.fetchone()[0]
    space = total_pages * (page_size / 1024.0)
    csv_writer.writerow(
        ['Base', 'N/A', execution_time, len(rows), space])

    # For simple indexing
    for indexing_column in indexing_columns:
        space = create_index([indexing_column])
        start_time = time.time()
        cursor.execute(query)
        execution_time = time.time() - start_time
        rows = cursor.fetchall()
        # cursor.execute(f"DROP INDEX {indexing_column} ON {table_name}")
        # connection.commit()
        csv_writer.writerow(
            ['Individual', indexing_column, execution_time, len(rows), space])

    # For complex indexing
    for indexing_column in compound_indexes:
        space = create_index(indexing_column)
        start_time = time.time()
        cursor.execute(query)
        execution_time = time.time() - start_time
        rows = cursor.fetchall()
        # cursor.execute(f"DROP INDEX {indexing_column} ON {table_name}")
        # connection.commit()
        csv_writer.writerow(
            ['Complex', indexing_column, execution_time, len(rows), space])

    csv_file.close()
    connection.close()

def create_index(cursor, columns):
    index_name = "_".join(columns)
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS {index_name}_index ON {table_name} ({', '.join(columns)});")
    cursor.execute("PRAGMA page_count;")
    total_pages = cursor.fetchone()[0]
    cursor.execute("PRAGMA page_size;")
    page_size = cursor.fetchone()[0]
    space_utilization_kb = total_pages * (page_size / 1024.0)
    return space_utilization_kb

def process_query_frontend(user_input, parameters):
    

    connection = sqlite3.connect('your_database_name.db')
    cursor = connection.cursor()

    timestamp = time.strftime("%Y%m%d%H%M%S")
    csv_filename = f'sql_query_{user_input}_results_{timestamp}.csv'

    with open(csv_filename, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)

        if user_input == 'query1':
            lower_bound = int(parameters.get("lowerBound", 0))
            upper_bound = int(parameters.get("upperBound", 1000))
            query = f"SELECT * FROM {table_name} WHERE Price BETWEEN {lower_bound} AND {upper_bound};"
        elif user_input == 'query2':
            string_value = parameters.get("stringValue", "")
            query = f"SELECT * FROM {table_name} WHERE name LIKE '%{string_value}%'"
        elif user_input == 'query3':
            numeric_value = int(parameters.get("numericValue", 0))
            query = f"SELECT * FROM {table_name} WHERE 'Rating count' > {numeric_value};"
        elif user_input == 'query4':
            numeric_value = int(parameters.get("numericValue", 0))
            query = f"SELECT * FROM {table_name} WHERE 'You saved' >= {numeric_value} ORDER BY 'You saved' DESC;"
        elif user_input == 'query5':
            query = f"SELECT COUNT(*) AS ProductCount FROM {table_name} WHERE Availability = 'In Stock';"

        
        # For base case
        start_time = time.time()
        cursor.execute(query)
        execution_time = time.time() - start_time
        rows = cursor.fetchall()
        cursor.execute("PRAGMA page_count;")
        total_pages = cursor.fetchone()[0]
        cursor.execute("PRAGMA page_size;")
        page_size = cursor.fetchone()[0]
        space = total_pages * (page_size / 1024.0)
        csv_writer.writerow(
            ['Base', 'N/A', execution_time, len(rows), space])

        # For simple indexing
        for indexing_column in indexing_columns:
            space = create_index(cursor, [indexing_column])
            start_time = time.time()
            cursor.execute(query)
            execution_time = time.time() - start_time
            rows = cursor.fetchall()
            # cursor.execute(f"DROP INDEX {indexing_column} ON {table_name}")
            # connection.commit()
            csv_writer.writerow(
                ['Individual', indexing_column, execution_time, len(rows), space])

        # For complex indexing
        for indexing_column in compound_indexes:
            space = create_index(cursor, indexing_column)
            start_time = time.time()
            cursor.execute(query)
            execution_time = time.time() - start_time
            rows = cursor.fetchall()
            # cursor.execute(f"DROP INDEX {indexing_column} ON {table_name}")
            # connection.commit()
            csv_writer.writerow(
                ['Complex', indexing_column, execution_time, len(rows), space])

        
    
    try:
        df = pd.read_csv(csv_filename)
        mysql_data = df.values.tolist()
        return mysql_data
    except Exception as e:
        print(f"An error occurred while reading the CSV file: {e}")
        return []
    connection.close() 
