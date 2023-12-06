# Web-scraper

Welcome to WebScraper, is an interactive web scraper solution designed to extract information from the Amazon website and store it in a MongoDB databse and MySQL databases. The code for the scraper, which automates the scraping process by using the Requests and BeautifulSoup libraries, is contained in this repository. Asyncio concurrency is another tool the scraper uses to effectively retrieve thousands of data points from the website. This project is an attempt to understand the relational and non-relational database systems and their indexing techniques. There is also an analysis of their efficiency based on their performances. 


# Install necessary requirments:
Installing a virtual environment first, then the prerequisites, is always a smart idea:

python.exe -m venv environmentname

source environmentname/bin/activate

# Install necessary requirements:
 pip install -r requirements.txt

To run the script, go to terminal and type:
 python3 main.py

To run the UI, go to terminal and type:
 python3 app.py

 See the UI up on localhost port 5000 (default port for Flask app):
 http://127.0.0.1:5000

Features
Upon executing the program, the scraper commences its operation by extracting the following fields and storing the required product information in Mongo databases.
Product
Asin
Description
Breakdown
Price
Deal Price
You Saved
Rating
Rating count
Availability
Hyperlink
Store

Database Integration
The data is stored in both MongoDB and MySQL, allowing you to store the scraped data in a database for further analysis or usage. The scraper can now save the scraped data directly to a MongoDB and MySQL databases.

To enable MongoDB integration, you need to follow these steps:
Make sure you have MongoDB installed and running on your machine or a remote server.
Install the pymongo package by running the following command:
python pip install pymongo

In the script or module where you handle the scraping and data extraction, import the pymongo With the MongoDB integration, you can easily query and retrieve the scraped data from the database, perform analytics, or use it for other purposes.

# Task distribution:
**Kruthi:**
- MongoDB integration.
- Integration of Flask application.
- Testing various queries and integration of indexing in mongo and mysql databases.
- Connection of frontend to backend.
- Compiling the project report.
- Uploading of the project on GitHub.

**Ketan:**
- Design of queries.
- Design of databases.
- Integration of MySQL database.
- Integration of Flask application.
- Testing various queries and integration of indexing in mysql database.
- Creation of index.html.

**Devi:**
- Compiling the project report, literature survey and references.
- Design of the scraper for Amazon websites.
- Testing of the scraper.
- Design of the database schema.
- Documentation and reporting of the project.


