# Extract: Process to pull data from Source system
# Load: Process to write data to a destination system

# Common upstream & downstream systems
# OLTP Databases: Postgres, MySQL, sqlite3, etc
# OLAP Databases: Snowflake, BigQuery, Clickhouse, DuckDB, etc
# Cloud data storage: AWS S3, GCP Cloud Store, Minio, etc
# Queue systems: Kafka, Redpanda, etc
# API
# Local disk: csv, excel, json, xml files
# SFTP\FTP server

# Databases: When reading or writing to a database we use a database driver. Database drivers are libraries that we can use to read or write to a database.
# Question: How do you read data from a sqlite3 database and write to a DuckDB database?
# Hint: Look at importing the database libraries for sqlite3 and duckdb and create connections to talk to the respective databases
import sqlite3

# Fetch data from the SQLite Customer table
conn = sqlite3.connect('tpch.db')
customers = conn.execute("SELECT * FROM Customer")

# Insert data into the DuckDB Customer table
import duckdb

duck_conn = duckdb.connect('tpch.db')

insert = "INSERT INTO employees VALUES (?, ?, ?, ?)"

duck_conn.executemany(insert, customers)


# Commit and close the connections
# Commit tells the DB connection to send the data to the database and commit it, if you don't commit the data will not be inserted
duck_conn.commit()

# We should close the connection, as DB connections are expensive
conn.close()
duck_conn.close()

# Cloud storage
# Question: How do you read data from the S3 location given below and write the data to a DuckDB database?
# Data source: https://docs.opendata.aws/noaa-ghcn-pds/readme.html station data at path "csv.gz/by_station/ASN00002022.csv.gz"
# Hint: Use boto3 client with UNSIGNED config to access the S3 bucket
# Hint: The data will be zipped you have to unzip it and decode it to utf-8


import duckdb
import boto3
from botocore import UNSIGNED
from botocore.client import Config

from io import BytesIO
import gzip

# AWS S3 bucket and file details
bucket_name = "noaa-ghcn-pds"
file_key = "csv.gz/by_station/ASN00002022.csv.gz"
# Create a boto3 client with anonymous access
s3_client = boto3.client('s3', config=Config(signature_version='UNSIGNED'))

# Download the CSV file from S3
file_obj = BytesIO()
data = s3_client.download_fileobj(bucket_name, file_key, file_obj)
file_obj.seek(0)

# Decompress the gzip data
csv = gzip.decompress(data).decode('utf-8')

# Read the CSV file using csv.reader
csv_reader = csv.reader(csv)
data = list(csv_reader)

# Connect to the DuckDB database (assume WeatherData table exists)
duck_conn = duckdb.connect('WeatherData.db')

# Insert data into the DuckDB WeatherData table
insert_query = """
INSERT INTO WeatherData (id, date, element, value, m_flag, q_flag, s_flag, obs_time)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

duck_conn.executemany(insert_query, data[:100000])

duck_conn.commit()
duck_conn.close()


# API
# Question: How do you read data from the CoinCap API given below and write the data to a DuckDB database?
# URL: "https://api.coincap.io/v2/exchanges"
# Hint: use requests library

import requests
import duckdb

# Define the API endpoint
url = "https://api.coincap.io/v2/exchanges"

# Fetch data from the CoinCap API
response = requests.get(url)
data = response.json()

# Connect to the DuckDB database
duck_conn = duckdb.connect('coindb.db')

# Insert data into the DuckDB Exchanges table
insert_query = """
INSERT INTO Exchanges (id, name, rank, percentTotalVolume, volumeUsd, tradingPairs, socket, exchangeUrl, updated)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

# Prepare data for insertion
# Hint: Ensure that the data types of the data to be inserted is compatible with DuckDBs data column types in ./setup_db.py

# I conceded defeat on this one, so it's copied from the solutions.
insert_data = [
    (
        exchange["exchangeId"],
        exchange["name"],
        int(exchange["rank"]),
        (
            float(exchange["percentTotalVolume"])
            if exchange["percentTotalVolume"]
            else None
        ),
        float(exchange["volumeUsd"]) if exchange["volumeUsd"] else None,
        exchange["tradingPairs"],
        exchange["socket"],
        exchange["exchangeUrl"],
        int(exchange["updated"]),
    )
    for exchange in data
]

duck_conn.executemany(insert_query, insert_data)

# Commit and close the connection
duck_conn.commit()
duck_conn.close()

# Local disk
# Question: How do you read a CSV file from local disk and write it to a database?
# Look up open function with csvreader for python

import csv

with open ('data.csv', 'r', newline='') as csvfile:
    csvreader = csv.reader(csvfile)
    next(csvreader)
    for row in csvreader:
        print(row)


# Web scraping
# Questions: Use beatiful soup to scrape the below website and print all the links in that website
# URL of the website to scrape
url = 'https://example.com'

import requests
from bs4 import BeautifulSoup

response = requests.get(url)

soup = BeautifulSoup(response.content, 'html.parser')

links = []

for a_tag in soup.find_all('a',href=True):
    links.append(a_tag['href'])

for link in links:
    print(link)