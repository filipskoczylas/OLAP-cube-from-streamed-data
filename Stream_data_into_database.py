import pandas as pd
from streamz import Stream
import json
import time
from sqlalchemy import create_engine, text
from datetime import datetime

# Dataset used in this script: https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store/data?select=2019-Nov.csv

# ----------------------Functions----------------------

# Function to stream data
# Data is streamed based on event_time parameter from dataset to simulate real e-commerce service data stream
def generate_stream(df):
    prev_time = None
    for _, row in df.iterrows():
        current_time = row['event_time']
        if prev_time is not None:
            time_diff = (current_time - prev_time).total_seconds()
            time.sleep(time_diff)
        row['event_time'] = row['event_time'].isoformat()
        yield row.to_json()
        prev_time = current_time

# Function to process the incoming data
def process_data(data):
    data = json.loads(data)
    data['event_time'] = datetime.fromisoformat(data['event_time'])
    return data

# Function to store data into the database
def store_data(data):
    print(data)  # Print the data being inserted, used for debug
    df = pd.DataFrame([data])
    df.to_sql('stream_data', engine, if_exists='append', index=False)

# ----------------------Main program----------------------

# Reading CSV file
# TODO, user has to fill this field to match own CSV file path
file_path = ''
data = pd.read_csv(file_path)

# Convert event_time to datetime format
data['event_time'] = data['event_time'].str.replace(' UTC', '', regex=False)
data['event_time'] = pd.to_datetime(data['event_time'], format='%Y-%m-%d %H:%M:%S')

# Replace null fields (needed for OLAP cube)
data['category_code'].fillna('unknown', inplace=True)
data['brand'].fillna('unknown', inplace=True)

# Create data stream
source = Stream()

# SQL Server connection
# TODO, user has to fill this fields to match own sql server database parameters
db_user = ''
db_password = ''
db_host = ''
db_name = ''

# Connection string from user specified params
connection_string = f"mssql+pyodbc://{db_user}:{db_password}@{db_host}/{db_name}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
engine = create_engine(connection_string)

# Create table
with engine.connect() as conn:
    conn.execute(text('DROP TABLE IF EXISTS stream_data'))

    conn.execute(text('''
    CREATE TABLE stream_data (
        event_time DATETIME,
        event_type VARCHAR(255),
        product_id BIGINT,
        category_id BIGINT,
        category_code VARCHAR(255),
        brand VARCHAR(255),
        price FLOAT,
        user_id BIGINT,
        user_session VARCHAR(255)
    )
    '''))

# Update the pipeline to store the data
source.map(process_data).sink(store_data)

# Generate and send data to the stream
for data in generate_stream(data):
    source.emit(data)

# ----------------------End----------------------