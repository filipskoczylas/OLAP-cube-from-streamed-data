import os
import pandas as pd
from sqlalchemy import create_engine

# Dataset used in this script: https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store/data?select=2019-Nov.csv

#TODO user has to fill this data
#----------------------Data to specify----------------------
# SQL Server connection
db_user = ''
db_password = ''
db_host = ''
db_name = ''

# Printed data
# Available dimensions: 'event_type', 'category_code', 'user_id'
dimensions = ['category_code']

# lm means last minute, made so that columns take less space
# Available measures: 'user count', 'product count',  'price', 'average price',
# 'user count lm', 'product count lm', 'price lm', 'average price lm'
measures = ['price', 'price lm', 'average price', 'average price lm']

# Connection string from user specified params
connection_string = f"mssql+pyodbc://{db_user}:{db_password}@{db_host}/{db_name}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
engine = create_engine(connection_string)

# Create hierarchy for category_code
if 'category_code' in dimensions:
    dimensions.insert(dimensions.index('category_code') + 1, 'brand')
# Determine displayed columns
display_columns = dimensions + measures

# ----------------------Functions----------------------
def fetch_data():
    query = """
    SELECT event_time, event_type, category_code, brand, product_id, price, user_id
    FROM stream_data
    """
    with engine.connect() as connection:
        return pd.read_sql(query, connection)


def create_olap_cube(df):
    # Convert event_time to datetime if not already
    df['event_time'] = pd.to_datetime(df['event_time'])

    # Define the last minute time window
    current_time = df['event_time'].max()
    last_minute_start = current_time - pd.Timedelta(minutes=1)

    # Create a OLAP cube
    olap_cube = df.groupby(dimensions).agg(
        distinct_user_count=('user_id', pd.Series.nunique),
        distinct_product_count=('product_id', pd.Series.nunique),
        total_price=('price', 'sum'),
        average_price=('price', 'mean'),

        # Last minute metrics
        distinct_user_count_last_minute=('user_id', lambda x: x[df['event_time'] > last_minute_start].nunique()),
        distinct_product_count_last_minute=('product_id', lambda x: x[df['event_time'] > last_minute_start].nunique()),
        total_price_last_minute=('price', lambda x: x[df['event_time'] > last_minute_start].sum()),
        average_price_last_minute=('price', lambda x: x[df['event_time'] > last_minute_start].mean())
    ).reset_index()

    # Rename columns
    olap_cube.rename(columns={
        'distinct_user_count': 'user count',
        'distinct_product_count': 'product count',
        'total_price': 'price',
        'average_price': 'average price',
        'distinct_user_count_last_minute': 'user count lm',
        'distinct_product_count_last_minute': 'product count lm',
        'total_price_last_minute': 'price lm',
        'average_price_last_minute': 'average price lm'
    }, inplace=True)

    return olap_cube

def clear_console():
    # Clear console for Windows
    if os.name == 'nt':
        os.system('cls')
    # Clear console for Unix-like systems (Linux, macOS)
    else:
        os.system('clear')


# ----------------------Main program----------------------

while True:
    # Fetch data from the database
    df = fetch_data()

    # Create OLAP cube
    olap_cube = create_olap_cube(df)

    # Print the OLAP cube to the console
    clear_console()
    pd.set_option('display.max_rows', None)  # Show all rows
    pd.set_option('display.max_columns', None)  # Show all columns
    pd.set_option('display.width', 1000)  # Adjust display width to print data in one line
    print(olap_cube[display_columns]) # Print selected columns

    # Wait for user to press Enter before refreshing
    input("Press Enter to refresh")

# ----------------------End----------------------
