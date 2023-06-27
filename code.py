import os
import json
import pandas as pd
import mysql.connector

root_dir = '/Users/veera/Desktop/guvi_project'
database_name = 'phonepe_1'
table_name = 'aggregated_transaction'

# Function to extract data from JSON files
def extract_data(json_file):
    with open(json_file) as f:
        data = json.load(f)
        return data['data']

# Extract Aggregated Transaction Data
aggregated_transaction_data = []
aggregated_transaction_dir = os.path.join(root_dir, 'data/aggregated/transaction')
for country_dir in os.listdir(aggregated_transaction_dir):
    country_path = os.path.join(aggregated_transaction_dir, country_dir)
    if os.path.isdir(country_path):
        for state_dir in os.listdir(country_path):
            state_path = os.path.join(country_path, state_dir)
            if os.path.isdir(state_path):
                for year_dir in os.listdir(state_path):
                    year_path = os.path.join(state_path, year_dir)
                    if os.path.isdir(year_path):
                        for json_file in os.listdir(year_path):
                            if json_file.endswith('.json'):
                                json_path = os.path.join(year_path, json_file)
                                data = extract_data(json_path)
                                for transaction_data in data['transactionData']:
                                    row_dict = {
                                        'name': transaction_data['name'],
                                        'type': transaction_data['paymentInstruments'][0]['type'],
                                        'count': transaction_data['paymentInstruments'][0]['count'],
                                        'amount': transaction_data['paymentInstruments'][0]['amount'],
                                        'state': state_dir,
                                        'year': year_dir,
                                        'quarter': int(json_file.split('.')[0])
                                    }
                                    aggregated_transaction_data.append(row_dict)

# Convert to DataFrame
aggregated_transaction_df = pd.DataFrame(aggregated_transaction_data)

# Establish connection to MySQL
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='mysql@123',
    database='phonepe_1'
)

# Create a cursor object
cursor = conn.cursor()

# Create the table in the database
create_table_query = '''
CREATE TABLE IF NOT EXISTS {} (
    name VARCHAR(100),
    type VARCHAR(100),
    count BIGINT,
    amount FLOAT,
    state VARCHAR(100),
    year INT,
    quarter INT
)
'''.format(table_name)
cursor.execute(create_table_query)

# Insert data into the table
for _, row in aggregated_transaction_df.iterrows():
    insert_query = '''
    INSERT INTO {} (name, type, count, amount, state, year, quarter)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    '''.format(table_name)
    values = (
        row['name'],
        row['type'],
        row['count'],
        row['amount'],
        row['state'],
        row['year'],
        row['quarter']
    )
    cursor.execute(insert_query, values)

# Commit the changes and close the cursor and connection
conn.commit()
cursor.close()
conn.close()



import json

def extract_aggregated_user_data(json_file):
    with open(json_file) as f:
        data = json.load(f)
        if 'data' in data and 'usersByDevice' in data['data']:
            return data['data']['usersByDevice']
        else:
            return []

aggregated_user_data = []
aggregated_user_dir = os.path.join(root_dir, 'data/aggregated/user')
for country_dir in os.listdir(aggregated_user_dir):
    country_path = os.path.join(aggregated_user_dir, country_dir)
    if os.path.isdir(country_path):
        for state_dir in os.listdir(country_path):
            state_path = os.path.join(country_path, state_dir)
            if os.path.isdir(state_path):
                for year_dir in os.listdir(state_path):
                    year_path = os.path.join(state_path, year_dir)
                    if os.path.isdir(year_path):
                        for json_file in os.listdir(year_path):
                            if json_file.endswith('.json'):
                                json_path = os.path.join(year_path, json_file)
                                user_data = extract_aggregated_user_data(json_path)
                                if user_data:
                                    for user_device in user_data:
                                        row_dict = {
                                            'brand': user_device['brand'],
                                            'count': user_device['count'],
                                            'percentage': user_device['percentage'],
                                        }
                                        aggregated_user_data.append(row_dict)

# Convert to DataFrame
aggregated_user_df = pd.DataFrame(aggregated_user_data)

# Print or use the DataFrame as needed
print("Aggregated User Data:")
print(aggregated_user_df)
import mysql.connector
from mysql.connector import Error

# Establish a connection to the MySQL server
try:
    connection = mysql.connector.connect(
        host='localhost',
        database='phonepe_1',
        user='root',
        password='mysql@123'
    )
    print('Connected to MySQL database')
except Error as e:
    print('Error connecting to MySQL database:', e)

try:
    cursor = connection.cursor()

    # Create the table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS aggregated_user (
            id INT AUTO_INCREMENT PRIMARY KEY,
            brand VARCHAR(255),
            count BIGINT,
            percentage FLOAT
        )
    """)

    print('Table created successfully')
except Error as e:
    print('Error creating table:', e)


def insert_data_into_mysql(data):
    try:
        cursor = connection.cursor()
        # Assuming your table is named "aggregated_user" as in the create table statement
        columns = ', '.join(data.keys())
        values = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO aggregated_user ({columns}) VALUES ({values})"
        cursor.execute(query, tuple(data.values()))
        connection.commit()
        print('Data inserted successfully')
    except Error as e:
        print('Error inserting data:', e)


# Insert the preprocessed user data into the MySQL table
for index, row in aggregated_user_df.iterrows():
    data = {
        'brand': row['brand'],
        'count': row['count'],
        'percentage': row['percentage']
    }
    insert_data_into_mysql(data)
connection.close()

import json
import os
import pandas as pd

root_dir = '/Users/veera/Desktop/guvi_project/data/top/transaction/country/india'

# Function to extract data from JSON files
def extract_data(json_file):
    with open(json_file) as f:
        data = json.load(f)
        return data['data']

# Extract Top Transaction Data
top_transaction_data = []
for year_dir in os.listdir(root_dir):
    year_path = os.path.join(root_dir, year_dir)
    if os.path.isdir(year_path):
        for json_file in os.listdir(year_path):
            if json_file.endswith('.json'):
                json_path = os.path.join(year_path, json_file)
                data = extract_data(json_path)
                for transaction_data in data['districts']:
                    district_name = transaction_data['entityName']
                    for quarter, metric_data in enumerate(transaction_data['metric'], start=1):
                        row_dict = {
                            'state': transaction_data['entityName'],
                            'year': year_dir,
                            'quarter': quarter,
                            'type': transaction_data['metric']['type'],
                            'district': district_name,
                            'count': transaction_data['metric']['count'],
                            'amount':transaction_data['metric']['amount']
                        }
                        top_transaction_data.append(row_dict)

# Convert to DataFrame
top_transaction_df = pd.DataFrame(top_transaction_data)

# Print or use the DataFrame as needed
print("Top Transaction Data:")
print(top_transaction_df.head())

import mysql.connector
from mysql.connector import Error

# Establish a connection to the MySQL server
try:
    connection = mysql.connector.connect(
        host='localhost',
        database='phonepe_1',
        user='root',
        password='mysql@123'
    )
    print('Connected to MySQL database')
except Error as e:
    print('Error connecting to MySQL database:', e)

try:
    cursor = connection.cursor()

    # Create the table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS top_transaction (
            id INT AUTO_INCREMENT PRIMARY KEY,
            state VARCHAR(255),
            year INT,
            quarter INT,
            type VARCHAR(255),
            district VARCHAR(255),
            count BIGINT,
            amount BIGINT
        )
    """)

    print('Table created successfully')
except Error as e:
    print('Error creating table:', e)


def insert_data_into_mysql(data):
    try:
        cursor = connection.cursor()
        # Assuming your table is named "top_transaction" as in the create table statement
        columns = ', '.join(data.keys())
        values = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO top_transaction ({columns}) VALUES ({values})"
        cursor.execute(query, tuple(data.values()))
        connection.commit()
        print('Data inserted successfully')
    except Error as e:
        print('Error inserting data:', e)


# Insert the preprocessed top transaction data into the MySQL table
for index, row in top_transaction_df.iterrows():
    data = {
        'state': row['state'],
        'year': row['year'],
        'quarter': row['quarter'],
        'type': row['type'],
        'district': row['district'],
        'count': row['count'],
        'amount': row['amount']
    }
    insert_data_into_mysql(data)
connection.close()

import json
import os
import pandas as pd
import mysql.connector

# MySQL database connection configuration
host = 'localhost'
database = 'phonepe_1'
user = 'root'
password = 'mysql@123'

# Establish MySQL database connection
try:
    connection = mysql.connector.connect(
        host=host,
        database=database,
        user=user,
        password=password
    )
    print('Connected to MySQL database')
except mysql.connector.Error as error:
    print('Error connecting to MySQL database:', error)

root_dir = '/Users/veera/Desktop/guvi_project/data/map/transaction/hover/country/india'

# Function to extract data from JSON files
def extract_data(json_file):
    with open(json_file) as f:
        data = json.load(f)
        return data['data']['hoverDataList']

# Extract Mapped Transaction Data
mapped_transaction_data = []
for year_dir in os.listdir(root_dir):
    year_path = os.path.join(root_dir, year_dir)
    if os.path.isdir(year_path):
        for json_file in os.listdir(year_path):
            if json_file.endswith('.json'):
                json_path = os.path.join(year_path, json_file)
                data = extract_data(json_path)
                for user_data in data:
                    district_name = user_data['name']
                    for quarter, metric_data in enumerate(user_data['metric'], start=1):
                        row_dict = {
                            'state': 'India',  # Assuming all data is for India
                            'year': year_dir,
                            'quarter': quarter,
                            'district': district_name,
                            'transactionCount': metric_data['count'],
                            'transactionAmount':metric_data['amount']
                        }
                        mapped_transaction_data.append(row_dict)

# Convert to DataFrame
mapped_transaction_df = pd.DataFrame(mapped_transaction_data)

# Print or use the DataFrame as needed
print("Mapped Transaction Data:")
print(mapped_transaction_df.head())

# Create table in MySQL database
create_table_query = """
CREATE TABLE IF NOT EXISTS mapped_transaction (
  state VARCHAR(255),
  year INT,
  quarter INT,
  district VARCHAR(255),
  transactionCount BIGINT,
  transactionAmount BIGINT
)
"""

try:
    cursor = connection.cursor()
    cursor.execute(create_table_query)
    print('Table created successfully')
except mysql.connector.Error as error:
    print('Error creating table:', error)

# Insert the preprocessed mapped transaction data into the MySQL table
try:
    cursor = connection.cursor()
    for index, row in mapped_transaction_df.iterrows():
        data = {
            'state': row['state'],
            'year': row['year'],
            'quarter': row['quarter'],
            'district': row['district'],
            'transactionCount': row['transactionCount'],
            'transactionAmount': row['transactionAmount']
        }
        query = """
        INSERT INTO mapped_transaction (state, year, quarter, district, transactionCount,transactionAmount)
        VALUES (%(state)s, %(year)s, %(quarter)s, %(district)s, %(transactionCount)s,%(transactionAmount)s)
        """
        cursor.execute(query, data)

    connection.commit()
    print('Data inserted successfully')
except mysql.connector.Error as error:
    print('Error inserting data:', error)
finally:
    connection.close()

import json
import os
import pandas as pd
import mysql.connector

# MySQL database connection configuration
host = 'localhost'
database = 'phonepe_1'
user = 'root'
password = 'mysql@123'

# Establish MySQL database connection
try:
    connection = mysql.connector.connect(
        host=host,
        database=database,
        user=user,
        password=password
    )
    print('Connected to MySQL database')
except mysql.connector.Error as error:
    print('Error connecting to MySQL database:', error)

root_dir = '/Users/veera/Desktop/guvi_project/data/top/user/country/india'

# Function to extract data from JSON files
def extract_data(json_file):
    with open(json_file) as f:
        data = json.load(f)
        return data['data']

# Extract Top User Data
top_user_data = []
for year_dir in os.listdir(root_dir):
    year_path = os.path.join(root_dir, year_dir)
    if os.path.isdir(year_path):
        for json_file in os.listdir(year_path):
            if json_file.endswith('.json'):
                json_path = os.path.join(year_path, json_file)
                data = extract_data(json_path)
                if 'districts' in data:
                    for quarter, user_data in enumerate(data['districts'], start=1):
                        district_name = user_data['name']
                        registered_users = user_data['registeredUsers']
                        row_dict = {
                            'state': district_name,
                            'quarter': json_file[:-5],
                            'year': year_dir,
                            'registeredUsers': registered_users
                        }
                        top_user_data.append(row_dict)

# Convert to DataFrame
top_user_df = pd.DataFrame(top_user_data)

# Print or use the DataFrame as needed
print("Top User Data:")
print(top_user_df)

# Create table in MySQL database
create_table_query = """
CREATE TABLE IF NOT EXISTS top_user (
  state VARCHAR(255),
  year INT,
  quarter INT,
  registeredUsers INT
)
"""

try:
    cursor = connection.cursor()
    cursor.execute(create_table_query)
    print('Table created successfully')
except mysql.connector.Error as error:
    print('Error creating table:', error)

# Insert the Top User Data into the MySQL table
try:
    cursor = connection.cursor()
    for index, row in top_user_df.iterrows():
        data = {
            'state': row['state'],
            'year': row['year'],
            'quarter': row['quarter'],
            'registeredUsers': row['registeredUsers']
        }
        query = """
        INSERT INTO top_user (state, year, quarter, registeredUsers)
        VALUES (%(state)s, %(year)s, %(quarter)s, %(registeredUsers)s)
        """
        cursor.execute(query, data)

    connection.commit()
    print('Data inserted successfully')
except mysql.connector.Error as error:
    print('Error inserting data:', error)
finally:
    connection.close()



