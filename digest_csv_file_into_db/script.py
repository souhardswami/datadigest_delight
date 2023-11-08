import pandas as pd
import mysql.connector
from dateutil import parser

# Define your database connection parameters
db_config = {
    "host": "localhost",
    "port": 3310,
    "user": "root",
    "password": "root",
    "database": "store",
}

def ingest_csv_to_mysql(csv_filename, table_name):
    # Read the CSV data in chunks
    chunk_size = 10000  # Adjust the chunk size as needed
    chunks = pd.read_csv(csv_filename, chunksize=chunk_size)

    # Establish a database connection
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    
    count = 0

    # Iterate through the data chunks
    for chunk in chunks:
        
        # Use pd.to_datetime to efficiently parse timestamps
        if 'timestamp_utc' in chunk:
            chunk['timestamp_utc'] = pd.to_datetime(chunk['timestamp_utc'])
            
            # Convert the Python datetime objects to MySQL DATETIME strings
            chunk['timestamp_utc'] = chunk['timestamp_utc'].dt.strftime('%Y-%m-%d %H:%M:%S')

        columns = ', '.join(chunk.columns)
        values = [tuple(row) for _, row in chunk.iterrows()]
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({', '.join(['%s'] * len(chunk.columns))})"
        cursor.executemany(insert_query, values)

        if chunk.index.max() % 10000 == 0:
            print(f"Inserted {chunk.index.max() + 1} rows.")
        
        print ("done -> count", str(count))
        count +=1

    # Commit the changes and close the database connection
    conn.commit()
    conn.close()


if __name__ == "__main__":
    # source file -> db table
    ingest_csv_to_mysql('data_files/store_status.csv', 'store_status')
    ingest_csv_to_mysql('data_files/store_location.csv', 'timezones')
    ingest_csv_to_mysql('data_files/business_hours.csv', 'business_hours')
