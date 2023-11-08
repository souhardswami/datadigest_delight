import mysql.connector

# Define your database connection parameters
db_config = {
    "host": "localhost",
    "port": "3310",
    "user": "root",
    "password": "root",
    "database": "store",
}


connection = mysql.connector.connect(**db_config)