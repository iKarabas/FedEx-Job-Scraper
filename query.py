import psycopg2
import csv
from pymongo import MongoClient
import os

class Postgresql:
    def __init__(self, dbname, user, password, host, port):
        self.conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        self.cur = self.conn.cursor()

    def execute_query(self, query):
        self.cur.execute(query)

    def fetch_all(self):
        return self.cur.fetchall()

    def fetch_column_names(self, table_name):
        query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';"
        self.cur.execute(query)
        return [column[0] for column in self.cur.fetchall()]

    def close_connection(self):
        self.cur.close()
        self.conn.close()

class MongoDB:
    def __init__(self,user, password, dbname, collection_name, host, port):
        self.mongo_uri = f"mongodb://{user}:{password}@{host}:{port}"
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[dbname]
        self.collection = self.db[collection_name]

    def fetch_all(self):
        return [list(document.values()) for document in self.collection.find()]

    def fetch_column_names(self):
        # Check if there is at least one document in the collection
        document = self.collection.find_one()
        if document:
            return list(document.keys())
        else:
            return []

    def close_connection(self):
        self.client.close()

if __name__ == "__main__":
    # Get the credentials using env variables 
    # PostgreSQL credentials
    pg_host = os.getenv('POSTGRES_HOST')
    pg_port = int(os.getenv('POSTGRES_PORT'))
    pg_dbname = os.getenv('POSTGRES_DB')
    pg_user = os.getenv('POSTGRES_USER')
    pg_password = os.getenv('POSTGRES_PASSWORD')
    pg_table_name = os.getenv('POSTGRES_TABLE_NAME')
    
    # MongoDB credentials
    mongo_host = os.getenv('MONGO_HOST')
    mongo_port = int(os.getenv('MONGO_PORT'))
    mongo_dbname = os.getenv('MONGO_DB')
    mongo_collection_name = os.getenv('MONGO_COLLECTION_NAME')
    mongo_user = os.getenv('MONGO_USERNAME')
    mongo_password = os.getenv('MONGO_PASSWORD')
    
    # Instantiate Database and MongoDB classes
    pg_db = Postgresql(pg_dbname, pg_user, pg_password, pg_host, pg_port)
    mongo_db = MongoDB(mongo_user, mongo_password, mongo_dbname, mongo_collection_name, mongo_host, mongo_port)

    # PostgreSQL
    pg_csv_headers = pg_db.fetch_column_names(pg_table_name)
    pg_query = f"SELECT * FROM {pg_table_name};"
    pg_db.execute_query(pg_query)
    pg_data = pg_db.fetch_all()

    # CSV file configuration for PostgreSQL
    pg_csv_filename = "output_data_pg.csv"

    # Clear existing file if it exists
    if os.path.exists(pg_csv_filename):
        os.remove(pg_csv_filename)

    with open(pg_csv_filename, 'w', newline='') as pg_csv_file:
        pg_csv_writer = csv.writer(pg_csv_file)
        pg_csv_writer.writerow(pg_csv_headers)
        pg_csv_writer.writerows(pg_data)

    print(f"PostgreSQL data has been exported to {pg_csv_filename}")

    # MongoDB
    mongo_csv_headers = mongo_db.fetch_column_names()
    mongo_data = mongo_db.fetch_all()

    # CSV file configuration for MongoDB
    mongo_csv_filename = "output_data_mongo.csv"

    # Clear existing file if it exists
    if os.path.exists(mongo_csv_filename):
        os.remove(mongo_csv_filename)

    with open(mongo_csv_filename, 'w', newline='') as mongo_csv_file:
        mongo_csv_writer = csv.writer(mongo_csv_file)
        mongo_csv_writer.writerow(mongo_csv_headers)
        mongo_csv_writer.writerows(mongo_data)

    print(f"MongoDB data has been exported to {mongo_csv_filename}")

    # Close the database connections
    pg_db.close_connection()
    mongo_db.close_connection()
