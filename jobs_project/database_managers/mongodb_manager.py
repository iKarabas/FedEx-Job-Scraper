from pymongo import MongoClient
from scrapy.utils.project import get_project_settings

class MongoDBManager:
    def __init__(self):
        settings = get_project_settings()
        self.collection_name = settings.get('MONGO_COLLECTION_NAME')
        self.db_settings = {
            'host': settings.get('MONGO_HOST'),
            'port': settings.get('MONGO_PORT'),
            'database': settings.get('MONGO_DB'),
            'user': settings.get('MONGO_USERNAME'),
            'password': settings.get('MONGO_PASSWORD'),
        }
        self.mongo_client = None
        self.mongo_db = None
        self.mongo_collection = None

        # Connect to MongoDB
        self.connect()
    
    def connect(self):
        # Connect to MongoDB
        try:
            self.mongo_uri = f"mongodb://{self.db_settings['user']}:{self.db_settings['password']}@{self.db_settings['host']}:{self.db_settings['port']}"
            self.mongo_client = MongoClient(self.mongo_uri)
            self.mongo_db = self.mongo_client[self.db_settings['database']]
            self.mongo_collection = self.mongo_db[self.collection_name]
        except Exception as e:
            # Handle the connection error
            print(f"Error connecting to MongoDB: {e}")

    def insert_values(self, values):
        try:
            self.mongo_collection.insert_one(values)
        except Exception as e:
            # Handle the insertion error
            print(f"Failed to insert item into MongoDB. Error: {e}")

    def close_connection(self):
        if self.mongo_client:
            self.mongo_client.close()
