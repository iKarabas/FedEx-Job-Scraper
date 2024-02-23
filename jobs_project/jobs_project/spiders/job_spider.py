import json
import scrapy
from jobs_project.items import JobItem
import os
import redis
import psycopg2

# Recursively flatten a nested dictionary, also name the 
# keys with respect to their position to avoid aliasing
def flatten_dict(data, parent_key='', sep='_'):
    flattened_data = {}
    for key, value in data.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            flattened_data.update(flatten_dict(value, new_key, sep=sep))
        else:
            flattened_data[new_key] = value          
    return flattened_data

class JobSpider(scrapy.Spider):
    name = 'job_spider'
    base_url = 'https://careers.fedex.com/api/jobs?page={}&sortBy=relevance&descending=false&internal=false&deviceId=undefined&domain=fedex.jibeapply.com'

    custom_settings = {
        'ITEM_PIPELINES': {
            'jobs_project.pipelines.PostgreSQLMongoDBPipeline': 300,
        },
    }

    allowed_keys = set(JobItem.fields.keys())  # Get the keys defined in JobItem

    # Redis connection settings
    redis_host = 'redis' 
    redis_port = 6379
    redis_db = 0
    redis_key_prefix = 'job_spider_cache'

    # Initialize common parameters
    # Add the first file again to the files list
    # so that we can test the redis duplication avoidance 
    def __init__(self, *args, **kwargs):
        super(JobSpider, self).__init__(*args, **kwargs)
        self.log("Starting the spider.")
        self.page_number = 1
        
        # Redis key prefix for storing job_identifier values
        self.redis_key_prefix_identifiers = 'job_identifiers'

        # Redis key prefix for caching
        self.redis_key_prefix_cache = 'job_cache'

        # Initialize Redis client for job identifiers
        self.redis_identifiers = redis.StrictRedis(host=self.redis_host, port=self.redis_port, db=self.redis_db)

        # Initialize Redis client for caching
        self.redis_cache = redis.StrictRedis(host=self.redis_host, port=self.redis_port, db=self.redis_db)

        # Load job identifiers from database to Redis
        self.load_identifiers_from_database()

    def load_identifiers_from_database(self):
        # Fetch all job identifiers from the PostgreSQL database
        select_identifiers_query = """
            SELECT job_identifier FROM {table_name}
        """.format(table_name=os.getenv('POSTGRES_TABLE_NAME'))

        try:
            # Establish a separate connection to the database to fetch identifiers
            connection = psycopg2.connect(
                host=os.getenv('POSTGRES_HOST'),
                port=int(os.getenv('POSTGRES_PORT')),
                database=os.getenv('POSTGRES_DB'),
                user=os.getenv('POSTGRES_USER'),
                password=os.getenv('POSTGRES_PASSWORD')
            )
            cursor = connection.cursor()

            cursor.execute(select_identifiers_query)
            identifiers = [row[0] for row in cursor.fetchall()]

            # Store identifiers in Redis set with initial status 'false' and key prefix
            for identifier in identifiers:
                redis_key = f"{self.redis_key_prefix_identifiers}:{identifier}"
                self.redis_identifiers.set(redis_key, 'false')

            self.log(f"Loaded job identifiers from the database to Redis.")
        except psycopg2.Error as e:
            self.log(f"Failed to retrieve job identifiers from PostgreSQL. Error: {e}")
        finally:
            # Close the cursor and connection
            cursor.close()
            connection.close()


    
    def start_requests(self):
        url = self.base_url.format(self.page_number)
        yield scrapy.Request(url=url, callback=self.parse_json_response)

    def parse_json_response(self, response):
        try:
            job_data = json.loads(response.text)
            jobs = job_data.get('jobs', [])
            # if there are no jobs, that means scraping is done. First delete all the inactive jobs 
            # from databases and then stop the spider
            if not jobs:
                self.delete_inactive_jobs_from_databases()
                self.log("No more data to scrape. Stopping the spider.")
                return
            
            for job in jobs: 
                req_id = job.get('data', {}).get('req_id')
                title = job.get('data', {}).get('title')
                street_address = job.get('data', {}).get('street_address')
                # Concatenate title and street_address to create a unique identifier
                identifier = f"{req_id}_{title}_{street_address}"
                # Since the job is still on the website , mark it as active
                self.mark_item_as_active(identifier)
                # Item is not in the cache, so forward it to the storing phase
                if not (self.is_item_cached(identifier) or self.is_item_in_the_database(identifier)):
                    item = JobItem()
                    flattened_data = flatten_dict(job["data"], parent_key='', sep='_')
                    filtered_data = {key: flattened_data[key] for key in flattened_data if key in self.allowed_keys}
                    filtered_data["job_identifier"] = identifier
                    item.update(filtered_data)
                    yield item
                    self.cache_item(identifier)
        except json.JSONDecodeError:
            self.log(f"Failed to decode JSON from response: {response.url}")

        # Follow the next page
        self.page_number += 1
        next_page_url = self.base_url.format(self.page_number)
        yield scrapy.Request(url=next_page_url, callback=self.parse_json_response)
    
    def mark_item_as_active(self, identifier):
        redis_key = f"{self.redis_key_prefix_identifiers}:{identifier}"
        # The key exists , mark its activation status as true
        if self.redis_identifiers.exists(redis_key):
            self.redis_identifiers.set(redis_key, 'true')    
        
    def is_item_in_the_database(self,identifier):
        # check if the item already in the database
        redis_key = f"{self.redis_key_prefix_identifiers}:{identifier}"
        return self.redis_identifiers.exists(redis_key)
        
    def is_item_cached(self, identifier):
        # Check if the identifier is present in Redis cache
        cache_key = f"{self.redis_key_prefix}:{identifier}"
        return self.redis_cache.exists(cache_key)
    
    def cache_item(self, identifier):
        # Cache the identifier in Redis
        cache_key = f"{self.redis_key_prefix}:{identifier}"
        self.redis_cache.set(cache_key, 1)

    def delete_inactive_jobs_from_databases(self):
        # Get all keys from Redis identifiers set where the value is 'false'
        false_identifiers = self.get_false_identifiers()
        
        # Delete elements from Redis based on keys in false_identifiers
        for identifier in false_identifiers:
            key_to_delete = f"{self.redis_key_prefix}:{identifier}"
            self.redis_identifier.delete(key_to_delete)
        
        # Delete items from PostgreSQL
        self.delete_inactive_jobs_from_postgresql(false_identifiers)

        # Delete items from MongoDB
        self.delete_inactive_jobs_from_mongodb(false_identifiers)
   
    def get_false_identifiers(self):
        cursor = '0'
        false_identifiers = []

        while cursor != '0':
            cursor, keys = self.redis_identifiers.scan(cursor, match=f"{self.redis_key_prefix_identifiers}:*", count=1000)
            for key in keys:
                identifier = key.decode('utf-8').split(":")[-1]
                value = self.redis_identifiers.get(key)
                if value == b'false':
                    false_identifiers.append(identifier)

        return false_identifiers
    

    
    
    def delete_inactive_jobs_from_postgresql(self, false_identifiers):
        # Check if there are any false identifiers
        if not false_identifiers:
            return

        # PostgreSQL connection settings
        postgresql_settings = {
            'host': self.settings.get('POSTGRES_HOST'),
            'port': self.settings.get('POSTGRES_PORT'),
            'database': self.settings.get('POSTGRES_DB'),
            'user': self.settings.get('POSTGRES_USER'),
            'password': self.settings.get('POSTGRES_PASSWORD'),
        }

        try:
            # Connect to PostgreSQL
            connection = psycopg2.connect(**postgresql_settings)
            cursor = connection.cursor()

            # Create a SQL query to delete items with false identifiers
            delete_query = """
                DELETE FROM raw_table
                WHERE job_identifier = ANY(%s)
            """

            # Execute the query
            cursor.execute(delete_query, (false_identifiers,))
            connection.commit()
        except psycopg2.Error as e:
            connection.rollback()
            self.log(f"Failed to delete items from PostgreSQL. Error: {e}")
        finally:
            # Close the connection
            if connection:
                connection.close()

    def delete_inactive_jobs_from_mongodb(self, false_identifiers):
        # Check if there are any false identifiers
        if not false_identifiers:
            return

        # MongoDB connection settings
        mongo_settings = {
            'host': self.settings.get('MONGO_HOST'),
            'port': self.settings.get('MONGO_PORT'),
            'database': self.settings.get('MONGO_DB'),
            'collection': self.settings.get('MONGO_COLLECTION_NAME'),
            'user': self.settings.get('MONGO_USERNAME'),
            'password': self.settings.get('MONGO_PASSWORD'),
        }

        try:
            # Connect to MongoDB
            mongo_uri = f"mongodb://{mongo_settings['user']}:{mongo_settings['password']}@{mongo_settings['host']}:{mongo_settings['port']}"
            mongo_client = MongoClient(mongo_uri)
            mongo_db = mongo_client[mongo_settings['database']]
            mongo_collection = mongo_db[mongo_settings['collection']]

            # Create a filter to match items with false identifiers
            filter_query = {'job_identifier': {'$in': false_identifiers}}

            # Delete items from MongoDB
            mongo_collection.delete_many(filter_query)
        except Exception as e:
            self.log(f"Failed to delete items from MongoDB. Error: {e}")
        finally:
            # Close the MongoDB connection
            if mongo_client:
                mongo_client.close()