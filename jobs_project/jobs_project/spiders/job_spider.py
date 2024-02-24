import json
import scrapy
from jobs_project.items import JobItem
import os
import psycopg2
from database_managers.postgresql_manager import PostgreSQLManager
from database_managers.mongodb_manager import MongoDBManager
from database_managers.redis_manager import RedisManager

### HELPERS ####
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
    base_url = 'https://careers.fedex.com/api/jobs?page=1&sortBy=relevance&descending=false&featured=true&internal=false&deviceId=undefined&domain=fedex.jibeapply.com'

    custom_settings = {
        'ITEM_PIPELINES': {
            'jobs_project.pipelines.PostgreSQLMongoDBPipeline': 300,
        },
    }

    allowed_keys = set(JobItem.fields.keys())  # Get the keys defined in JobItem

    # Initialize common parameters
    def __init__(self, *args, **kwargs):
        super(JobSpider, self).__init__(*args, **kwargs)
        self.log("Starting the spider.")
        self.page_number = 0
        
        # Redis key prefix for storing job_identifier values
        self.key_prefix_for_identifiers = 'job_identifiers'

        # Redis key prefix for caching
        self.key_prefix_for_job_cache = 'job_cache'

        # Initialize Redis client for job identifiers
        self.redis_identifiers = RedisManager()

        # Initialize Redis client for job caching
        self.redis_cache = RedisManager()

        # Load job_identifiers from database to Redis
        self.load_identifiers_from_database()

    
    def start_requests(self):
        url = self.base_url
        # First url was for featured jobs, now change it to get the regular jobs
        self.base_url = 'https://careers.fedex.com/api/jobs?page={}&sortBy=relevance&descending=false&internal=false&deviceId=undefined&domain=fedex.jibeapply.com'
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
                
                # Since the job is still on the website , mark it as active by setting its value to true
                self.redis_identifiers.set_value_for_an_existing_key(self.key_prefix_for_identifiers, identifier,'true')
            
                # Item is neither in the cache nor in the database , so forward it to the storing phase
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
    
    def load_identifiers_from_database(self):
        postgres_manager = PostgreSQLManager()
    
        # First check whether the table exists or not
        try:
            postgres_manager.execute_query("select * from information_schema.tables where table_name=%s", (os.getenv('POSTGRES_TABLE_NAME'),))
            table_exists = bool(postgres_manager.cursor.rowcount)
        except:
            table_exists =  False    
        if table_exists:
            
            select_identifiers_query = """
                SELECT job_identifier FROM {table_name}
            """.format(table_name=os.getenv('POSTGRES_TABLE_NAME'))
            
            try:
                # Retrieve job_identifiers rows from PostgreSQL
                data = postgres_manager.fetch_values(select_identifiers_query)
                postgres_manager.close_connection()
                identifiers = [row[0] for row in data]
                
                # Store identifiers in Redis set with initial value 'false' and key prefix
                for identifier in identifiers:
                    redis_key = f"{self.key_prefix_for_identifiers}:{identifier}"
                    self.redis_identifiers.set_value(redis_key, 'false')
            except psycopg2.Error as e:
                self.log(f"Failed to retrieve job identifiers from PostgreSQL. Error: {e}")


        
    def is_item_in_the_database(self,identifier):
        # check if the item already in the redis structure for job_identifiers
        redis_key = f"{self.key_prefix_for_identifiers}:{identifier}"
        return self.redis_identifiers.exists(redis_key)
        
    def is_item_cached(self, identifier):
        # Check if the identifier is present in Redis cache for job postings
        cache_key = f"{self.key_prefix_for_job_cache}:{identifier}"
        return self.redis_cache.exists(cache_key)
    
    def cache_item(self, identifier):
        # Cache the identifier in Redis cache for job postings
        cache_key = f"{self.key_prefix_for_job_cache}:{identifier}"
        self.redis_cache.set_value(cache_key, 1)

    def delete_inactive_jobs_from_databases(self):
        # Get all keys from Redis identifiers set where the value is 'false'
        false_identifiers = self.redis_identifiers.get_keys_with_value_and_prefix(self.key_prefix_for_identifiers, 'false')
        # Delete job_identifier elements from Redis based on keys in false_identifiers
        for identifier in false_identifiers:
            key_to_delete = f"{self.key_prefix_for_identifiers}:{identifier}"
            self.redis_identifiers.delete(key_to_delete)
        
        # Delete items from PostgreSQL
        self.delete_inactive_jobs_from_postgresql(false_identifiers)

        # Delete items from MongoDB
        self.delete_inactive_jobs_from_mongodb(false_identifiers)
        
        print(f"Number of deleted closed job postings: {len(false_identifiers)}")
        print(f"Deleted job postings: {false_identifiers}")
        
        
    def delete_inactive_jobs_from_postgresql(self, false_identifiers):
        # Check if there are any false identifiers
        if not false_identifiers:
            return

        # Connect to postgres
        postgres_manager = PostgreSQLManager()
        
        # Create a SQL query to delete items with false identifiers
        delete_query = """
            DELETE FROM raw_table
            WHERE job_identifier = ANY(%s)
        """
        
        # Execute the query
        postgres_manager.execute_query(delete_query, (false_identifiers,))
        
        # Close database connection
        postgres_manager.close_connection()
        
        
    def delete_inactive_jobs_from_mongodb(self, false_identifiers):
        # Check if there are any false identifiers
        if not false_identifiers:
            return

        # Connect to MongoDB
        mongo_manager = MongoDBManager()

        # Create a filter to match items with false identifiers
        filter_query = {'job_identifier': {'$in': false_identifiers}}

        # Delete items from MongoDB
        mongo_manager.mongo_collection.delete_many(filter_query)
        
        # Close database connection
        mongo_manager.close_connection()