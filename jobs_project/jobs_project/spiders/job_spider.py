import json
import scrapy
from jobs_project.items import JobItem
import os
import redis

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
        self.redis = redis.StrictRedis(host=self.redis_host, port=self.redis_port, db=self.redis_db)
        self.page_number = 1
        self.log("Starting the spider.")

    
    def start_requests(self):
        url = self.base_url.format(self.page_number)
        yield scrapy.Request(url=url, callback=self.parse_json_response)

    def parse_json_response(self, response):
        try:
            job_data = json.loads(response.text)
            jobs = job_data.get('jobs', [])
            # Check if there are no jobs, stop the spider
            if not jobs:
                self.log("No more data to scrape. Stopping the spider.")
                return
            
            for job in jobs:
                title = job.get('data', {}).get('title')
                street_address = job.get('data', {}).get('street_address')
                postal_code = job.get('data', {}).get('postal_code')
                # Concatenate title and street_address to create a unique identifier
                identifier = f"{title}_{postal_code}_{street_address}"
                if not self.is_item_cached(identifier):
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
        
    def is_item_cached(self, identifier):
        # Check if the identifier is present in Redis cache
        cache_key = f"{self.redis_key_prefix}:{identifier}"
        return self.redis.exists(cache_key)
    
    def cache_item(self, identifier):
        # Cache the identifier in Redis
        cache_key = f"{self.redis_key_prefix}:{identifier}"
        self.redis.set(cache_key, 1)
