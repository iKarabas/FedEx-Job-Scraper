import os
#from scrapy.settings.default_settings import TWISTED_REACTOR

TWISTED_REACTOR = "twisted.internet.selectreactor.SelectReactor"

BOT_NAME = "jobs_project"

SPIDER_MODULES = ["jobs_project.spiders"]
NEWSPIDER_MODULE = "jobs_project.spiders"

# Disable robots.txt rules
ROBOTSTXT_OBEY = False

# Set settings whose default value is deprecated to a future-proof value
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
FEED_EXPORT_ENCODING = "utf-8"

# Set to 'DEBUG' for more details
LOG_LEVEL = 'INFO'

POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = int(os.getenv('POSTGRES_PORT'))
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_TABLE_NAME = os.getenv('POSTGRES_TABLE_NAME')

MONGO_HOST = os.getenv('MONGO_HOST')
MONGO_PORT = int(os.getenv('MONGO_PORT'))
MONGO_DB = os.getenv('MONGO_DB')
MONGO_COLLECTION_NAME = os.getenv('MONGO_COLLECTION_NAME')
MONGO_USERNAME = os.getenv('MONGO_USERNAME')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')

# Configure pipelines
ITEM_PIPELINES = {
    'jobs_project.pipelines.PostgreSQLMongoDBPipeline': 300,
}

REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = int(os.getenv('REDIS_PORT'))
REDIS_DB = os.getenv('REDIS_DB')

# Configure Redis as a caching backend
REDIS_URL = 'redis://redis:6379/0' 
SCHEDULER = "scrapy.core.scheduler.Scheduler"
SCHEDULER_FLUSH_ON_START = True
DUPEFILTER_CLASS = "scrapy.dupefilters.RFPDupeFilter"
