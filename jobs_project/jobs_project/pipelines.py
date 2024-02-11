import psycopg2
from psycopg2.extras import Json
from pymongo import MongoClient
from jobs_project.items import JobItem

class PostgreSQLMongoDBPipeline:
    def __init__(self, db_settings, mongo_settings):
        self.db_settings = db_settings
        self.mongo_settings = mongo_settings
        self.connection = None
        self.cursor = None
        self.mongo_client = None
        self.mongo_db = None
        self.mongo_collection = None

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        db_settings = {
            'host': settings.get('POSTGRES_HOST'),
            'port': settings.get('POSTGRES_PORT'),
            'database': settings.get('POSTGRES_DB'),
            'user': settings.get('POSTGRES_USER'),
            'password': settings.get('POSTGRES_PASSWORD'),
        }
        mongo_settings = {
            'host': settings.get('MONGO_HOST'),
            'port': settings.get('MONGO_PORT'),
            'database': settings.get('MONGO_DB'),
            'collection': settings.get('MONGO_COLLECTION_NAME'),
            'user': settings.get('MONGO_USERNAME'),
            'password': settings.get('MONGO_PASSWORD'),
        }
        return cls(db_settings, mongo_settings)

    def open_spider(self, spider):
        # PostgreSQL
        self.connection = psycopg2.connect(**self.db_settings)
        self.cursor = self.connection.cursor()

        # Create the raw_table if it doesn't exist
        create_table_query = """
                      CREATE TABLE IF NOT EXISTS raw_table (
            id SERIAL PRIMARY KEY,
            slug VARCHAR(255),
            language VARCHAR(10),
            languages VARCHAR(255)[],
            req_id VARCHAR(255),
            title VARCHAR(255),
            description TEXT,
            location_name TEXT,
            street_address VARCHAR(255),
            city VARCHAR(255),
            state VARCHAR(255),
            country VARCHAR(255),
            country_code CHAR(2),
            postal_code VARCHAR(20),
            location_type VARCHAR(20),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            tags VARCHAR(255)[],
            tags5 VARCHAR(255)[],
            tags6 VARCHAR(255)[],
            brand VARCHAR(255),
            promotion_value INTEGER,
            salary_currency VARCHAR(20),
            salary_value INTEGER,
            salary_min_value INTEGER,
            salary_max_value INTEGER,
            employment_type VARCHAR(20),
            hiring_organization TEXT,
            source VARCHAR(255),
            apply_url TEXT,
            internal BOOLEAN,
            searchable BOOLEAN,
            applyable BOOLEAN,
            li_easy_applyable BOOLEAN,
            meta_data_login_url TEXT,
            meta_data_region_description TEXT,
            meta_data_site_id VARCHAR(255),
            meta_data_googlejobs_companyName VARCHAR(255),
            meta_data_googlejobs_jobName VARCHAR(255),
            meta_data_googlejobs_derivedInfo_jobCategories VARCHAR(255)[],
            meta_data_googlejobs_jobSummary TEXT,
            meta_data_googlejobs_jobTitleSnippet VARCHAR(255),
            meta_data_googlejobs_searchTextSnippet VARCHAR(255),
            meta_data_canonical_url TEXT,
            meta_data_last_mod TIMESTAMP,
            meta_data_gdpr BOOLEAN,
            update_date TIMESTAMP,
            create_date TIMESTAMP,
            category VARCHAR(255),
            full_location VARCHAR(255),
            short_location VARCHAR(255)
        );
        
        """
        self.cursor.execute(create_table_query)
        self.connection.commit()

        # MongoDB with authentication
        mongo_uri = f"mongodb://{self.mongo_settings['user']}:{self.mongo_settings['password']}@{self.mongo_settings['host']}:{self.mongo_settings['port']}"
        self.mongo_client = MongoClient(mongo_uri)
        self.mongo_db = self.mongo_client[self.mongo_settings['database']]
        self.mongo_collection = self.mongo_db[self.mongo_settings['collection']]


    def close_spider(self, spider):
        self.connection.commit()
        self.connection.close()
        self.mongo_client.close()

    def process_item(self, item, spider):
        if isinstance(item, JobItem):
            values = dict(item)
            # Exclude the 'id' column from the list of columns and values
            id_column = 'id'
            values.pop(id_column, None)
            mongodb_values = values 
                    
            # Convert nested structures to JSON for PostgreSQL
            for key, value in values.items():
                if isinstance(value, dict):
                    values[key] = Json(value)

            # PostgreSQL insertion
            insert_query = """
                INSERT INTO raw_table (
                    {columns}
                ) VALUES (
                    {values}
                )
            """.format(
                columns=', '.join(values.keys()),
                values=', '.join('%({})s'.format(key) for key in values.keys())
            )

            # PostgreSQL insertion
            try:
                self.cursor.execute(insert_query, values)
                self.connection.commit()
            except psycopg2.Error as e:
                self.connection.rollback()
                spider.log(f"Failed to insert item into PostgreSQL. Error: {e}")

            # MongoDB insertion
            try:
                self.mongo_collection.insert_one(mongodb_values)
            except Exception as e:
                spider.log(f"Failed to insert item into MongoDB. Error: {e}")
        return item


