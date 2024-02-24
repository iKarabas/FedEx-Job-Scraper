from psycopg2.extras import Json
from jobs_project.items import JobItem
from database_managers.postgresql_manager import PostgreSQLManager
from database_managers.mongodb_manager import MongoDBManager

class PostgreSQLMongoDBPipeline:
    def __init__(self):
        # Create instances of the database managers
        self.postgres_manager = PostgreSQLManager()
        self.mongo_manager = MongoDBManager()

    def open_spider(self, spider):
        # PostgreSQL
        # Create the raw_table if it doesn't exist
        create_table_query = """
                      CREATE TABLE IF NOT EXISTS raw_table (
            id SERIAL PRIMARY KEY,
            job_identifier TEXT,
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
        self.postgres_manager.create_table(create_table_query)
 

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
            self.postgres_manager.insert_values(values)
            
            # MongoDB insertion
            self.mongo_manager.insert_values(mongodb_values)
        return item
    
    
    def close_spider(self, spider):
        self.postgres_manager.close_connection()
        self.mongo_manager.close_connection()
    


