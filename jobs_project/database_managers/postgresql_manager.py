import psycopg2
from scrapy.utils.project import get_project_settings

class PostgreSQLManager:
    def __init__(self):
        settings = get_project_settings()
        self.table_name = settings.get('POSTGRES_TABLE_NAME')
        self.db_settings = {
            'host': settings.get('POSTGRES_HOST'),
            'port': settings.get('POSTGRES_PORT'),
            'database': settings.get('POSTGRES_DB'),
            'user': settings.get('POSTGRES_USER'),
            'password': settings.get('POSTGRES_PASSWORD'),
        }
        self.connection = None
        self.cursor = None
        # Connect to the database
        self.connect()
                    
    def connect(self):
        try:
            self.connection = psycopg2.connect(**self.db_settings)
            self.cursor = self.connection.cursor()
        except psycopg2.Error as e:
            # Handle the connection error
            print(f"Error connecting to PostgreSQL: {e}")
        
        
    def create_table(self, create_table_query):
        self.cursor.execute(create_table_query)
        self.connection.commit()
         
    def insert_values(self, values):
        insert_query = """
            INSERT INTO {table_name} (
                {columns}
            ) VALUES (
                {values}
            )
        """.format(
            table_name = self.table_name,
            columns=', '.join(values.keys()),
            values=', '.join('%({})s'.format(key) for key in values.keys())
        )
        
        self.execute_query(insert_query,values)
    
    def fetch_values(self,query):
        self.execute_query(query)
        return self.cursor.fetchall()
    
            
    def execute_query(self, query, values=None):
        # Execute a query on the PostgreSQL database
        if not self.connection:
            self.connect()
        try:
            self.cursor.execute(query, values)
            self.connection.commit()
        except psycopg2.Error as e:
            self.connection.rollback()
            print(f"Failed to insert item into PostgreSQL. Error: {e}")


    def close_connection(self):
        self.connection.commit()
        self.connection.close()

# Example Usage:
# postgres_manager = PostgreSQLManager(host='localhost', port=5432, database='example_db', user='example_user', password='example_password')
# postgres_manager.execute_query('CREATE TABLE IF NOT EXISTS example_table (id SERIAL PRIMARY KEY, name VARCHAR);')
# postgres_manager.execute_query('INSERT INTO example_table (name) VALUES (%s);', ('John Doe',))
# postgres_manager.close_connection()
