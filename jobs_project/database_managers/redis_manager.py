import redis
from scrapy.utils.project import get_project_settings

class RedisManager:
    def __init__(self):
        settings = get_project_settings()
        self.host = settings.get('REDIS_HOST')
        self.port = settings.get('REDIS_PORT')
        self.db = settings.get('REDIS_DB')
        self.connection = None

        # Connect to Redis
        self.connect()

    def connect(self):
        """Establish a connection to the Redis database."""
        try:
            self.connection = redis.StrictRedis(host=self.host, port=self.port, db=self.db)
        except Exception as e:
            # Handle the connection error
            print(f"Error connecting to redis: {e}")
            
    def set_value_for_an_existing_key(self, prefix, key, value):
        """Set a key-value pair in the Redis database if it exists"""
        if not self.connection:
            self.connect()
            
        redis_key = f"{prefix}:{key}"
        # The key exists , set its value
        if self.connection.exists(redis_key):
            self.connection.set(redis_key, value)      
    def set_value(self, key, value):
        """Set a key-value pair in the Redis database """
        if not self.connection:
            self.connect()
        self.connection.set(key, value)    

    def get_value(self, key):
        """Get the value associated with a given key from the Redis database."""
        if not self.connection:
            self.connect()
        return self.connection.get(key)
    
    def delete(self, key):
        """Delete the key-value pair in the Redis database."""
        if self.exists(key):
            self.connection.delete(key)
    
    def exists(self, key):
        """Check whether the key exists in the Redis database."""
        return self.connection.exists(key)
        
        
    def close_connection(self):
        """Close the connection to the Redis database."""
        if self.connection:
            self.connection.close()

    def get_keys_with_value_and_prefix(self, key_prefix, target_value):
        cursor = 0
        matching_keys = []

        # Convert target_value to bytes using utf-8 encoding
        target_value_bytes = target_value.encode('utf-8')

        while True:
            cursor, keys = self.connection.scan(cursor, match=f"{key_prefix}:*", count=1000)
            # Break out of the loop if no keys are found
            if keys == []:
                break

            # Retrieve values for multiple keys in a single Redis round-trip
            values = self.connection.mget(keys)
            
            for key, value in zip(keys, values):            
                # Decode key to get identifier
                identifier = key.decode('utf-8').split(":")[-1]
    
                # Check if the values match after converting to bytes
                if value == target_value_bytes:
                    matching_keys.append(identifier)

            # Break out of the loop when cursor is 0
            if cursor == 0:
                break
        return matching_keys

