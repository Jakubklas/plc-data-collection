import psycopg2
import psycopg2.extras
from datetime import datetime
import logging
import time

logger = logging.getLogger("DatabaseManager")

class IndustrialDatabaseManager:
    def __init__(self, config=None):
        """
        Manager Postgres operations for PLC data. Data
        is time-series (timestamped) and quality is examined
        w/ every reading.
        """
        self.config = {
            "host": "postgres_db",
            "database": "industrial_data",
            "user": "postgres",
            "password": "idustrial123",
            "connect_timeout": 10,
            "port": 5432
        }

        self.connection = None
        self.connected = False

        logger.info(f"Database manager initialized for {self.config['host']}")

    
    def connect(self):
        """
        Connects to DB w/ retry logic. 
        """
        max_retries = 5
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                self.connection = psycopg2.connect(**self.config)

                with self.connection.cursor as cursor:
                    cursor.execute("SELECT 1")

                self.connected = True
                logger.info(f"Connected to database on attempt {attempt + 1}")
                return True
            
            except Exception as e:
                logger.warning(f"Database connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    logger.error("All connections attempts failed.")
                    self.connected = False
                    return False
        
        return False
    

    def initialize_database(self):
        """
        Create a table w/ a time-series setup to enable fast
        bulk operations, time-ranged queries and partitioning
        for large datasets. 
        """

        if not self.connected:
            logger.error("Cannot initialize DB due to missing connection.")
            return False
        
        create_readings_table = """
        CREATE TABLE IF NOT EXISTS sensor_readings (
            id BIGSERIAL PRIMARY_KEY,
            sensor_id VARCHAR(50) NOT NULL,
            timestamp TIMESTAMPZ NOT NULL,
            value FLOAT NOT NULL,
            quality VARCHAR(10) DEFUALT 'good',
            unit VARCHAR(20),
            created_at TIMESTAMPZ DEFAULT NOW()
        );
        """

        # Crearting indexes for efficient querying
        create_indexes = [
            """
            CREATE INDEX IF NOT EXISTS idx_sensor_readings_sensor_time
            ON sensor_readings (sensor_id, timestamp DESC);
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_sensor_readings_timestamp
            ON sensor_readings (timestamp DESC);
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_sensor_readings_quality
            ON sensor_readings (quality) WHERE quality != 'good';
            """
        ]

        create_sensors_table = """
        CREATE TABLE IF NOT EXISTS sensors (
            sensor_id VARCHAR(50) PRIMARY_KEY,
            description TEXT,
            unit VARCHAR(20),
            min_value FLOAT,
            max_value FLOAT,
            location VARCHAR(100),
            created_at TIMESTAMPZ DEFAULT NOW(),
            updated_at TIMESTAMPZ DEFAULT NOW()
        );
        """


        try:
            with self.connection as conn:
                with conn.cursor as cursor:
                    cursor.execute(create_readings_table)
                    cursor.execute(create_sensors_table)
                    for query in create_indexes:
                        cursor.execute(query)
                    conn.commit()
                    logger.info("Database tables and indexes created successfully")
                    self._insert_default_sensors()

                    return True

        except psycopg2.Error as e:
            logger.error(f"Failed to initialize database: {e}")
            self.connection.rollback()
            return False
        

        


            

