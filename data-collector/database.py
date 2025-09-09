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
            "host": "postgres-db",
            "database": "industrial_data",
            "user": "postgres",
            "password": "industrial123",
            "connect_timeout": 10,
            "port": 5432
        }
        
        # Override defaults with provided config
        if config:
            self.config.update(config)

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

                with self.connection.cursor() as cursor:
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
                    logger.error("All connection attempts failed.")
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
            id BIGSERIAL PRIMARY KEY,
            sensor_id VARCHAR(50) NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            value FLOAT NOT NULL,
            quality VARCHAR(10) DEFAULT 'good',
            unit VARCHAR(20),
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """

        # Creating indexes for efficient querying
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
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        """


        try:
            with self.connection as conn:
                with conn.cursor() as cursor:
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
        

    def _insert_default_sensors(self):
        """
        Metadata for simulated sensors. IRL, this actually comes
        from the config files stored separately in our codebase.
        """
        
        # Dummy Data to simulate sensor data rows.
        # Each row comes as a tuple of columns
        # Measure - Sensor Name - Unit - Min Range - Max Range - Production Line 
        sensors_metadata = [
            ('temperature', 'Process Temperature Sensor', '°C', 0.0, 100.0, 'Reactor Tank A'),
            ('pressure', 'System Pressure Gauge', 'bar', 0.0, 15.0, 'Main Pipeline'),
            ('flow_rate', 'Coolant Flow Meter', 'L/min', 0.0, 1000.0, 'Cooling Circuit'),
            ('vibration', 'Motor Vibration Sensor', 'mm/s', 0.0, 25.0, 'Drive Motor M1'),
            ('motor_speed', 'Motor Speed Encoder', 'RPM', 0, 4000, 'Drive Motor M1')
        ]

        # Inserting dummy data into the table
        insert_query = """
        INSERT INTO sensors (sensor_id, description, unit, min_value, max_value, location)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (sensor_id) DO NOTHING;
        """

        try:
            with self.connection as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(insert_query, sensors_metadata)
                    conn.commit()
                    logger.debug("Sensor data was inserted to the table.")

        except psycopg2.Error as e:
            logger.warning(f"Failed to insert data to a table. Error: {e}")

            
    def bulk_insert_readings(self, readings):
        """
        Bulk-inserts all readings accumulated over the 
        buffer period (30s) into the DB table. Readings
        come as a list of rows -> row is a tuple of column
        values.  
        """

        if not readings:
            logger.debug("No readings to insert.")
            return False
            
        if not self.connected:
            logger.error("No connection is established with DB. Insertion failed.")
            return False
        
        insert_data = []
        for reading in readings:
            insert_data.append((
                reading["sensor_id"],
                reading["timestamp"],
                reading["value"],
                reading.get("quality", "good"),
                reading.get("unit", "")
            ))

        insert_sql = """
        INSERT INTO sensor_readings (sensor_id, timestamp, value, quality, unit)
        VALUES (%s, %s, %s, %s, %s)
        """

        try:
            with self.connection as conn:
                with conn.cursor() as cursor:
                    psycopg2.extras.execute_values(
                        cursor,
                        insert_sql,
                        insert_data,
                        template=None,
                        page_size=1000          # Processed in batches of 1000
                        
                    )
                    conn.commit()
                    logger.debug("Sensor data was inserted to the table.")
                    return True

        except psycopg2.Error as e:
            logger.warning(f"Failed to insert data to a table. Error: {e}")
            return False
        

    def get_recent_readings(self, sensor_id=None, hours=1, limit=1000):
        """
        Retrieves the most recent sensor readings. Returns a list of 
        dictionaries (i.e. a DataFrame).
        """ 
        if not self.connected:
            logger.error("No connection is established with DB. Query failed.")
            return False

        where_cond = ["timestamp > NOW() - INTERVAL '%s hours'"]
        params = [hours]

        if sensor_id:
            where_cond.append("sensor_id = %s")
            params.append(sensor_id)

        query = f"""
        SELECT sensor_id, timestamp, value, quality, unit
        FROM sensor_readings
        WHERE {' AND '.join(where_cond)}
        ORDER BY timestamp DESC
        LIMIT %s
        """
        params.append(limit)

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)

                readings = []
                for row in cursor.fetchall():
                    readings.append({
                        "sensor_id": row[0],
                        "timestamp": row[1],
                        "value": row[2],
                        "quality": row[3],
                        "unit": row[4]
                    })

                logger.debug(f"Retrieved {len(readings)} readings read.")
                return readings
        
        except psycopg2.Error as e:
            logger.error(f"Failed to pull sensor data due to error: {e}")
            return []


    def get_sensor_stats(self, hours=24):
        if not self.connected:
            return {}
        
        query = """
        SELECT 
            sensor_id,
            COUNT(*) as reading_count,
            AVG(value) as avg_value,
            MIN(value) as min_value,
            MAX(value) as max_value,
            STDDEV(value) as std_deviation,
            COUNT(CASE WHEN quality != 'good' THEN 1 END) as bad_readings
        FROM sensor_readings
        WHERE timestamp > NOW() - INTERVAL '%s hours'
        GROUP BY sensor_id
        ORDER BY sensor_id
        ;
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, (hours,))
                
                stats = {}
                for row in cursor.fetchall():
                    stats[row[0]] = {
                        'reading_count': row[1],
                        'avg_value': float(row[2]) if row[2] else 0,
                        'min_value': float(row[3]) if row[3] else 0,
                        'max_value': float(row[4]) if row[4] else 0,
                        'std_deviation': float(row[5]) if row[5] else 0,
                        'bad_readings': row[6]
                    }
                
                logger.debug(f"Generated statistics for {len(stats)} sensors")
                return stats
        
        except psycopg2.Error as e:
            logger.error(f"Failed to generate statistics: {e}")
            return {}
    

    def disconnect(self):
        """
        Clean disconnect from database.
        """
        if self.connection:
            try:
                self.connection.close()
                self.connected = False
                logger.info("Disconnected from database")
            except Exception as e:
                logger.warning(f"Error during database disconnect: {e}")

    
    def health_check(self):
        """
        Check database connectivity and basic functionality.
        """
        if not self.connected:
            return {'status': 'disconnected', 'error': 'No database connection'}
        
        try:
            with self.connection.cursor() as cursor:
                # Test basic connectivity
                cursor.execute("SELECT NOW()")
                db_time = cursor.fetchone()[0]
                
                # Check table existence
                cursor.execute("""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_name = 'sensor_readings'
                """)
                table_exists = cursor.fetchone()[0] > 0
                
                # Get recent data count
                cursor.execute("""
                    SELECT COUNT(*) FROM sensor_readings 
                    WHERE timestamp > NOW() - INTERVAL '1 hour'
                """)
                recent_count = cursor.fetchone()[0]
                
                return {
                    'status': 'healthy',
                    'database_time': db_time,
                    'tables_exist': table_exists,
                    'recent_readings': recent_count
                }
                
        except Exception as e:
            return {'status': 'error', 'error': str(e)}



            

            

            




