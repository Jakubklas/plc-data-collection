import time
import logging
from datetime import datetime
from collections import deque
from threading import Thread, Lock, Event
import signal
import sys
from pymodbus.client.sync import ModbusTcpClient
from database import IndustrialDatabaseManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('DataCollector')


class PLCConnection:
    """
    Manages connection to a single PLC device over Modbus/TCP
    """

    def __init__(self, host, port=502, timeout=3):
        """
        Init the PLC connection params like PLC IP Address, port, timeout
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self.client = None
        self.connected = None
        self.last_error = None

        # Define the sensor map
        self.sensor_map = {
            'temperature': {'address': 0, 'scale': 100, 'unit': '°C'},
            'pressure': {'address': 1, 'scale': 100, 'unit': 'bar'},
            'flow_rate': {'address': 2, 'scale': 100, 'unit': 'L/min'},
            'vibration': {'address': 3, 'scale': 100, 'unit': 'mm/s'},
            'motor_speed': {'address': 4, 'scale': 100, 'unit': 'RPM'},
        }

        logger.info(f"Initialized PLC class to: {host}:{port}")


    def connect(self):
        """
        Establish connection w/ PLC.
        """
        try:
            if self.client:
                self.client.close()

            self.client = ModbusTcpClient(self.host, port=self.port, timeout=self.timeout)
            self.connected = self.client.connect()
            
            if self.connected:
                logger.info(f"Successfully connected to PLC at {self.host}:{self.port}")
                self.last_error = None
            else:
                self.last_error = "Connection failed"
                logger.info(f"Failed to connect to PLC at {self.host}:{self.port}")
        except Exception as e:
            self.connected = False
            self.last_error = str(e)
            logger.error(f"Failed the connection attempts due to: {e}")

        return self.connected


    def read_sensors(self):
        """
        Read all sensor data over established Modbus/TCP connection.
        
        Returns a list of sensor readings w/ timestamps.
        """

        # Handle cases where connection was not yet established
        if not self.connected:
            logger.warning("Cannot read from sources, PLC, not connected.")
            return []
        
        # If connected, init an empty list & current timestamps
        readings = []
        timestamp = datetime.utcnow()

        try:
            for sensor, config in self.sensor_map.items():
                result = self.client.read_holding_registers(
                    config["address"],
                    1,
                    unit=1
                )
                logger.debug(f"Raw data from {sensor}: {result}")

                if result.isError():
                    logger.warning(f"Error reading {sensor}: {result}")
                    continue

                # Converting scaled values into real ones since Modbus cannot send decimals/floats --> We have to scale to get the real values
                raw_value = result.registers[0]
                actual_value = raw_value / config.get("scale")

                reading = {
                    "sensor_id": sensor,
                    "value": actual_value,
                    "timestamp": timestamp,
                    "quality": "good",              # In production we'd validate the values are within exp. range, etc. to eliminate outliers
                    "unit": config["unit"]
                }

                readings.append(reading)

            if readings:
                logger.debug(f"Read {len(readings)} sensor readings successfully.")
        
        except Exception as e:
            logger.error(f"Error reading from sensors: {e}")
            self.connected = False
        
        return readings


    def disconnect(self):
        """
        Gracefully disconnect from the PLC.
        """
        if self.client:
            self.client.close()
            self.connected = False
            logger.info(f"Disconnected from PLC at {self.host}:{self.port}")
        

class DataBuffer:
    """
    Thread-enabled buffer for sensor data reading.
    """

    def __init__(self, flush_interval=30, max_size=1000):
        self.buffer = deque()                       # A Deque is a list that can be appended/removed from both ends
        self.flush_interval = flush_interval
        self.max_size = max_size
        self.lock = Lock()
        self.last_flush = time.time()

        logger.info("Initialized DataBuffer")


    def add_readings(self, readings):
        """
        Adds sensor data list to the buffer.
        """ 
        if not readings:
            return
        
        with self.lock:
            self.buffer.extend(readings)
            buffer_size = len(self.buffer)

        logger.debug(f"Added {len(readings)} readings, buffer size: {buffer_size}")

        # Check if flush is needed due to size
        if buffer_size >= self.max_size:
            logger.info(f"Buffer size limit reached, forcing flush.")
            return True
        
        return False
    
    def should_flush(self):
        """Checks if it's time to flush."""
        return (time.time() - self.last_flush) >= self.flush_interval
    
    def get_and_clear(self):
        """Get all buffered data and clear the buffer."""
        with self.lock:
            data = list(self.buffer)
            self.buffer.clear()
            self.last_flush = time.time()
            return data
        

class IndustrialDataCollector:
    def __init__(self, plc_host="plc_simulator", db_config=None):
       self.plc = PLCConnection(plc_host)
       self.buffer = DataBuffer()
       self.db = IndustrialDatabaseManager(db_config)

       self.running = Event()
       self.running.set()

       self.stats = {
            'readings_collected': 0,
            'readings_stored': 0,
            'connection_errors': 0,
            'database_errors': 0,
            'start_time': time.time()
        }

    def connect_tosystems(self):
       logger.info("Connecting to industrial systems...")

       if not self.db.connect():
           logger.error("Failed to connect to database.")
           return False
       
       self.db.initialize_database()

       if not self.plc.connect():
            logger.error("Failed to connect to PLC")
            return False
       
       logger.info("Successfully connected to all systems")
       return True
    

    def data_collection_loop(self):
        """
        Main data collection loop.
        """
        logger.info("Starting the data collaction loop.")
        read_interval = 5

        while self.running.is_set():
            try:
                # Ensuring actuve connection
                if not self.plc.connected:
                    if not self.plc.connect():
                        self.stats["connection_errors"] += 1
                        time.sleep(10) # Waiting before retries
                        continue
                readings = self.plc.read_sensors()

                if readings:
                    force_flush = self.buffer.add_readings(readings)
                    self.stats["readings_collected"] += len(readings)

                    if force_flush or self.buffer.should_flush():
                        self.flush_buffer_to_database()

                time.sleep(read_interval)
            
            except Exception as e:
                logger.error(f"Error in data collection loop: {e}")
                self.stats['connection_errors'] += 1
                time.sleep(5)


    def flush_buffer_to_database(self):
        readings = self.buffer.get_and_clear()

        if not readings:
            return None
        
        logger.info(f"Flushing {len(readings)} readings to database")

        try:
            success = self.db.bulk_insert_readings(readings)
            
            if success:
                self.stats["readings_stored"] += len(readings)
                logger.info(f"Successfully stored {len(readings)} readings")
            else:
                self.stats['database_errors'] += 1
                logger.error(f"Failed to store {len(readings)} readings - data lost!")

        except Exception as e:
            self.stats['database_errors'] += 1
            logger.error(f"Database flush error: {e}")
        

    def run(self):
        """Main class entry point for the data collector."""
        
        # Signal handler for graceful shutdowns
        def _signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initializing shut-down.")
            self.shutdown()

        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGINT, _signal_handler)

        try:
            # Connectig to systems
            if not self.connect_tosystems():
                logger.error("Failed to connect to systems.")
                return 1
            
            # Log stats
            stats_thread = Thread(target=self._stats_loop)
            stats_thread.daemon = True
            stats_thread.start()

            self.data_collection_loop()

        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
            return 1
        finally:
            self.cleanup()


    def cleanup(self):
        """Disconnects ans shuts down."""
        logger.info("Cleaning up resources...")

        if self.plc:
            self.plc.disconnect()
        if self.db:
            self.db.disconnect()

        logger.info("Cleaning up resources...")
        

    def shutdown(self):
        """
        Graceful loop shut-down, ensuring no unsavec data is lost.
        """
        logger.info("Starting graceful shut-down...")
        self.running.clear()
        self.flush_buffer_to_database()

        logger.info("Gracefil shut-down complete.")
        





if __name__ == "__main__":
    """
    Entry point for the industrial data collector.
    
    In production, this would be deployed as a service on edge computers
    in factories, connecting to real PLCs and central databases.
    """
    logger.info("=== Industrial Data Collector Starting ===")
    
    # Database configuration (from container environment variables)
    db_config = {
        'host': 'postgres-db',  # Docker service name
        'database': 'industrial_data',
        'user': 'postgres',
        'password': 'industrial123'
    }
    
    try:
        # Create and run the data collector
        collector = IndustrialDataCollector(
            plc_host="plc-simulator",  # Docker service name
            db_config=db_config
        )
        
        exit_code = collector.run()
        sys.exit(exit_code)
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)