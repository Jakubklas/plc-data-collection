import time
import random
import math
from pymodbus.server.sync import StartTcpServer
from pymodbus.device import ModbusDeviceIdentification
from pymodbus.datastore import ModbusSequentialDataBlock, ModbusSlaveContext, ModbusServerContext
import threading
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - PLC - %(message)s')
logger = logging.getLogger(__name__)


class IndustrialPLCSimulator:
    """
    Simulates real-world PLC incl. data collection, logging, connectivity, etc..
    
    Mimics:
    - Input Registers (sensor readings)
    - Holding Registers (configuration values)
    - Discrete Inputs (digital sensors, e.g. switches)
    - Coils (digital outputs, e.g. motor controls)
    """

    def __init__(self):
        """
        Utilizes modbus with 16-bit registers (0-65535). We'll scale decimal values
        to make them readable.
        """
        # Defining the sensors & their specs
        self.sensors = {
            "temperature": {"address": 0, "min": 15.0, "max": 85.0, "unit": "Celsius"},
            "pressure": {"address": 1, "min": 0.8, "max": 12.5, "unit": "bar"},
            "flow_rate": {"address": 2, "min": 0.0, "max": 500.0, "unit": "L/min"},
            "vibration": {"address": 3, "min": 0.1, "max": 15.0, "unit": "mm/s"},
            "motor_speed": {"address": 4, "min": 0.0, "max": 3600.0, "unit": "RPM"}
        }

        # Modbus data blocks (registers), i.e. PLC memory areas
        self.holding_registers = ModbusSequentialDataBlock(0, [0]*100)                          #TODO: Need to better understand the HRs

        # Modbus Context (like a PLC memory map)
        self.context = ModbusSlaveContext(
            di = None,                      # Dicrete inputs
            co = None,                      # Coils, digital outputs
            hr = self.holding_registers,    # Holding registers for collected data
            ir = None                       # Input Registers (Read-only sensors)
        )

        # Wrapping it as a server supporting multiple slave devices
        self.server_context = ModbusServerContext(
            slaves = self.context,
            single = True
            )
        
        # Time tracking for sensor variations
        self.start_time = time.time()
        self.running = True

        logger.info("PLC Simulator initialized with sensors: %s", list(self.sensors.keys()))


    def generate_data(self):
        """
        Generates realistic sensor time-series data.
        """
        current_time = time.time() - self.start_time
        
        # Store current values to avoid circular dependencies
        current_values = {}
        for sensor_name, config in self.sensors.items():
            address = config['address']
            try:
                scaled_value = self.holding_registers.getValues(address, 1)[0]
                current_values[sensor_name] = scaled_value / 100.0
            except:
                current_values[sensor_name] = (config['min'] + config['max']) / 2
        
        for sensor_name, config in self.sensors.items():
            trend_factor = math.sin(current_time / 300) * 0.3
            
            if sensor_name == 'temperature':
                base_value = 45 + trend_factor * 20
                noise = random.gauss(0, 1.5)
                
            elif sensor_name == 'pressure':
                temp_influence = (current_values.get('temperature', 45) - 45) / 10
                base_value = 5.2 + temp_influence + trend_factor * 2
                noise = random.gauss(0, 0.1)
                
            elif sensor_name == 'flow_rate':
                pressure_influence = (current_values.get('pressure', 5) - 5) * 20
                base_value = 150 + pressure_influence + trend_factor * 50
                noise = random.gauss(0, 5)
                
            elif sensor_name == 'vibration':
                motor_influence = current_values.get('motor_speed', 0) / 500
                base_value = 2.0 + motor_influence + abs(trend_factor) * 3
                noise = random.gauss(0, 0.2)
                
            elif sensor_name == 'motor_speed':
                cycle_position = (current_time % 120) / 120
                if cycle_position < 0.7:
                    base_value = 1800 + trend_factor * 200
                else:
                    base_value = 0
                noise = random.gauss(0, 25) if base_value > 0 else 0
            
            final_value = base_value + noise
            final_value = max(config['min'], min(config['max'], final_value))
            
            scaled_value = int(final_value * 100)

            # Writing the reading to the PLC holding registers
            self.holding_registers.setValues(config['address'], [scaled_value])
            
        logger.debug("Updated sensor readings at time %.1f", current_time)
        

    def get_sensor_values(self, sensor_name):
        """
        Retrieves loaded sensor values.
        """
        if sensor_name in self.sensors:
            address = self.sensors[sensor_name]["address"]
            scaled_values = self.holding_registers.getValues(address, 1)[0]
            return scaled_values / 100.0
        return 0
    

    def update_sensor_continuously(self):
        """
        Simulates sensor values arriving to PLC each 3 sec. IRL, this
        happens immediately as the sensors receive data.
        """
        logger.info("Starting continuous sensor updates every 3 seconds.")

        while self.running:
            try:
                self.generate_data()
                
                if int(time.time()) % 15 == 0:      # Emit log each 15 seconds
                    values = []
                    for name, config in self.sensors.items():
                        value = self.get_sensor_values(name)
                        values.append(f"{name}: {value:.1f} {config["unit"]}")
                    logger.info("Current readings - %s", " | ".join(values))

                time.sleep(3)

            except Exception as e:
                logger.error("Error updating sensors: %s", str(e))


    def start_server(self):
        """
        Starting a Modbus/TCP PLC server. This makes the PLC
        accessible over the network. IRL PLCs listen on port 502
        by default.
        """
        # Setting up the device identification over the network
        identity = ModbusDeviceIdentification()
        identity.VendorName = "Testing Industries"
        identity.ProductCode = "SIM-PLC-001"
        identity.VendorUrl = "www.example.com"
        identity.ProductName = "Industrial Process Simulator Container"
        identity.ModelName = "PLC Simulator v1"
        identity.MajorMinorRevision = "1.0"

        # Start the sensor update threads
        sensor_thread = threading.Thread(target=self.update_sensor_continuously)
        sensor_thread.daemon = True
        sensor_thread.start()

        logger.info("Starting Modbus TCP server on 0.0.0.0:502")
        logger.info("Sensor addresses: %s", {name: config["address"] for name, config in self.sensors.items()})

        try:
            # Start Modbus Server
            StartTcpServer(
                context=self.server_context,
                identity=identity,
                address=("0.0.0.0", 502),
                allow_reuse_address=True
            )
        except Exception as e:
            logger.error(f"Failed to start Modbus TCP server due to: {e}")
            self.running = False
        finally:
            logger.info("PLC Simulator Stopped.")


if __name__ == "__main__":

    logger.info("=== Test Industries PLC Simulator Starting ===")
    try:
        plc = IndustrialPLCSimulator()
        plc.start_server()

    except Exception as e:
        logger.info(f"Received Shut-down signal: {e}")
