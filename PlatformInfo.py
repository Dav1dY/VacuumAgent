import paho.mqtt.client as mqtt
import json
import socket
import time
import logging
# import psutil
import platform
import subprocess
import re
import threading
import select


class PlatformInfo:
    def __init__(self, maincomponent_id, subcomponent, broker="localhost", port=1883):
        self.init_success = False
        logging.basicConfig(filename='platform.log', level=logging.INFO, format='%(asctime)s %(message)s')
        logging.info("Initializing")

        # init parameters
        # noinspection PyBroadException
        try:
            self.broker = broker
            self.port = port
            self.config_topic = "/Devices/" + maincomponent_id + "/" + subcomponent + "/" + "Config"
            self.query_config_topic = "/Devices/adc_agent/QueryConfig"
            self.analog_topic = "/Devices/" + maincomponent_id + "/" + subcomponent + "/" + "Analog"
            self.last_time = int(time.time())
            self.current_time = 0
            self.target_address = self.loaded_data.get('target_address', None)  # default:"192.168.3.250"
            self.start_port = int(self.loaded_data.get('start_port', None))
            self.end_port = int(self.loaded_data.get('end_port', None))
            self.config_path = self.loaded_data.get('config_path', None)
            self.report_interval = int(self.loaded_data.get('report_interval', None))
            self.connect_retry_times = int(self.loaded_data.get('connect_retry_times', None))
            self.socket_timeout = int(self.loaded_data.get('socket_timeout', None))
            self.port_in_use = 0
            self.connection_state = False
            self.update_state = False
            self.sock = None
            self.client = None
            self.scheduled_report_ready = False
        except Exception:
            logging.error("Initialize parameters fail.")
            return
        if not self.target_address:
            logging.error("missing target_address in json")
            return
        if not self.start_port:
            logging.error("missing start_port in json")
            return
        if not self.end_port:
            logging.error("missing end_port in json")
            return
        if not self.config_path:
            logging.error("missing config_path in json")
            return
        if not self.report_interval:
            logging.error("missing report_interval in json")
            return
        if not self.connect_retry_times:
            logging.error("missing connect_retry_times in json")
            return
        if not self.socket_timeout:
            logging.error("missing socket_timeout in json")
            return
        logging.info("All parameters loaded success.")

        self.system = platform.system()
        self.cpu_usage = 0
        self.ram_usage = 0
        self.disk_usage = 0
        self.pc_model = "Unknown Model"
        self.json_data = None
        self.interval = 3
        logging.info(f"System is {self.system}")
        if self.system == 'Windows':
            # noinspection PyBroadException
            try:
                self.pc_model = subprocess.check_output('wmic csproduct get name', shell=True).decode().split('\n')[1]\
                    .strip()
            except Exception:
                self.pc_model = "Unknown Model"
        elif self.system == 'Linux':
            # noinspection PyBroadException
            try:
                self.pc_model = subprocess.check_output('sudo dmidecode -s system-product-name', shell=True).decode().\
                    strip()
            except Exception:
                self.pc_model = "Unknown Model"
        elif self.system == "Darwin":
            # noinspection PyBroadException
            try:
                result = subprocess.run(['system_profiler', 'SPHardwareDataType'], capture_output=True, text=True)
                lines = result.stdout.split('\n')
                for line in lines:
                    if "Model Name" in line:
                        self.pc_model = line.split(":")[1].strip()
            except Exception:
                self.pc_model = "Unknown Model"
        else:
            self.pc_model = "Unknown System"
        logging.info(f"pc model is {self.pc_model}")
        self.create_json(['Platform', 'Model', 'CPU_Usage', 'RAM_Usage', 'Disk_Usage'], [self.system,
                                                                                         self.pc_model,
                                                                                         self.cpu_usage,
                                                                                         self.ram_usage,
                                                                                         self.disk_usage])
        self.client = mqtt.Client(broker, port)
        logging.info("mqtt client established.")
        self.client.on_message = self.on_message
        self.client.on_connect = self.on_connect
        # noinspection PyBroadException
        try:
            self.client.connect("localhost", 1883, 60)
        except Exception as e:
            logging.error(f"Failed to connect to the broker:{e}.")
            return
        (result, mid) = self.client.subscribe('/topic/QueryUsage')
        if result == mqtt.MQTT_ERR_SUCCESS:
            logging.info("Subscribe successfully.")
        elif result == mqtt.MQTT_ERR_NO_CONN:
            logging.error(f"Subscription failed, no connection.")
            return
        else:
            logging.error(f"Subscription failed, error code {result}")
            return
        self.client.loop_start()
        logging.info("Loop started.")
        self.init_success = True
