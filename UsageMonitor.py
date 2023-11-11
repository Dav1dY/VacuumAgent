import paho.mqtt.client as mqtt
import json
import time
import logging
from logging.handlers import TimedRotatingFileHandler
import platform
import subprocess
import threading
import psutil
import os
from typing import Optional


class UsageMonitor:
    CLASS_NAME = 'UsageMonitor'
    REPORTER_NAME = 'ReportThread'
    CONFIG_PATH = '/vault/UsageMonitor/config/UsageMonitor_config.json'

    def __init__(self):
        self.init_success = False

        self.logger: Optional[logging.Logger] = None
        self.loaded_config = None
        self.mqtt_client = None
        self.mqtt_connect_state = False
        self.scheduled_report_ready = False
        self.scheduled_report_thread = None
        self.config_msg = None

        self.station_type = None
        self.station_number = None
        self.system = None
        self.total_disk = 0
        self.cpu_usage = 0
        self.ram_usage = 0
        self.disk_usage = 0
        self.pc_model = "Unknown Model"
        self.ip_address = None
        self.cpu_message = None
        self.ram_message = None
        self.disk_message = None
        self.maincomponent_id = None
        self.subcomponent_id = '/' + self.CLASS_NAME
        self.config_topic = None
        self.analog_topic = None

        self.fixture_ip = "10.0.1."
        self.local_ip = "10.0.1.200"
        self.mqtt_host = "localhost"
        self.mqtt_port = 1883
        self.mqtt_keepalive = 60
        self.configmsg_path = "/vault/UsageMonitor/config/Config2Send_Vacuum.json"
        self.station_path = "/vault/ADCAgent/dst/setting/adc_agent_register.json"
        self.fixture_path = "/vault/data_collection/test_station_config/gh_station_info.json"
        self.report_interval = 10
        self.connect_retry_times = 3
        self.publish_fail_tolerance = 5
        self.query_config_topic = "/Devices/adc_agent/QueryConfig"

        try:
            self.init_logger()
            self.logger.info("*************************")
            self.logger.info("Initializing.")
            self.load_config()
            self.init_parameters()
            self.get_system_name()
            self.get_pc_model()
            self.get_total_disk_size()
            self.load_configmsg()
            self.json_data_init()
            self.mqtt_client_init()
            self.mqtt_connect()
            self.scheduled_report_init()
            self.scheduled_report_ready = True
            self.init_success = True
            self.logger.info("All init done.")
        except Exception as e:
            self.logger.error(f"Initialization failed : {e}.")
            raise

    def init_logger(self):
        LOGGER_NAME = "UsageMonitorLogger"
        LOG_PATH = '/vault/UsageMonitor/log'
        LOG_HEADER = LOG_PATH + '/' + self.CLASS_NAME
        LOG_UPDATE_TIME = 'midnight'
        LOG_SAVE_NUMBER = 30
        LOG_NAME_FORMAT = "%Y-%m-%d.log"
        LOG_FORMAT = '%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'

        if self.logger is not None:
            return
        self.logger = logging.getLogger(LOGGER_NAME)
        self.logger.setLevel(logging.INFO)
        if not os.path.exists(LOG_PATH):
            try:
                os.makedirs(LOG_PATH)
            except Exception as e:
                print(f"Can not create log file: {e}, exit.")
                raise ValueError(f"Logger initialization failed")
        try:
            handler = TimedRotatingFileHandler(LOG_HEADER, when=LOG_UPDATE_TIME, backupCount=LOG_SAVE_NUMBER)
            handler.suffix = LOG_NAME_FORMAT
            formatter = logging.Formatter(LOG_FORMAT)
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        except Exception as e:
            print(f"Logger error: {e}, exit.")
            raise ValueError(f"Logger initialization failed: {e}")

    def load_config(self):
        try:
            with open(self.CONFIG_PATH, 'r') as f:
                self.loaded_config = json.load(f)
            self.logger.info("vacuum_config load success.")
        except FileNotFoundError:
            self.logger.error("vacuum_config was not found, will use default.")
        except json.JSONDecodeError:
            self.logger.error("An error occurred decoding the JSON, will use default.")
        except Exception as e:
            self.logger.error(f"An unexpected error occurred loading config file: {e}, will use default", exc_info=True)

    def init_parameters(self):
        TOPIC_HEADER = "/Devices/"
        CONFIG_TOPIC_END = '/Config'
        ANALOG_TOPIC_END = '/Analog'
        try:
            if self.loaded_config is not None:
                self.fixture_ip = self.loaded_config.get('fixture_ip')
                self.local_ip = self.loaded_config.get('local_ip')
                self.mqtt_host = self.loaded_config.get('broker_host')
                self.mqtt_port = int(self.loaded_config.get('broker_port'))
                self.mqtt_keepalive = int(self.loaded_config.get('mqtt_keepalive'))
                self.configmsg_path = self.loaded_config.get('config_path')
                self.station_path = self.loaded_config.get('station_path')
                self.fixture_path = self.loaded_config.get('fixture_path')
                self.report_interval = int(self.loaded_config.get('report_interval'))
                self.connect_retry_times = int(self.loaded_config.get('connect_retry_times'))
                self.publish_fail_tolerance = int(self.loaded_config.get('publish_fail_tolerance'))
                self.query_config_topic = self.loaded_config.get('query_config_topic')
            if (not self.get_maincomponent_id()) or (self.maincomponent_id is None):
                self.logger.error("Can not find local address.")
                raise ValueError(f"Parameters initialization failed.")
            else:
                self.logger.info("Load station info success.")
            self.config_topic = TOPIC_HEADER + self.maincomponent_id + self.subcomponent_id + CONFIG_TOPIC_END
            self.analog_topic = TOPIC_HEADER + self.maincomponent_id + self.subcomponent_id + ANALOG_TOPIC_END
            self.logger.info(f"config_topic: {self.config_topic}")
            self.logger.info(f"analog_topic: {self.analog_topic}")
        except Exception as e:
            self.logger.error(f"Initialize parameters fail: {e}.")
            raise ValueError(f"Parameters initialization failed: {e}")
        self.logger.info("All parameters loaded success.")

    def get_maincomponent_id(self) -> bool:
        CELL_TYPE = 'cell_type'
        MAIN_ID_FRONT = "work_station_"
        GH_INFO_KEY = 'ghinfo'
        STATION_NUMBER_KEY = 'STATION_NUMBER'
        STATION_TYPE_KEY = 'STATION_TYPE'

        for interface, addrs in psutil.net_if_addrs().items():
            for addr in addrs:
                if addr.address == self.local_ip:  # case cam station
                    try:
                        with open(self.station_path, 'r') as f:
                            data = json.load(f)
                            if CELL_TYPE in data:
                                self.maincomponent_id = MAIN_ID_FRONT + data[CELL_TYPE]
                                return True
                            else:
                                self.logger.error("Can not find cell type in register.json.")
                                break
                    except Exception as e:
                        self.logger.error(f"Failed to load adc_agent/register.json : {e}.")
                        break
                elif addr.address.startswith(self.fixture_ip):  # case fixture station
                    try:
                        with open(self.fixture_path, 'r') as f:
                            data = json.load(f)
                            if GH_INFO_KEY in data and STATION_NUMBER_KEY in data[GH_INFO_KEY]:
                                self.station_number = data[GH_INFO_KEY][STATION_NUMBER_KEY]
                            else:
                                self.logger.error('Cannot find STATION_NUMBER.')
                                break
                            if GH_INFO_KEY in data and STATION_TYPE_KEY in data[GH_INFO_KEY]:
                                self.station_type = data[GH_INFO_KEY][STATION_TYPE_KEY]
                            else:
                                self.logger.error('Cannot find STATION_TYPE.')
                                break
                    except FileNotFoundError:
                        self.logger.error('File gh_station_info.json not found.')
                        break
                    except json.JSONDecodeError:
                        self.logger.error('An error occurred while decoding the JSON file.')
                        break
                    except Exception as e:
                        self.logger.error(f"An error occurred : {e}.")
                        break
                    self.maincomponent_id = MAIN_ID_FRONT + self.station_type + "_" + self.station_number
                    return True
        return False

    def get_system_name(self):
        try:
            self.system = platform.system()
            self.logger.info(f"System is {self.system}")
        except Exception as e:
            self.logger.error(f"Failed to get system name, error is {e}.")
            raise ValueError(f"Failed to get system name: {e}")

    def get_pc_model(self):
        UNKNOWN_MODEL = "Unknown Model"
        if self.system == 'Windows':
            try:
                self.pc_model = subprocess.check_output('wmic csproduct get name', shell=True).decode().split('\n')[1]\
                    .strip()
            except Exception as e:
                self.logger.error(f"Failed to get PC model: {e}.")
                self.pc_model = UNKNOWN_MODEL
        elif self.system == 'Linux':
            try:
                self.pc_model = subprocess.check_output('sudo dmidecode -s system-product-name', shell=True).decode().\
                    strip()
            except Exception as e:
                self.logger.error(f"Failed to get PC model {e}.")
                self.pc_model = UNKNOWN_MODEL
        elif self.system == "Darwin":
            try:
                result = subprocess.run(['system_profiler', 'SPHardwareDataType'], capture_output=True, text=True)
                lines = result.stdout.split('\n')
                for line in lines:
                    if "Model Name" in line:
                        self.pc_model = line.split(":")[1].strip()
            except Exception as e:
                self.logger.error(f"Failed to get PC model: {e}.")
                self.pc_model = UNKNOWN_MODEL
        else:
            self.pc_model = "Unknown System"
            raise ValueError("Unknown system")
        self.logger.info(f"Model of this PC is {self.pc_model}.")

    def get_total_disk_size(self):
        try:
            for par in psutil.disk_partitions():
                usage = psutil.disk_usage(par.mountpoint)
                self.total_disk += usage.total
        except Exception as e:
            self.logger.error(f"Failed to get total disk size, error:{e}.")
            raise ValueError(f"Failed to get total disk size: {e}")

    def load_configmsg(self):
        try:
            with open(self.configmsg_path, 'r') as f:
                self.config_msg = json.load(f)
            self.logger.info("Config2Send_UsageMonitor.json load success.")
            self.config_msg["hw_config"] = self.pc_model
            self.config_msg["sw_version"] = self.system
        except FileNotFoundError:
            self.logger.error("Config2Send_UsageMonitor.json was not found.")
            raise ValueError(f"Load config message failed")
        except json.JSONDecodeError:
            self.logger.error("An error occurred while decoding the JSON.")
            raise ValueError(f"Load config message failed")
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {e}", exc_info=True)
            raise ValueError(f"Load config message failed: {e}")

    def json_data_init(self):
        self.cpu_message = self.create_json(['name', 'value', 'interval', 'timestamp'],
                                            ['cpu_usage', 0.0, self.report_interval, 0])
        self.ram_message = self.create_json(['name', 'value', 'interval', 'timestamp'],
                                            ['ram_usage', 0.0, self.report_interval, 0])
        self.disk_message = self.create_json(['name', 'value', 'interval', 'timestamp'],
                                             ['disk_usage', 0.0, self.report_interval, 0])

        if self.cpu_message is None and self.ram_message is None and self.disk_message is None:
            self.logger.error("All json data to send initialization failed.")
            raise ValueError("Usage json data initialize fail")

    def create_json(self, index, value):
        try:
            data_dict = dict(zip(index, value))
            self.logger.info("Json file initialized.")
        except TypeError as e:
            self.logger.error(f"TypeError when creating json data: {e}.")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error when creating json data: {e}.")
            return None
        return data_dict

    def mqtt_client_init(self):
        try:
            self.mqtt_client = mqtt.Client(self.local_ip, self.mqtt_port)
            self.logger.info("mqtt client established.")
        except Exception as e:
            self.logger.error(f"Failed to establish mqtt client: {e}")
            raise ValueError(f"MQTT client initialization failed: {e}")
        try:
            self.mqtt_client.on_message = self.on_message
            self.logger.info("Message callback registered.")
        except Exception as e:
            self.logger.error(f"Failed to register message callback: {e}")
            raise ValueError(f"MQTT client initialization failed: {e}")
        try:
            self.mqtt_client.on_connect = self.on_connect
            self.logger.info("Connect callback registered.")
        except Exception as e:
            self.logger.error(f"Failed to register connect callback: {e}")
            raise ValueError(f"MQTT client initialization failed: {e}")

    def mqtt_connect(self):
        retry_count = 0
        while retry_count < self.connect_retry_times:
            retry_count += 1
            try:
                self.mqtt_client.connect(self.mqtt_host, self.mqtt_port, self.mqtt_keepalive)
                self.logger.info("Set mqtt connection parameters.")
                break
            except Exception as e:
                self.logger.error(f"Failed to set mqtt connection parameters: {e}.")
                if retry_count == self.connect_retry_times:
                    self.logger.error(f"Set mqtt connection parameters failed {self.connect_retry_times} time, exit.")
                    raise ValueError("MQTT set connection failed")

        (result, mid) = self.mqtt_client.subscribe(self.query_config_topic)
        if result == mqtt.MQTT_ERR_SUCCESS:
            self.logger.info("Subscribe successfully.")
        elif result == mqtt.MQTT_ERR_NO_CONN:
            self.logger.error("Subscription failed, no connection.")
            raise ValueError("MQTT set connection failed")
        else:
            self.logger.error(f"Subscription failed, error code : {result}.")
            raise ValueError("MQTT set connection failed")

    def scheduled_report_init(self):
        if self.scheduled_report_thread is not None:
            self.logger.error("Scheduled report already initialized.")
            return
        try:
            self.scheduled_report_thread = threading.Thread(name=self.REPORTER_NAME, target=self.scheduled_report)
            self.scheduled_report_thread.setDaemon(True)
            self.logger.info("Scheduled report initialized.")
        except Exception as e:
            raise ValueError(f"Report thread initialization failed: {e}")

    def send_config(self) -> bool:
        self.config_msg['timestamp'] = time.time()
        data2send = json.dumps(self.config_msg)
        if self.mqtt_client:
            try:
                self.mqtt_client.publish(self.config_topic, data2send)
                self.logger.info("Config message published.")
                self.logger.info(f"Config message: {data2send}.")
            except Exception as e:
                self.logger.error(f"Failed to publish message: {e}.")
                return False
        else:
            self.logger.error("Mqtt client not exist.")
            return False
        return True

    def on_message(self, _, __, message):
        if message.topic == self.query_config_topic:
            self.send_config()

    def on_connect(self, _, __, ___, rc):
        if rc == 0:
            self.mqtt_connect_state = True
            self.logger.info("Connected successfully.")
        else:
            self.mqtt_connect_state = False
            self.logger.error(f"Connection failed with error code {rc}.")

    def get_all_disk_usage(self):
        used = 0
        for par in psutil.disk_partitions():
            usage = psutil.disk_usage(par.mountpoint)
            used += usage.used
        return round((used/self.total_disk)*100, 1)

    def update_info(self) -> bool:
        try:
            self.cpu_usage = psutil.cpu_percent(interval=1)
            self.ram_usage = psutil.virtual_memory().percent
            self.disk_usage = self.get_all_disk_usage()
            current_time = time.time()
            self.cpu_message['value'] = self.cpu_usage
            self.cpu_message['timestamp'] = current_time
            self.ram_message['value'] = self.ram_usage
            self.ram_message['timestamp'] = current_time
            self.disk_message['value'] = self.disk_usage
            self.disk_message['timestamp'] = current_time
            cpu_message = json.dumps(self.cpu_message)
            ram_message = json.dumps(self.ram_message)
            disk_message = json.dumps(self.disk_message)
        except psutil.Error as e:
            self.logger.error(f"An error occurred while getting system usage : {e}.")
            return False
        except KeyError as e:
            self.logger.error(f"Failed to update to json data : {e}.")
            return False
        except (TypeError, OverflowError) as e:
            self.logger.error(f"Failed to dump to json file : {e}.")
            return False
        try:
            self.mqtt_client.publish(self.analog_topic, cpu_message)
            self.mqtt_client.publish(self.analog_topic, ram_message)
            self.mqtt_client.publish(self.analog_topic, disk_message)
        except Exception as e:
            self.logger.error(f"Failed to publish : {e}.")
            return False
        self.logger.info(f"Sent, CPU_Usage={self.cpu_usage}, RAM_Usage={self.ram_usage}, Disk_Usage={self.disk_usage}.")
        return True

    def start_scheduled_report(self):
        if self.scheduled_report_thread.is_alive():
            self.logger.error("Report thread is already on.")
            return
        self.logger.info("Starting scheduled report.")
        self.scheduled_report_ready = True
        self.scheduled_report_thread.start()
        self.logger.info("Scheduled report start.")

    def scheduled_report(self):
        self.logger.info("Thread start.")
        update_fail_count = 0
        while self.scheduled_report_ready and self.mqtt_client:
            if self.update_info():
                update_fail_count = 0
            else:
                update_fail_count += 1
                if update_fail_count == self.publish_fail_tolerance:
                    self.scheduled_report_ready = False
            time.sleep(self.report_interval)
        self.logger.info("Thread end.")

    def start(self):
        MQTT_CONN_TIMEOUT = 3
        if self.mqtt_client:
            try:
                self.mqtt_client.loop_start()
                self.logger.info("Mqtt loop started.")
            except Exception as e:
                self.logger.error(f"Mqtt loop start fail: {e}.")
                return
            s_time = time.time()
            while not self.mqtt_connect_state:
                if time.time()-s_time > MQTT_CONN_TIMEOUT:
                    self.logger.error("Mqtt client failed to connect to broker, timeout.")
                    return
                time.sleep(0.1)
            for i in range(0, self.connect_retry_times):
                if self.send_config():
                    break
                elif i == self.connect_retry_times-1:
                    self.logger.error(f"Send config failed {self.connect_retry_times} times.")
                    return
            self.logger.info("First config data sent.")
            self.start_scheduled_report()
        else:
            self.logger.error("Mqtt client not exist.")
        while self.scheduled_report_ready:
            time.sleep(10)


if __name__ == '__main__':
    new = UsageMonitor()
    if new.init_success:
        new.start()
