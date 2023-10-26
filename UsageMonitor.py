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


class UsageMonitor:
    def __init__(self):
        self.init_success = False

        self.logger = logging.getLogger('UsageMonitorLogger')
        self.logger.setLevel(logging.INFO)

        dir_name = '/vault/UsageMonitor/log'
        if not os.path.exists(dir_name):
            # noinspection PyBroadException
            try:
                os.makedirs(dir_name)
            except Exception:
                print("Can not create log file, exit.")
                return
        # noinspection PyBroadException
        try:
            handler = TimedRotatingFileHandler('/vault/UsageMonitor/log/UsageMonitor.log', when='midnight', backupCount=30)
        except Exception:
            print("Logger error, exit.")
            return
        handler.suffix = "%Y-%m-%d"
        formatter = logging.Formatter('%(asctime)s -  %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.info("*************************")
        self.logger.info("Initializing.")

        # load config file
        self.loaded_data = None
        # noinspection PyBroadException
        try:
            with open('/vault/UsageMonitor/config/UsageMonitor_config.json', 'r') as f:
                self.loaded_data = json.load(f)
            self.logger.info("UsageMonitor_config load success.")
        except FileNotFoundError:
            self.logger.error("UsageMonitor_config was not found.")
            return
        except json.JSONDecodeError:
            self.logger.error("An error occurred while decoding the JSON.")
            return
        except Exception:
            self.logger.error("An unexpected error occurred: ", exc_info=True)
            return

        # init parameters
        # noinspection PyBroadException
        try:
            self.station_type = None
            self.station_number = None
            self.client = None
            self.scheduled_report_ready = False
            self.scheduled_report_thread = None
            self.system = None
            self.total_disk = 0
            self.cpu_usage = 0
            self.ram_usage = 0
            self.disk_usage = 0
            self.pc_model = "Unknown Model"
            self.json_data = None
            self.ip_address = None
            self.cpu_message = None
            self.ram_message = None
            self.disk_usage = None
            self.maincomponent_id = None

            for interface, addrs in psutil.net_if_addrs().items():
                for addr in addrs:
                    if addr.address == '10.0.1.200':
                        try:
                            with open('/vault/ADCAgent/dst/setting/adc_agent/register.json', 'r') as f:
                                data = json.load(f)
                                if 'cell_type' in data:
                                    self.maincomponent_id = "work_station_" + data['cell_type']
                                    break
                                else:
                                    self.logger.error("Can not find cell type in register.json.")
                                    return
                        except Exception as e:
                            self.logger.error(f"Failed to load adc_agent/register.json : {e}.")
                            return
                    elif addr.address.startswith('10.0.1.'):
                        try:
                            with open('/vault/data_collection/test_station_config/gh_station_info.json', 'r') as f:
                                data = json.load(f)
                                if 'ghinfo' in data and 'STATION_NUMBER' in data['ghinfo']:
                                    self.station_number = data['ghinfo']['STATION_NUMBER']
                                else:
                                    self.logger.error('Cannot find STATION_NUMBER.')
                                    return
                                if 'ghinfo' in data and 'STATION_TYPE' in data['ghinfo']:
                                    self.station_type = data['ghinfo']['STATION_TYPE']
                                else:
                                    self.logger.error('Cannot find STATION_TYPE.')
                                    return
                        except FileNotFoundError:
                            self.logger.error('File gh_station_info.json not found.')
                            return
                        except json.JSONDecodeError:
                            self.logger.error('An error occurred while decoding the JSON file.')
                            return
                        except Exception as e:
                            self.logger.error(f"An error occurred : {e}.")
                            return
                        self.maincomponent_id = "work_station_" + self.station_type + "_" + self.station_number
                        break
            if self.maincomponent_id is None:
                self.logger.error("Can not find local address.")
                return
            else:
                self.logger.info("Load station info success.")
            subcomponent = "UsageMonitor"
            self.config_topic = "/Devices/" + self.maincomponent_id + "/" + subcomponent + "/" + "Config"
            self.analog_topic = "/Devices/" + self.maincomponent_id + "/" + subcomponent + "/" + "Analog"
            self.logger.info(f"config_topic: {self.config_topic}")
            self.logger.info(f"analog_topic: {self.analog_topic}")

            if self.loaded_data is None:
                self.query_config_topic = "/Devices/adc_agent/QueryConfig"
                self.broker = "10.0.1.200"
                self.port = 1883
                self.config_path = "Config2Send_UsageMonitor.json"
                self.report_interval = 5
                self.connect_retry_times = 3
                self.publish_fail_tolerance = 5
            else:
                self.query_config_topic = self.loaded_data.get('query_config_topic', "/Devices/adc_agent/QueryConfig")
                self.broker = self.loaded_data.get('broker', "10.0.1.200")
                self.port = int(self.loaded_data.get('broker_port', "1883"))
                self.config_path = self.loaded_data.get('config_path', "Config2Send_Vacuum.json")
                self.report_interval = int(self.loaded_data.get('report_interval', "5"))
                self.connect_retry_times = int(self.loaded_data.get('connect_retry_times', "3"))
                self.publish_fail_tolerance = int(self.loaded_data.get('publish_fail_tolerance', "5"))
        except Exception:
            self.logger.error("Initialize parameters fail.")
            return
        self.logger.info("All parameters loaded success.")

        # get system name
        try:
            self.system = platform.system()
            self.logger.info(f"System is {self.system}")
        except Exception as e:
            self.logger.error(f"Failed to get system name, error is {e}.")
            return

        # get pc model
        self.get_pc_model()
        if not self.get_total_disk_size():
            return

        # load config_data
        # noinspection PyBroadException
        try:
            with open(self.config_path, 'r') as f:
                self.config_data = json.load(f)
            self.logger.info("Config2Send_UsageMonitor.json load success.")
        except FileNotFoundError:
            self.logger.error("Config2Send_UsageMonitor.json was not found.")
            return
        except json.JSONDecodeError:
            self.logger.error("An error occurred while decoding the JSON.")
            return
        except Exception:
            self.logger.error("An unexpected error occurred: ", exc_info=True)
            return
        self.config_data["hw_config"] = self.pc_model
        self.config_data["sw_version"] = self.system

        # init json to send
        self.cpu_message = self.create_json(['name', 'value', 'interval', 'timestamp'],
                                            ['cpu_usage', 0.0, self.report_interval, 0])
        self.ram_message = self.create_json(['name', 'value', 'interval', 'timestamp'],
                                            ['ram_usage', 0.0, self.report_interval, 0])
        self.disk_message = self.create_json(['name', 'value', 'interval', 'timestamp'],
                                             ['disk_usage', 0.0, self.report_interval, 0])

        if self.cpu_message is None and self.ram_message is None and self.disk_message is None:
            self.logger.error("All json data to send initialization failed.")
            return

        # init mqtt
        if not self.mqtt_client_init():
            return
        if not self.mqtt_connect():
            return

        self.scheduled_report_init()
        self.scheduled_report_ready = True
        self.init_success = True

    def get_pc_model(self):
        if self.system == 'Windows':
            # noinspection PyBroadException
            try:
                self.pc_model = subprocess.check_output('wmic csproduct get name', shell=True).decode().split('\n')[1]\
                    .strip()
            except Exception:
                self.logger.error("Failed to get PC model.")
                self.pc_model = "Unknown Model"
        elif self.system == 'Linux':
            # noinspection PyBroadException
            try:
                self.pc_model = subprocess.check_output('sudo dmidecode -s system-product-name', shell=True).decode().\
                    strip()
            except Exception:
                self.logger.error("Failed to get PC model.")
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
                self.logger.error("Failed to get PC model.")
                self.pc_model = "Unknown Model"
        else:
            self.pc_model = "Unknown System"
        self.logger.info(f"Model of this PC is {self.pc_model}.")

    def get_total_disk_size(self) -> bool:
        try:
            for par in psutil.disk_partitions():
                usage = psutil.disk_usage(par.mountpoint)
                self.total_disk += usage.total
            return True
        except Exception as e:
            self.logger.error(f"Failed to get total disk size, error:{e}.")
            return False

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

    def mqtt_client_init(self) -> bool:
        try:
            self.client = mqtt.Client(self.broker, self.port)
            self.logger.info("mqtt client established.")
        except Exception as e:
            self.logger.error(f"Failed to establish mqtt client: {e}")
            return False
        try:
            self.client.on_message = self.on_message
            self.logger.info("Message callback registered.")
        except Exception as e:
            self.logger.error(f"Failed to register message callback: {e}")
            return False
        try:
            self.client.on_connect = self.on_connect
            self.logger.info("Connect callback registered.")
        except Exception as e:
            self.logger.error(f"Failed to register connect callback: {e}")
            return False
        return True

    def mqtt_connect(self) -> bool:
        retry_count = 0
        while retry_count < self.connect_retry_times:
            retry_count += 1
            try:
                self.client.connect("localhost", self.port, 60)
                self.logger.info("Connect to broker success.")
                break
            except Exception as e:
                self.logger.error(f"Failed to connect to the broker: {e}, trial {retry_count}.")
                if retry_count == self.connect_retry_times:
                    return False
                time.sleep(1)

        (result, mid) = self.client.subscribe('/Devices/adc_agent/QueryConfig')
        if result == mqtt.MQTT_ERR_SUCCESS:
            self.logger.info("Subscribe successfully.")
        elif result == mqtt.MQTT_ERR_NO_CONN:
            self.logger.error(f"Subscription failed, no connection.")
            return False
        else:
            self.logger.error(f"Subscription failed, error code : {result}.")
            return False
        return True

    def scheduled_report_init(self):
        if self.scheduled_report_thread is not None:
            self.logger.error("Scheduled report already initialized.")
            self.scheduled_report_thread.setDaemon(True)
            return
        self.scheduled_report_thread = threading.Thread(target=self.scheduled_report)
        self.scheduled_report_thread.setDaemon(True)
        self.logger.info("Scheduled report initialized.")

    def send_config(self) -> bool:
        self.config_data['timestamp'] = time.time()
        data2send = json.dumps(self.config_data)
        if self.client:
            try:
                self.client.publish(self.config_topic, data2send)
                self.logger.info("Config message published.")
                self.logger.info(f"Config message: {data2send}.")
            except Exception as e:
                self.logger.error(f"Failed to publish message: {e}.")
                return False
        else:
            self.logger.error("Mqtt client not exist.")
            return False
        return True

    def on_message(self, client, userdata, message):
        if message.topic == self.query_config_topic:
            self.send_config()

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.logger.info("Connected successfully.")
        else:
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
            self.client.publish(self.analog_topic, cpu_message)
            self.client.publish(self.analog_topic, ram_message)
            self.client.publish(self.analog_topic, disk_message)
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
        while self.scheduled_report_ready:
            if self.update_info():
                update_fail_count = 0
            else:
                update_fail_count += 1
                if update_fail_count == self.publish_fail_tolerance:
                    self.scheduled_report_ready = False
            time.sleep(self.report_interval)
        self.logger.info("Thread end.")

    def start(self):
        if self.client:
            self.client.loop_start()
            self.logger.info("Mqtt loop started.")
            for i in range(0, self.connect_retry_times):
                if self.send_config():
                    break
                elif i == self.connect_retry_times-1:
                    self.logger.error(f"Send config failed {self.connect_retry_times} times.")
                    return False
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
