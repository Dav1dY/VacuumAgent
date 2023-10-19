import paho.mqtt.client as mqtt
import json
import time
import logging
import platform
import subprocess
import threading
import psutil


class SystemInfo:
    def __init__(self, maincomponent_id, subcomponent):
        self.init_success = False
        logging.basicConfig(filename='platform.log', level=logging.INFO, format='%(asctime)s %(message)s')
        logging.info("Initializing.")

        # load config file
        self.loaded_data = None
        # noinspection PyBroadException
        try:
            with open("SystemInfo_config.json", 'r') as f:
                self.loaded_data = json.load(f)
            logging.info("SystemInfo_config load success.")
        except FileNotFoundError:
            logging.error("SystemInfo_config was not found.")
            return
        except json.JSONDecodeError:
            logging.error("An error occurred while decoding the JSON.")
            return
        except Exception:
            logging.error("An unexpected error occurred: ", exc_info=True)
            return

        # init parameters
        # noinspection PyBroadException
        try:
            self.config_topic = "/Devices/" + maincomponent_id + "/" + subcomponent + "/" + "Config"
            self.analog_topic = "/Devices/" + maincomponent_id + "/" + subcomponent + "/" + "SystemInfo"
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
            if self.loaded_data is None:
                self.query_config_topic = "/Devices/adc_agent/QueryConfig"
                self.broker = "10.0.1.200"
                self.port = 1883
                self.config_path = "Config.json"
                self.report_interval = 5
                self.connect_retry_times = 3
                self.publish_fail_tolerance = 5
            else:
                self.query_config_topic = self.loaded_data.get('query_config_topic', "/Devices/adc_agent/QueryConfig")
                self.broker = self.loaded_data.get('broker', "10.0.1.200")
                self.port = int(self.loaded_data.get('broker_port', "1883"))
                self.config_path = self.loaded_data.get('config_path', "Config.json")
                self.report_interval = int(self.loaded_data.get('report_interval', "5"))
                self.connect_retry_times = int(self.loaded_data.get('connect_retry_times', "3"))
                self.publish_fail_tolerance = int(self.loaded_data.get('publish_fail_tolerance', "5"))
        except Exception:
            logging.error("Initialize parameters fail.")
            return
        logging.info("All parameters loaded success.")

        # get system name
        try:
            self.system = platform.system()
            logging.info(f"System is {self.system}")
        except Exception as e:
            logging.error(f"Failed to get system name, error is {e}.")
            return

        # get pc model
        self.get_pc_model()
        if not self.get_total_disk_size():
            return

        # init json data
        self.create_json(['Platform', 'Model', 'CPU_Usage', 'RAM_Usage', 'Disk_Usage'], [self.system,
                                                                                         self.pc_model,
                                                                                         self.cpu_usage,
                                                                                         self.ram_usage,
                                                                                         self.disk_usage])
        if self.json_data is None:
            logging.error("Json data initialization failed.")
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
                logging.error("Failed to get PC model.")
                self.pc_model = "Unknown Model"
        elif self.system == 'Linux':
            # noinspection PyBroadException
            try:
                self.pc_model = subprocess.check_output('sudo dmidecode -s system-product-name', shell=True).decode().\
                    strip()
            except Exception:
                logging.error("Failed to get PC model.")
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
                logging.error("Failed to get PC model.")
                self.pc_model = "Unknown Model"
        else:
            self.pc_model = "Unknown System"
        logging.info(f"Model of this PC is {self.pc_model}.")

    def get_total_disk_size(self) ->bool:
        try:
            for par in psutil.disk_partitions():
                usage = psutil.disk_usage(par.mountpoint)
                self.total_disk += usage.total
            return True
        except Exception as e:
            logging.error(f"Failed to get total disk size, error:{e}.")
            return False

    def create_json(self, index, value):
        try:
            data_dict = dict(zip(index, value))
            logging.info("Json file initialized.")
        except TypeError as e:
            logging.error(f"TypeError : {e}.")
            return
        except Exception as e:
            logging.error(f"Unexpected error : {e}.")
            return
        self.json_data = data_dict

    def mqtt_client_init(self) -> bool:
        try:
            self.client = mqtt.Client(self.broker, self.port)
            logging.info("mqtt client established.")
        except Exception as e:
            logging.error(f"Failed to establish mqtt client: {e}")
            return False
        try:
            self.client.on_message = self.on_message
            logging.info("Message callback registered.")
        except Exception as e:
            logging.error(f"Failed to register message callback: {e}")
            return False
        try:
            self.client.on_connect = self.on_connect
            logging.info("Connect callback registered.")
        except Exception as e:
            logging.error(f"Failed to register connect callback: {e}")
            return False
        return True

    def mqtt_connect(self) -> bool:
        retry_count = 0
        while retry_count < self.connect_retry_times:
            retry_count += 1
            try:
                self.client.connect("localhost", self.port, 60)
                logging.info("Connect to broker success.")
                break
            except Exception as e:
                logging.error(f"Failed to connect to the broker: {e}, trial {retry_count}.")
                if retry_count == self.connect_retry_times:
                    return False
                time.sleep(1)

        (result, mid) = self.client.subscribe('/topic/QueryUsage')
        if result == mqtt.MQTT_ERR_SUCCESS:
            logging.info("Subscribe successfully.")
        elif result == mqtt.MQTT_ERR_NO_CONN:
            logging.error(f"Subscription failed, no connection.")
            return False
        else:
            logging.error(f"Subscription failed, error code : {result}.")
            return False
        return True

    def scheduled_report_init(self):
        if self.scheduled_report_thread is not None:
            logging.error("Scheduled report already initialized.")
            self.scheduled_report_thread.setDaemon(True)
            return
        self.scheduled_report_thread = threading.Thread(target=self.scheduled_report)
        self.scheduled_report_thread.setDaemon(True)
        logging.info("Scheduled report initialized.")

    def on_message(self, client, userdata, message):
        if message.topic == 'topic/QueryUsage':
            self.update_info()

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected successfully.")
        else:
            logging.error(f"Connection failed with error code {rc}.")

    @staticmethod
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
            # self.disk_usage = psutil.disk_usage('/').percent
            self.disk_usage = self.get_all_disk_usage(self)
            self.json_data['CPU_Usage'] = self.cpu_usage
            self.json_data['RAM_Usage'] = self.ram_usage
            self.json_data['Disk_Usage'] = self.disk_usage
            json_data = json.dumps(self.json_data)
        except psutil.Error as e:
            logging.error(f"An error occurred while getting system usage : {e}.")
            return False
        except KeyError as e:
            logging.error(f"Failed to update to json data : {e}.")
            return False
        except (TypeError, OverflowError) as e:
            logging.error(f"Failed to dump to json file : {e}.")
            return False
        try:
            self.client.publish("/topic/ReportUsage", json_data)
        except Exception as e:
            logging.error(f"Failed to publish : {e}.")
            return False
        logging.info(f"Update, CPU_Usage={self.cpu_usage}, RAM_Usage={self.ram_usage}, Disk_Usage={self.disk_usage}.")
        return True

    def start_scheduled_report(self):
        if self.scheduled_report_thread.is_alive():
            logging.error("Report thread is already on.")
            return
        logging.info("Starting scheduled report.")
        self.scheduled_report_ready = True
        self.scheduled_report_thread.start()
        logging.info("Scheduled report start.")

    def scheduled_report(self):
        logging.info("Thread start.")
        update_fail_count = 0
        while self.scheduled_report_ready:
            if self.update_info():
                update_fail_count = 0
            else:
                update_fail_count += 1
                if update_fail_count == self.publish_fail_tolerance:
                    self.scheduled_report_ready = False
            time.sleep(self.report_interval)
        logging.info("Thread end.")

    def start(self):
        if self.client:
            self.client.loop_start()
            logging.info("Mqtt loop started.")
            self.start_scheduled_report()
        else:
            logging.error("Mqtt client not exist.")
        while self.scheduled_report_ready:
            pass


if __name__ == '__main__':
    new = SystemInfo("insider_transfer_DVI_1", "pc_1")
    if new.init_success:
        new.start()
