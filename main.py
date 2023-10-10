
import paho.mqtt.client as mqtt
import json
import socket
import time
import logging
# import psutil
import platform
import subprocess


class Vacuum:
    def __init__(self, maincomponent_id, subcomponent, broker="localhost", port=1883):
        self.init_success = False
        logging.basicConfig(filename='vacuum.log', level=logging.INFO, format='%(asctime)s %(message)s')
        logging.info("Initializing")

        # load config file
        # noinspection PyBroadException
        try:
            with open("vacuum_config.json", 'r') as f:
                self.loaded_data = json.load(f)
            logging.info("vacuum_config load success.")
        except FileNotFoundError:
            logging.error("vacuum_config was not found.")
            return
        except json.JSONDecodeError:
            logging.error("An error occurred while decoding the JSON.")
            return
        except Exception as e:
            logging.error("An unexpected error occurred: ", exc_info=True)
            return
        # init parameters
        self.config_topic = "/Devices/" + maincomponent_id + "/" + subcomponent + "/" + "Config"
        self.query_config_topic = "/Devices/adc_agent/QueryConfig"
        self.analog_topic = "/Devices/" + maincomponent_id + "/" + subcomponent + "/" + "Analog"
        self.last_time = int(time.time())
        self.current_time = 0
        self.target_address = self.loaded_data.get('target_address', None)  # default:"192.168.3.250"
        self.start_port = int(self.loaded_data.get('start_port', None))
        self.end_port = int(self.loaded_data.get('end_port', None))
        self.config_path = self.loaded_data.get('config_path', None)
        self.port_in_use = 0
        self.connection_state = False
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
        logging.info("All parameters loaded success.")

        # load config_data
        # noinspection PyBroadException
        try:
            with open(self.config_path, 'r') as f:
                self.config_data = json.load(f)
            logging.info("Config.json load success.")
        except FileNotFoundError:
            logging.error("Config.json was not found.")
            return
        except json.JSONDecodeError:
            logging.error("An error occurred while decoding the JSON.")
            return
        except Exception as e:
            logging.error("An unexpected error occurred: ", exc_info=True)
            return

        # connect to plc
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except socket.error as e:
            logging.error(f"Failed to create a socket. Error: {e}")
            return
        self.connect_to_target()
        # todo: maybe need to keep alive

        self.client = mqtt.Client(broker, port)
        logging.info("mqtt client established.")
        self.client.on_message = self.on_message
        logging.info("message callback registered.")
        # noinspection PyBroadException
        try:
            self.client.connect("localhost", 1883, 60)
        except Exception as e:
            logging.error(f"Failed to connect to the broker.")
        (subscribe_result, mid) = self.client.subscribe("/Devices/adc_agent/QueryConfig")
        # self.client.subscribe("")   add and to result for different subscribe
        if subscribe_result == 0:
            logging.info("subscribe success.")
        else:
            logging.error(f"Failed to subscribe. Result code: {subscribe_result}")
        self.client.loop_start()
        logging.info("loop started.")
        self.init_success = True

    def update_json(self, data):
        self.current_time = int(time.time())
        with open('Analog.json', 'r+') as f:
            json_data = json.load(f)
            json_data['value'] = float(data)
            json_data['interval'] = self.current_time - self.last_time
            json_data['timestamp'] = self.current_time
            f.seek(0)
            json.dump(json_data, f)
            f.truncate()
        self.last_time = self.current_time

    @staticmethod
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected successfully.")
        else:
            logging.error(f"Connection failed with error code {rc}.")
            return

    def on_message(self, client, userdata, message):
        if message.topic == '/Devices/adc_agent/QueryConfig':  # query config
            client.publish(self.config_topic, self.config_data)
        elif message.topic == '' or message.topic == '':  # suck or release
            self.sock.send("check_analog")  # to plc or to robot //plc=192.168.3.250
            data = self.sock.recv(1024)
            self.update_json(data)  # update json
            with open('Analog.json', 'r') as f:
                json_data = json.load(f)
                client.publish(self.analog_topic, json_data)

    def connect_to_target(self):
        self.port_in_use = 0
        logging.info("Starting to connecting to plc.")
        for p in range(self.start_port, self.end_port + 1):
            try:
                logging.info(f"Starting to connecting to port{p}.")
                self.sock.connect((self.target_address, p))
                # todo: check why takes 20s here
                self.port_in_use = p
                break
            except socket.error as e:
                logging.error(f"Port {p} fail.")
        if self.port_in_use == 0:
            logging.error("All port failed.")
            return
        self.update_connection_state()
        logging.info(f"Connect to {self.target_address}:{self.port_in_use}.")

    def update_connection_state(self):
        if self.sock.fileno() == -1:
            self.connection_state = False
        else:
            self.connection_state = True

    def check_connection_loop(self):
        while True:
            self.update_connection_state()
            if not self.connection_state:
                logging.info("Connection closed, retrying.")
                self.connect_to_target()
            # try:
            #     self.sock.send("123\r\n")
            # except socket.error:
            #     logging.info("Connection closed, retrying.")
            #     self.connect_to_target()
            time.sleep(10)

    def scheduled_report(self):
        pass


class PlatformInfo:
    def __init__(self, maincomponent_id, subcomponent, broker="localhost", port=1883):
        self.init_success = False
        logging.basicConfig(filename='platform.log', level=logging.INFO, format='%(asctime)s %(message)s')
        logging.info("Initializing")
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
            except Exception as e:
                self.pc_model = "Unknown Model"
        elif self.system == 'Linux':
            # noinspection PyBroadException
            try:
                self.pc_model = subprocess.check_output('sudo dmidecode -s system-product-name', shell=True).decode().\
                    strip()
            except Exception as e:
                self.pc_model = "Unknown Model"
        elif self.system == "Darwin":
            # noinspection PyBroadException
            try:
                result = subprocess.run(['system_profiler', 'SPHardwareDataType'], capture_output=True, text=True)
                lines = result.stdout.split('\n')
                for line in lines:
                    if "Model Name" in line:
                        self.pc_model = line.split(":")[1].strip()
            except Exception as e:
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
            logging.error(f"Failed to connect to the broker.")
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

    def update_info(self):
        # self.cpu_usage = psutil.cpu_percent(interval=1)
        # self.ram_usage = psutil.virtual_memory().percent
        # self.disk_usage = psutil.disk_usage('/').percent
        self.json_data['CPU_Usage'] = self.cpu_usage
        self.json_data['RAM_Usage'] = self.ram_usage
        self.json_data['Disk_Usage'] = self.disk_usage
        json_data = json.dumps(self.json_data)
        self.client.publish("/topic/ReportUsage", json_data)

    def create_json(self, index, value):
        data_dict = dict(zip(index, value))
        self.json_data = data_dict

    def on_message(self, client, userdata, message):
        if message.topic == 'topic/QueryUsage':
            self.update_info()

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected successfully.")
        else:
            logging.error(f"Connection failed with error code {rc}.")
            return


if __name__ == '__main__':
    # new = PlatformInfo("insider_transfer_DVI_1", "vacuum_1")
    # if new.init_success:
    #     while True:
    #         new.update_info()
    #         time.sleep(new.interval)
    new = Vacuum("insider_transfer_DVI_1", "vacuum_1")
