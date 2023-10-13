
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
        except Exception:
            logging.error("An unexpected error occurred: ", exc_info=True)
            return

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
        except Exception:
            logging.error("An unexpected error occurred: ", exc_info=True)
            return
        self.config_data = json.dumps(self.config_data)

        # init socket client
        if not self.socket_init():
            return
        if not self.socket_connect():
            return
        # todo: may need to keep alive

        # init mqtt
        if not self.mqtt_client_init():
            return
        if not self.mqtt_connect():
            return

        self.scheduled_report_ready = True
        self.init_success = True

    def start(self):
        if self.client:
            self.client.loop_start()
            logging.info("Mqtt loop started.")
        else:
            logging.error("Mqtt client not exist.")

    def socket_connect(self) -> bool:
        retry_times = 0
        while retry_times < self.connect_retry_times:
            if not self.sock:
                if not self.socket_init():
                    return False
            retry_times += 1
            logging.info(f"Retry time = {retry_times}.")
            if self.connect_to_target():
                if self.is_socket_connected():
                    return True
            # time.sleep(1)
            self.sock = None
        logging.error("Socket connect fail.")
        return False
    # todo: may add retry here or outside

    def mqtt_connect(self) -> bool:
        try:
            self.client.connect("localhost", 1883, 60)
            logging.info("Connect to broker success.")
        except Exception as e:
            logging.error(f"Failed to connect to the broker: {e}.")
            return False

        (subscribe_result, mid) = self.client.subscribe("/Devices/adc_agent/QueryConfig")
        # self.client.subscribe("")   add more topics
        if subscribe_result == 0:
            logging.info("subscribe success.")
        else:
            logging.error(f"Failed to subscribe. Result code: {subscribe_result}")
            return False
        return True
    # todo: may add retry here or outside

    @staticmethod
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected successfully.")
        else:
            logging.error(f"Connection failed with error code {rc}.")

    def on_message(self, client, userdata, message):
        if message.topic == '/Devices/adc_agent/QueryConfig':  # query config
            if self.client:
                try:
                    client.publish(self.config_topic, self.config_data)
                    logging.info("Config message published.")
                except Exception as e:
                    logging.error(f"Failed to publish message: {e}.")
            else:
                logging.error("Mqtt client not exist.")
        elif message.topic == '' or message.topic == '':  # suck or release
            message = "CHECK_ANALOG"  # todo: change to check analog cmd
            message = message.encode()
            if self.sock:
                try:
                    if self.socket_send(message):
                        logging.info("Command sent.")
                    else:
                        logging.error("Failed to send command.")
                except Exception as e:
                    logging.error(f"Failed to send command: {e}")
                    return

                data = None
                try:
                    ready = select.select([self.sock], [], [], self.socket_timeout)
                    if ready[0]:
                        data = self.sock.recv(1024)
                        logging.info("Reply received.")
                    else:
                        logging.error("Receive operation timed out.")
                except Exception as e:
                    logging.error(f"Failed to receive analog: {e}")

                self.update_json(data)  # update json

                if self.update_state:
                    try:
                        with open('Analog.json', 'r') as f:
                            json_data = json.dumps(json.load(f))
                            if self.client:
                                try:
                                    client.publish(self.analog_topic, json_data)
                                    logging.info("Analog message published.")
                                except Exception as e:
                                    logging.error(f"Failed to publish message: {e}")
                            else:
                                logging.error("Mqtt client not exist.")
                    except FileNotFoundError:
                        logging.error("The file 'Analog.json' was not found.")
                    except json.JSONDecodeError:
                        logging.error("An error occurred while decoding the JSON.")
                    except Exception as e:
                        logging.error(f"An unexpected error occurred: {e}")

    def socket_init(self) -> bool:
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            return True
        except socket.error as e:
            logging.error(f"Failed to create a socket. Error: {e}")
            return False

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

    def connect_to_target(self) -> bool:
        self.port_in_use = 0
        logging.info("Starting to connecting to plc.")

        for p in range(self.start_port, self.end_port + 1):
            try:
                logging.info(f"Starting to connecting to {self.target_address}:{p}.")
                self.sock.connect((self.target_address, p))
                # todo: figure out why takes 20s here
                self.port_in_use = p
                break
            except socket.error:
                logging.error(f"Port {p} fail.")
        if self.port_in_use == 0:
            logging.error("All port failed.")
            return False
        logging.info(f"Connect to {self.target_address}:{self.port_in_use}.")
        return True

    def is_socket_connected(self) -> bool:
        # check self.sock before call this
        try:
            self.sock.sendall(b'')
            # todo: check if this would cause problems on server
            return True
        except socket.error:
            return False
        # try:
        #     ready_to_read, _, _ = select.select([self.sock], [], [], 0)
        # except select.error as e:
        #     logging.error(f"Select error: {e}")
        #     return False
        #
        # if ready_to_read:
        #     try:
        #         self.sock.setblocking(False)
        #         data = self.sock.recv(16, socket.MSG_PEEK)
        #         self.sock.setblocking(True)
        #     except socket.error as e:
        #         logging.error(f"Socket error: {e}")
        #         return False
        #     return len(data) > 0
        # return False

    def update_json(self, data):
        self.update_state = False

        if data:
            try:
                data = data.decode('utf-8')
            except UnicodeDecodeError as e:
                logging.error(f"Failed to decode message: {e}.")
                return

            try:
                match = re.search('UPDATE_ANALOG,(\\d+)', data)
                if match:
                    data = int(match.group(1))
                else:
                    logging.error("Receive bad message.")
                    return
            except ValueError:
                logging.error("Receive bad message.")
                return
        else:
            # case that socket receive timeout and have no data send back
            data = 0

        self.current_time = int(time.time())

        try:
            with open('Analog.json', 'r+') as f:
                json_data = json.load(f)
                json_data['value'] = float(data/4000)
                json_data['interval'] = self.current_time - self.last_time
                json_data['timestamp'] = self.current_time
                f.seek(0)
                json.dump(json_data, f)
                f.truncate()
        except FileNotFoundError:
            logging.error("The file 'Analog.json' was not found.")
            return
        except json.JSONDecodeError:
            logging.error("An error occurred while decoding the JSON.")
            return
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            return

        self.last_time = self.current_time
        self.update_state = True

    def scheduled_report(self):
        logging.info("Thread start.")
        message = "CHECK_ANALOG"  # todo: change to check analog cmd
        message = message.encode()
        last_time = time.time()
        while self.sock and self.client:
            # todo: may need add mqtt retry here
            try:
                if self.socket_send(message):
                    logging.info("Command sent.")
                # self.sock.send(message)  # to plc or to robot //plc=192.168.3.250
                else:
                    logging.error("Failed to send command.")
                    return
            except Exception as e:
                logging.error(f"Failed to send command: {e}.")
                return

            data = None
            try:
                ready = select.select([self.sock], [], [], self.socket_timeout)
                if ready[0]:
                    data = self.sock.recv(1024)
                    logging.info("Reply received.")
                else:
                    logging.error("Receive operation timed out.")
            except Exception as e:
                logging.error(f"Failed to receive analog: {e}")

            self.update_json(data)  # update json

            if self.update_state:
                try:
                    with open('Analog.json', 'r') as f:
                        json_data = json.dumps(json.load(f))
                        if self.client:
                            try:
                                self.client.publish(self.analog_topic, json_data)
                                logging.info("Analog message published.")
                            except Exception as e:
                                logging.error(f"Failed to publish message: {e}")
                        else:
                            logging.error("Mqtt client not exist.")
                except FileNotFoundError:
                    logging.error("The file 'Analog.json' was not found.")
                except json.JSONDecodeError:
                    logging.error("An error occurred while decoding the JSON.")
                except Exception as e:
                    logging.error(f"An unexpected error occurred: {e}")

            while time.time()-last_time <= self.report_interval:
                time.sleep(1)
            last_time = time.time()
        logging.info("Thread end.")

    def socket_send(self, message) -> bool:
        if not self.sock:
            logging.error("Socket client not exist.")
            return False
        retry_times = 0
        if not self.is_socket_connected():
            logging.info(f"Connection closed, will start retry.")
            if not self.socket_connect():
                logging.error("Send fail, socket can not connect.")
                return False

        try:
            self.sock.sendall(message)  # to plc or to robot //plc=192.168.3.250
            logging.info("Command sent.")
            return True
        except Exception as e:
            logging.error(f"Failed to send command: {e}")
            return False


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
    new = Vacuum("insider_transfer_DVI_1", "vacuum_1")
    if new.init_success:
        if new.socket_connect():
            if new.mqtt_connect():
                # new.start()
                # new_thread = threading.Thread(target=new.scheduled_report())
                # new_thread.setDaemon(True)
                # new_thread.start()
                # pass
                while new.scheduled_report_ready:
                    pass


