import paho.mqtt.client as mqtt
import json
import socket
import time
import logging
from logging.handlers import TimedRotatingFileHandler
import re
import threading
import select
import os
import psutil
import atexit


# todo: 1.remove reconnect from socket_send to outer layer 2.compare on_message and s_report(not important)
class Vacuum:
    def __init__(self):
        self.init_success = False

        # Init logger
        self.logger = logging.getLogger('VacuumLogger')
        self.logger.setLevel(logging.INFO)
        dir_name = '/vault/VacuumMonitor/log'
        if not os.path.exists(dir_name):
            try:
                os.makedirs(dir_name)
            except Exception as e:
                print(f"Can not create log file: {e}, exit.")
                return
        try:
            handler = TimedRotatingFileHandler(dir_name+'/VacuumMonitor', when='midnight', backupCount=30)
            handler.suffix = "%Y-%m-%d.log"
            formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        except Exception as e:
            print(f"Logger error: {e}, exit.")
            return
        self.logger.info("************************************************")
        self.logger.info("Initializing.")

        # Load config file
        self.loaded_data = None
        try:
            with open("/vault/VacuumMonitor/config/vacuum_config.json", 'r') as f:
                self.loaded_data = json.load(f)
            self.logger.info("vacuum_config load success.")
        except FileNotFoundError:
            self.logger.error("vacuum_config was not found.")
            return
        except json.JSONDecodeError:
            self.logger.error("An error occurred while decoding the JSON.")
            return
        except Exception as e:
            self.logger.error(f"An unexpected error occurred when loading config file: {e}", exc_info=True)
            return

        # Init parameters
        try:
            self.query_config_topic = "/Devices/adc_agent/QueryConfig"
            self.last_time = int(time.time())
            self.current_time = 0
            self.port_in_use = 0
            self.connection_state = False
            self.update_state = False
            self.sock = None
            self.mqtt_client = None
            self.scheduled_report_ready = False
            self.scheduled_report_thread = None
            self.maincomponent_id = None
            self.subcomponent_id = "VacuumMonitor"
            self.protocol_sn = 1
            if (not self.get_local_ip()) or (self.maincomponent_id is None):
                self.logger.error("Can not find local address.")
                return
            else:
                self.logger.info("Load station info success.")
            self.config_topic = "/Devices/" + self.maincomponent_id + "/" + self.subcomponent_id + "/" + "Config"
            self.analog_topic = "/Devices/" + self.maincomponent_id + "/" + self.subcomponent_id + "/" + "Analog"
            self.logger.info(f"config_topic: {self.config_topic}")
            self.logger.info(f"analog_topic: {self.analog_topic}")
            if self.loaded_data is not None:
                self.mqtt_broker = self.loaded_data.get('broker', "10.0.1.200")
                self.mqtt_port = int(self.loaded_data.get('broker_port', "1883"))
                self.target_address = self.loaded_data.get('target_address', "10.0.1.202")
                self.start_port = int(self.loaded_data.get('start_port', "4096"))
                self.end_port = int(self.loaded_data.get('end_port', "4101"))
                self.config_path = self.loaded_data.get('config_path', "Config2Send_Vacuum.json")
                self.report_interval = int(self.loaded_data.get('report_interval', "5"))
                self.connect_retry_times = int(self.loaded_data.get('connect_retry_times', "3"))
                self.socket_timeout = int(self.loaded_data.get('socket_timeout', "3"))
            else:
                self.mqtt_broker = "10.0.1.200"
                self.mqtt_port = 1883
                self.target_address = "10.0.1.202"
                self.start_port = 4096
                self.end_port = 4101
                self.config_path = "Config2Send_Vacuum.json"
                self.report_interval = 5
                self.connect_retry_times = 3
                self.socket_timeout = 3
        except Exception as e:
            self.logger.error(f"Initialize parameters fail: {e}.")
            return
        self.logger.info("All parameters loaded success.")

        # Load config2send file
        try:
            with open(self.config_path, 'r') as f:
                self.config_data = json.load(f)
            self.logger.info("Config2Send_Vacuum.json load success.")
        except FileNotFoundError:
            self.logger.error("Config2Send_Vacuum.json was not found.")
            return
        except json.JSONDecodeError:
            self.logger.error("An error occurred while decoding the JSON.")
            return
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {e}", exc_info=True)
            return

        # init socket client
        if not self.socket_init():
            return
        if not self.socket_connect_with_retry():
            return
        try:
            atexit.register(self.clean_up)
        except Exception as e:
            self.logger.error(f"Failed to register cleanup function: {e}.")
        self.logger.info("Cleanup function registered.")

        # init mqtt
        if not self.mqtt_client_init():
            return
        if not self.mqtt_connect():
            return

        self.start_scheduled_init()
        self.scheduled_report_ready = True
        self.init_success = True
        self.logger.info("All init done.")

    def get_local_ip(self) -> bool:
        for interface, addrs in psutil.net_if_addrs().items():
            for addr in addrs:
                if addr.address == '10.0.1.200':
                    try:
                        with open('/vault/ADCAgent/dst/setting/adc_agent_register.json', 'r') as f:
                            data = json.load(f)
                            if 'cell_type' in data:
                                self.maincomponent_id = "work_station_" + data['cell_type']
                                return True
                            else:
                                self.logger.error("Can not find cell type in register.json.")
                                break
                    except Exception as e:
                        self.logger.error(f"Failed to load adc_agent_register.json : {e}.")
                        break
        return False

    def socket_init(self) -> bool:
        try:
            self.sock = None
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            return True
        except socket.error as e:
            self.logger.error(f"Failed to create a socket. Error: {e}")
            return False

    def socket_connect_with_retry(self) -> bool:
        retry_times = 0
        if not self.connect_retry_times or 0 == self.connect_retry_times:
            self.logger.error("Parameter connect_retry_times not set.")
            return False
        while retry_times < self.connect_retry_times:
            if not self.sock:
                if not self.socket_init():
                    return False
            retry_times += 1
            self.logger.info(f"Trial = {retry_times}.")
            if self.connect_to_target():
                return True
            self.sock = None
        self.logger.error("Socket connect fail.")
        return False

    def mqtt_client_init(self) -> bool:
        try:
            self.mqtt_client = mqtt.Client(self.mqtt_broker, self.mqtt_port)
            self.logger.info("mqtt client established.")
        except Exception as e:
            self.logger.error(f"Failed to establish mqtt client: {e}")
            return False
        try:
            self.mqtt_client.on_message = self.on_message
            self.logger.info("Message callback registered.")
        except Exception as e:
            self.logger.error(f"Failed to register message callback: {e}")
            return False
        try:
            self.mqtt_client.on_connect = self.on_connect
            self.logger.info("Connect callback registered.")
        except Exception as e:
            self.logger.error(f"Failed to register connect callback: {e}")
            return False
        return True

    def mqtt_connect(self) -> bool:
        # connect
        retry_times = 0
        while retry_times < self.connect_retry_times:
            retry_times += 1
            try:
                self.mqtt_client.connect(self.mqtt_broker, self.mqtt_port, 60)
                # todo: may need to check if connection is success
                self.logger.info("Connect to broker success.")
                break
            except Exception as e:
                self.logger.error(f"Failed to connect to the broker: {e}.")
                if retry_times == self.connect_retry_times:
                    return False

        # subscribe
        (subscribe_result, mid) = self.mqtt_client.subscribe(self.query_config_topic)
        # todo: add more topics to get vacuum generator state
        if subscribe_result == 0:
            self.logger.info("subscribe success.")
        else:
            self.logger.error(f"Failed to subscribe. Result code: {subscribe_result}")
            return False
        return True

    def start_scheduled_init(self):
        if self.scheduled_report_thread is not None:
            self.logger.error("Scheduled report already started.")
            return
        self.scheduled_report_thread = threading.Thread(name='ReportThread', target=self.scheduled_report)
        self.scheduled_report_thread.setDaemon(True)

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.logger.info("Mqtt connected successfully.")
        else:
            self.logger.error(f"Mqtt connection failed with error code {rc}.")

    def send_config(self) -> bool:
        self.config_data['timestamp'] = time.time()
        data2send = json.dumps(self.config_data)
        if self.mqtt_client:
            try:
                self.mqtt_client.publish(self.config_topic, data2send)
                self.logger.info(f"Config message published: {data2send}.")
            except Exception as e:
                self.logger.error(f"Failed to publish message: {e}.")
                return False
        else:
            self.logger.error("Mqtt client not exist.")
            return False
        return True

    def on_message(self, client, userdata, message):
        if message.topic == '/Devices/adc_agent/QueryConfig':  # query config
            self.send_config()
        elif message.topic == '/Test' or message.topic == '/Try':  # supposed to follow some topics from robot, tbd
            sn = f'{self.protocol_sn:05d}'
            message = sn + ",QUERY_ANALOG#"
            message = message.encode()
            self.protocol_sn += 1
            if self.protocol_sn >= 100000:
                self.protocol_sn = 1
            if self.sock:
                # send cmd
                _, ready_to_write, _ = select.select([], [self.sock], [], self.socket_timeout)
                if ready_to_write[0]:
                    try:
                        if self.socket_send(message):
                            self.logger.info("Command sent.")
                        else:
                            self.logger.error("Failed to send command.")
                    except Exception as e:
                        self.logger.error(f"Failed to send command: {e}")
                        return
                else:
                    self.logger.error("Socket unable to write, timeout.")
                    return

                # recv reply
                ready_to_read, _, _ = select.select([self.sock], [], [], self.socket_timeout)
                if ready_to_read[0]:
                    try:
                        data = self.sock.recv(1024)
                        self.logger.info("Reply received.")
                    except Exception as e:
                        self.logger.error(f"Failed to receive analog: {e}")
                        return
                else:
                    self.logger.error("Socket unable to read, timeout.")
                    return

                # handle data
                self.update_json(data, sn)
                if self.update_state:
                    try:
                        with open('/vault/VacuumMonitor/Analog.json', 'r') as f:
                            json_data = json.dumps(json.load(f))
                            if self.mqtt_client:
                                try:
                                    client.publish(self.analog_topic, json_data)
                                    self.logger.info("Analog message published.")
                                except Exception as e:
                                    self.logger.error(f"Failed to publish message: {e}")
                            else:
                                self.logger.error("Mqtt client not exist.")
                    except FileNotFoundError:
                        self.logger.error("The file 'Analog.json' was not found.")
                    except json.JSONDecodeError:
                        self.logger.error("An error occurred while decoding the JSON.")
                    except Exception as e:
                        self.logger.error(f"An unexpected error occurred: {e}")

    def connect_to_target(self) -> bool:
        self.port_in_use = 0
        self.logger.info("Starting to connecting to plc.")

        for p in range(self.start_port, self.end_port + 1):
            try:
                self.logger.info(f"Starting to connecting to {self.target_address}:{p}.")
                self.sock.connect((self.target_address, p))
                if self.is_socket_connected():
                    self.logger.info(f"connected! port = {p}.")
                    self.port_in_use = p
                    break
                else:
                    self.sock = None
                    if not self.socket_init():
                        self.logger.error("Failed to reinit socket in this trial.")
                        return False
                    self.logger.info(f"Port {p} fail, start next trial.")
            except socket.error:
                self.logger.error(f"Port {p} fail.")
        if self.port_in_use == 0:
            self.logger.error("All ports failed.")
            return False
        self.logger.info(f"Connected to {self.target_address}:{self.port_in_use}.")
        return True

    def is_socket_connected(self) -> bool:
        if not self.sock:
            self.logger.error("Socket not exist, check connection fail.")
            return False
        sn = f'{self.protocol_sn:05d}'
        message = sn + ",QUERY_IO#"
        message = message.encode()
        self.protocol_sn += 1
        if self.protocol_sn >= 100000:
            self.protocol_sn = 1
        try:
            _, ready_to_write, _ = select.select([], [self.sock], [], self.socket_timeout)
            if ready_to_write:
                try:
                    self.sock.sendall(message)
                except Exception as e:
                    self.logger.error(f"Failed to send check conn cmd: {e}")
                    return False
            else:
                self.logger.error("Socket unable to write when check conn cmd, timeout.")
                return False

            ready_to_read, _, _ = select.select([self.sock], [], [], self.socket_timeout)
            if ready_to_read:
                try:
                    data = self.sock.recv(1024)
                    self.logger.info("Check conn reply received.")
                except Exception as e:
                    self.logger.error(f"Failed to receive check conn reply: {e}")
                    return False
            else:
                self.logger.error("Socket unable to read when check conn reply, timeout.")
                return False

            data_str = data.decode('UTF-8')
            if ((sn+',') in data_str) and ("UPDATE_IO," in data_str) and (data_str.endswith('#')):
                return True
            else:
                self.logger.error(f"Wrong received reply: {data_str}.")
                return False
        except socket.error:
            return False

    def update_json(self, data, sn):
        self.update_state = False

        if data:
            try:
                data = data.decode('utf-8')
            except UnicodeDecodeError as e:
                self.logger.error(f"Failed to decode message: {e}.")
                return
            if (sn+',') not in data:
                self.logger.error("Reply SN not match command.")
                return
            try:
                match = re.search(',REPORT_ANALOG,(\\s*)(\\d+)', data)
                if match:
                    analog_data = int(match.group(2))
                else:
                    self.logger.error("Receive bad message1.")
                    return
            except ValueError:
                self.logger.error("Receive bad message2.")
                return
        else:
            # case that socket receives timeout and have no data send back
            analog_data = 0

        self.current_time = int(time.time())
        try:
            with open('/vault/VacuumMonitor/Analog.json', 'r+') as f:
                json_data = json.load(f)
                json_data['value'] = float(analog_data/400)
                json_data['interval'] = self.current_time - self.last_time
                json_data['timestamp'] = self.current_time
                f.seek(0)
                json.dump(json_data, f)
                f.truncate()
        except FileNotFoundError:
            self.logger.error("File 'Analog.json' not found.")
            return
        except json.JSONDecodeError:
            self.logger.error("Error occurred while decoding the JSON.")
            return
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {e}")
            return

        self.last_time = self.current_time
        self.update_state = True

    def scheduled_report(self):
        self.logger.info("Thread start.")
        last_time = time.time()
        while self.scheduled_report_ready and self.sock and self.mqtt_client:
            while time.time()-last_time <= self.report_interval:
                time.sleep(1)
            last_time = time.time()

            # setup cmd
            sn = f'{self.protocol_sn:05d}'
            message = sn + ",QUERY_ANALOG#"
            message = message.encode()
            self.protocol_sn += 1
            if self.protocol_sn >= 100000:
                self.protocol_sn = 1

            # send cmd
            _, ready_to_write, _ = select.select([], [self.sock], [], self.socket_timeout)
            if ready_to_write:
                try:
                    if self.socket_send(message):
                        self.logger.info("Command sent.")
                    else:
                        self.logger.error("Failed to send command.")
                except Exception as e:
                    self.logger.error(f"Failed to send command: {e}")
                    continue
            else:
                self.logger.error("Socket unable to write, timeout.")
                continue
            time.sleep(0.1)

            # recv reply
            ready_to_read, _, _ = select.select([self.sock], [], [], self.socket_timeout)
            if ready_to_read:
                try:
                    data = self.sock.recv(1024)
                    self.logger.info("Reply received.")
                except Exception as e:
                    self.logger.error(f"Failed to receive analog: {e}")
                    continue
            else:
                self.logger.error("Socket unable to read, timeout.")
                continue

            self.logger.info(f"DATA = {data}.")
            self.update_json(data, sn)  # update json

            if self.update_state:
                try:
                    with open('/vault/VacuumMonitor/Analog.json', 'r') as f:
                        json_data = json.dumps(json.load(f))
                        if self.mqtt_client:
                            try:
                                self.mqtt_client.publish(self.analog_topic, json_data)
                                self.logger.info("Analog message published.")
                            except Exception as e:
                                self.logger.error(f"Failed to publish message: {e}")
                        else:
                            self.logger.error("Mqtt client not exist.")
                except FileNotFoundError:
                    self.logger.error("The file 'Analog.json' was not found.")
                except json.JSONDecodeError:
                    self.logger.error("An error occurred while decoding the JSON.")
                except Exception as e:
                    self.logger.error(f"An unexpected error occurred: {e}")

        self.logger.info("Thread end.")
        self.scheduled_report_ready = False

    def socket_send(self, message) -> bool:
        if not self.sock:
            self.logger.error("Socket client not exist.")
            return False

        for reconnect_retry_times in range(self.connect_retry_times):
            for send_retry_times in range(self.connect_retry_times):
                try:
                    self.sock.sendall(message)
                    return True
                except socket.error as e:
                    self.logger.error(f"Socket error: {e}, try {send_retry_times} times.")
                    time.sleep(1)
                except Exception as e:
                    self.logger.error(f"Failed to send command: {e}, try {send_retry_times} times.")
                    time.sleep(1)
            self.logger.error(f"Retry {self.connect_retry_times} times.")
            self.sock.close()
            self.connect_to_target()
        self.logger.error(f"Reconnect failed {self.connect_retry_times} times, send fail")
        return False

    def start_scheduled_report(self):
        if self.scheduled_report_thread.is_alive():
            self.logger.error("Report thread is already on.")
            return
        self.logger.info("Starting scheduled report.")
        self.scheduled_report_ready = True
        self.scheduled_report_thread.start()
        self.logger.info("Scheduled report start.")

    def start(self):
        if self.mqtt_client:
            self.mqtt_client.loop_start()
            self.logger.info("Mqtt loop started.")
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

    def clean_up(self):
        self.sock.close()
        self.sock = None


if __name__ == '__main__':
    new = Vacuum()
    if new.init_success:
        new.start()
