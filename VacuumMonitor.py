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
from typing import Optional


class Vacuum:
    # class constant
    CLASS_NAME = 'VacuumMonitor'
    REPORTER_NAME = 'ReportThread'
    CONFIG_PATH = '/vault/VacuumMonitor/config/vacuum_config.json'

    # message json keys
    VALUE_KEY = 'value'
    INTERVAL_KEY = 'interval'
    TIMESTAMP_KEY = 'timestamp'

    # plc commands
    COMMA = ','
    ZERO_SN = '00000,'
    CHECK_CMD = ',QUERY_IO#'
    CHECK_RESP = ',UPDATE_IO,'
    ANALOG_CMD = ',QUERY_ANALOG#'
    ANALOG_RESP = ',REPORT_ANALOG,(\\s*)(\\d+)'
    CLEAR_CMD = ',CLEAR_ERROR#'
    CMD_END = '#'
    ENCODE_MODE = 'UTF-8'

    # update state
    UPDATE_SUCCESS = 0
    UPDATE_FAIL = 1
    WRONG_PLC_PROG = 2

    def __init__(self):
        self.init_success = False

        self.logger: Optional[logging.Logger] = None
        self.loaded_config = None
        self.config_msg = None

        self.last_time = int(time.time())
        self.current_time = 0
        self.port_in_use = 0
        self.connection_state = False
        self.update_state = self.UPDATE_FAIL
        self.sock = None
        self.mqtt_client = None
        self.mqtt_connect_state = False
        self.scheduled_report_ready = False
        self.scheduled_report_thread = None
        self.maincomponent_id = None
        self.subcomponent_id = '/' + self.CLASS_NAME
        self.protocol_sn = 1
        self.config_topic = None
        self.analog_topic = None
        # use default
        self.local_ip = '10.0.1.200'
        self.mqtt_host = 'localhost'
        self.mqtt_port = 1883
        self.mqtt_keepalive = 60
        self.target_address = '10.0.1.202'
        self.start_port = 4096
        self.end_port = 4101
        self.station_path = "/vault/ADCAgent/dst/setting/adc_agent_register.json"
        self.configmsg_path = "/vault/VacuumMonitor/config/Config2Send_Vacuum.json"
        self.analog_path = '/vault/VacuumMonitor/Analog.json'
        self.report_interval = 5
        self.connect_retry_times = 3
        self.socket_timeout = 3
        self.query_config_topic = '/Devices/adc_agent/QueryConfig'

        try:
            self.init_logger()
            self.logger.info("************************************************")
            self.logger.info("Initializing.")
            self.load_config()
            self.init_parameters()
            self.load_configmsg()
            self.socket_init()
            self.socket_connect_with_retry()
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
        LOG_PATH = '/vault/VacuumMonitor/log'
        LOGGER_NAME = 'VacuumLogger'
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
            raise ValueError(f"Logger initialization failed")

    def load_config(self):
        try:
            with open(self.CONFIG_PATH, 'r') as f:
                self.loaded_config = json.load(f)
            self.logger.info("vacuum_config load success.")
        except FileNotFoundError:
            self.logger.error("vacuum_config was not found, will use default.")
        except json.JSONDecodeError:
            self.logger.error("An error occurred while decoding the JSON, will use default.")
        except Exception as e:
            self.logger.error(f"An unexpected error occurred when loading config file: {e}, will use default", exc_info=True)

    def init_parameters(self):
        TOPIC_HEADER = "/Devices/"
        CONFIG_TOPIC_END = '/Config'
        ANALOG_TOPIC_END = '/Analog'
        try:
            if self.loaded_config is not None:
                self.local_ip = self.loaded_config.get('local_ip')
                self.mqtt_host = self.loaded_config.get('broker_host')
                self.mqtt_port = int(self.loaded_config.get('broker_port'))
                self.mqtt_keepalive = int(self.loaded_config.get('mqtt_keepalive'))
                self.target_address = self.loaded_config.get('target_address')
                self.start_port = int(self.loaded_config.get('start_port'))
                self.end_port = int(self.loaded_config.get('end_port'))
                self.station_path = self.loaded_config.get('station_path')
                self.configmsg_path = self.loaded_config.get('config_path')
                self.analog_path = self.loaded_config.get('analog_path')
                self.report_interval = int(self.loaded_config.get('report_interval'))
                self.connect_retry_times = int(self.loaded_config.get('connect_retry_times'))
                self.socket_timeout = int(self.loaded_config.get('socket_timeout'))
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

    def load_configmsg(self):
        try:
            with open(self.configmsg_path, 'r') as f:
                self.config_msg = json.load(f)
            self.logger.info("Config2Send_Vacuum.json load success.")
        except FileNotFoundError:
            self.logger.error("Config2Send_Vacuum.json was not found.")
            raise ValueError(f"Load config message failed")
        except json.JSONDecodeError:
            self.logger.error("An error occurred while decoding the JSON.")
            raise ValueError(f"Load config message failed")
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {e}", exc_info=True)
            raise ValueError(f"Load config message failed: {e}")

    def get_maincomponent_id(self) -> bool:
        CELL_TYPE = 'cell_type'
        MAIN_ID_FRONT = "work_station_"
        for interface, addrs in psutil.net_if_addrs().items():
            for addr in addrs:
                if addr.address == self.local_ip:
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
                        self.logger.error(f"Failed to load adc_agent_register.json : {e}.")
                        break
        return False

    def socket_init(self):
        try:
            self.sock = None
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except socket.error as e:
            self.logger.error(f"Failed to create a socket. Error: {e}")
            raise ValueError(f"Socket client initialization failed: {e}")

    def socket_connect_with_retry(self):
        retry_times = 0
        if not self.connect_retry_times or 0 == self.connect_retry_times:
            self.logger.error("Parameter connect_retry_times not set.")
            raise ValueError("Socket connect failed")
        while retry_times < self.connect_retry_times:
            if not self.sock:
                try:
                    self.socket_init()
                except Exception as e:
                    raise ValueError(f"Socket connect failed: {e}")
            retry_times += 1
            self.logger.info(f"Trial = {retry_times}.")
            if self.connect_to_target():
                try:
                    atexit.register(self.clean_up)
                    self.logger.info("Cleanup function registered.")
                except Exception as e:
                    self.logger.error(f"Failed to register cleanup function: {e}.")
                return
            self.sock = None
        self.logger.error("Socket connect fail.")
        raise ValueError("Socket connect failed")

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
        # set connect (does not really connect)
        retry_times = 0
        while retry_times < self.connect_retry_times:
            retry_times += 1
            try:
                self.mqtt_client.connect(self.mqtt_host, self.mqtt_port, self.mqtt_keepalive)
                self.logger.info("Set mqtt connection parameters.")
                break
            except Exception as e:
                self.logger.error(f"Failed to set mqtt connection parameters: {e}.")
                if retry_times == self.connect_retry_times:
                    self.logger.error(f"Set mqtt connection parameters failed {self.connect_retry_times} time, exit.")
                    raise ValueError("MQTT set connection failed")
        # subscribe
        (subscribe_result, mid) = self.mqtt_client.subscribe(self.query_config_topic)
        # todo: add more topics to get vacuum generator state
        if subscribe_result == mqtt.MQTT_ERR_SUCCESS:
            self.logger.info("subscribe success.")
        else:
            self.logger.error(f"Failed to subscribe. Result code: {subscribe_result}")
            raise ValueError("MQTT set connection failed")

    def scheduled_report_init(self):
        if self.scheduled_report_thread is not None:
            self.logger.error("Scheduled report already started.")
            return
        try:
            self.scheduled_report_thread = threading.Thread(name=self.REPORTER_NAME, target=self.scheduled_report)
            self.scheduled_report_thread.setDaemon(True)
        except Exception as e:
            raise ValueError(f"Report thread initialization failed: {e}")

    def on_connect(self, _, __, ___, rc):
        if 0 == rc:
            self.mqtt_connect_state = True
            self.logger.info("Mqtt connected successfully.")
        else:
            self.mqtt_connect_state = False
            self.logger.error(f"Mqtt connection failed with error code {rc}.")

    def send_config(self) -> bool:
        self.config_msg[self.TIMESTAMP_KEY] = time.time()
        data2send = json.dumps(self.config_msg)
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

    def on_message(self, client, _, message):
        if message.topic == self.query_config_topic:
            self.send_config()
        elif message.topic == '/Test' or message.topic == '/Try':  # supposed to follow some topics from robot, tbd
            sn = f'{self.protocol_sn:05d}'
            message = sn + self.ANALOG_CMD
            message = message.encode()
            self.sn_add()  # can move to other place, depends on whether only add when send success
            if self.sock:
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
                        return
                else:
                    self.logger.error("Socket unable to write, timeout.")
                    return

                time.sleep(0.1)

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
                self.logger.info(f"DATA = {data}.")
                self.update_json(data, sn)

                if self.update_state == self.UPDATE_SUCCESS:
                    try:
                        with open(self.analog_path, 'r') as f:
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
                    if self.sock:
                        self.sock.close()
                        self.sock = None
                    try:
                        self.socket_init()
                    except Exception as e:
                        self.logger.error(f"Failed to reinit socket in this trial {e}.")
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
        message = sn + self.CHECK_CMD
        message = message.encode()
        self.sn_add()
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

            data_str = data.decode(self.ENCODE_MODE)
            if ((sn+self.COMMA) in data_str or self.ZERO_SN in data_str) and (self.CHECK_RESP in data_str) and (data_str.endswith(self.CMD_END)):
                return True
            else:
                self.logger.error(f"Wrong received reply: {data_str}, sn={sn}.")
                return False
        except socket.error:
            return False

    def update_json(self, data, sn):
        self.update_state = self.UPDATE_FAIL

        if data:
            try:
                data = data.decode(self.ENCODE_MODE)
            except UnicodeDecodeError as e:
                self.logger.error(f"Failed to decode message: {e}.")
                return
            if (sn+',') not in data:
                self.logger.error("Reply SN not match command.")
                return
            try:
                match = re.search(self.ANALOG_RESP, data)
                if match:
                    analog_data = int(match.group(2))
                else:
                    self.logger.error("Receive bad message1.")
                    self.update_state = self.WRONG_PLC_PROG
                    return
            except ValueError:
                self.logger.error("Receive bad message2.")
                self.update_state = self.WRONG_PLC_PROG
                return
        else:
            # case that socket receives timeout and have no data send back
            analog_data = 0

        self.current_time = int(time.time())
        try:
            with open(self.analog_path, 'r+') as f:
                json_data = json.load(f)
                json_data[self.VALUE_KEY] = float(analog_data/400)
                json_data[self.INTERVAL_KEY] = self.current_time - self.last_time
                json_data[self.TIMESTAMP_KEY] = self.current_time
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
        self.update_state = self.UPDATE_SUCCESS

    def scheduled_report(self):
        self.logger.info("Thread start.")
        send_failed_count = 0
        socket_recreate_flag = False
        publish_failed_count = 0
        last_time = time.time()

        while self.scheduled_report_ready and self.mqtt_client:
            if publish_failed_count >= self.connect_retry_times:
                self.logger.error(f"Publish failed {self.connect_retry_times} times, report thread will terminate.")
                break
            if send_failed_count >= self.connect_retry_times or socket_recreate_flag:
                self.logger.info(f"Will start reconnect.")
                if self.sock:
                    self.sock.close()
                self.sock = None
                if (not self.socket_init()) or (not self.socket_connect_with_retry()):
                    self.logger.error("Connect retry failed, report thread will terminate.")
                    break
                self.logger.info("Socket recreated.")
                send_failed_count = 0
                socket_recreate_flag = False
            if not self.sock:
                self.logger.error("Socket client not exist, will re create.")
                socket_recreate_flag = True
                continue

            while time.time()-last_time <= self.report_interval:
                time.sleep(0.5)
            last_time = time.time()

            # setup cmd
            sn = f'{self.protocol_sn:05d}'
            message = sn + self.ANALOG_CMD
            message = message.encode()
            self.sn_add()  # can move to other place, depends on whether only add when send success

            # send cmd
            _, ready_to_write, _ = select.select([], [self.sock], [], self.socket_timeout)
            if ready_to_write:
                try:
                    if self.socket_send(message):
                        self.logger.info("Command sent.")
                    else:
                        send_failed_count += 1
                        self.logger.error(f"Failed to send command {send_failed_count} times.")
                        continue
                except Exception as e:
                    send_failed_count += 1
                    self.logger.error(f"Failed to send command {send_failed_count} times: {e}.")
                    continue
            else:
                send_failed_count += 1
                self.logger.error(f"Socket unable to write {send_failed_count} times, timeout.")
                continue
            time.sleep(0.1)

            # recv reply
            ready_to_read, _, _ = select.select([self.sock], [], [], self.socket_timeout)
            if ready_to_read:
                try:
                    data = self.sock.recv(1024)
                    self.logger.info("Reply received.")
                except Exception as e:
                    send_failed_count += 1
                    self.logger.error(f"Failed to receive analog {send_failed_count} times: {e}")
                    continue
            else:
                send_failed_count += 1
                self.logger.error(f"Socket unable to read {send_failed_count} times, timeout.")
                continue

            self.logger.info(f"DATA = {data}.")
            if data:
                send_failed_count = 0
            self.update_json(data, sn)  # update json

            if self.update_state == self.UPDATE_SUCCESS:
                try:
                    with open(self.analog_path, 'r') as f:
                        json_data = json.dumps(json.load(f))
                        if self.mqtt_client:
                            try:
                                self.mqtt_client.publish(self.analog_topic, json_data)
                                self.logger.info("Analog message published.")
                                publish_failed_count = 0
                            except Exception as e:
                                publish_failed_count += 1
                                self.logger.error(f"Failed to publish message {publish_failed_count} times: {e}")
                        else:
                            self.logger.error("Mqtt client not exist.")
                            break
                except FileNotFoundError:
                    self.logger.error("The file 'Analog.json' was not found.")
                    break
                except json.JSONDecodeError:
                    self.logger.error("An error occurred while decoding the JSON.")
                    break
                except Exception as e:
                    publish_failed_count += 1
                    self.logger.error(f"An unexpected error occurred: {e}, trial {publish_failed_count}.")
            elif self.update_state == self.WRONG_PLC_PROG:
                self.logger.error("Wrong PLC program, will exit.")
                sn = f'{self.protocol_sn:05d}'
                message = sn + self.CLEAR_CMD
                message = message.encode()
                _, ready_to_write, _ = select.select([], [self.sock], [], self.socket_timeout)
                if ready_to_write:
                    try:
                        if self.socket_send(message):
                            self.logger.info("Command sent.")
                        else:
                            self.logger.error(f"Failed to send command, directly exit.")
                            break
                    except Exception as e:
                        self.logger.error(f"Failed to send command: {e}, directly exit.")
                        break
                else:
                    self.logger.error(f"Socket unable to write, directly exit.")
                    break
                ready_to_read, _, _ = select.select([self.sock], [], [], self.socket_timeout)
                if ready_to_read:
                    try:
                        self.sock.recv(1024)
                        self.logger.info("Reply received, will exit.")
                    except Exception as e:
                        self.logger.error(f"Failed to receive: {e}, directly exit.")
                else:
                    self.logger.error(f"Socket unable to read, directly exit.")
                break

        self.logger.info("Thread end.")
        self.scheduled_report_ready = False

    def socket_send(self, message) -> bool:
        if not self.sock:
            self.logger.error("Socket client not exist.")
            return False

        for send_retry_times in range(self.connect_retry_times):
            try:
                self.sock.sendall(message)
                return True
            except socket.error as e:
                self.logger.error(f"Socket error: {e}, trial {send_retry_times}.")
                time.sleep(0.1)
            except Exception as e:
                self.logger.error(f"Failed to send command: {e}, trial {send_retry_times}.")
                time.sleep(0.1)

        self.logger.error(f"Sending failed {self.connect_retry_times} times.")
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
            try:
                self.mqtt_client.loop_start()
                self.logger.info("Mqtt loop started.")
            except Exception as e:
                self.logger.error(f"Mqtt loop start fail: {e}.")
                return
            s_time = time.time()
            while not self.mqtt_connect_state:
                if time.time()-s_time > self.socket_timeout:
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

    def clean_up(self):
        if self.sock:
            self.sock.close()
        self.logger.info("Socket closed.")
        self.sock = None

    def sn_add(self):
        self.protocol_sn += 1
        if self.protocol_sn >= 100000:
            self.protocol_sn = 1


if __name__ == '__main__':
    new = Vacuum()
    if new.init_success:
        new.start()
