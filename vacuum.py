import paho.mqtt.client as mqtt
import json
import socket
import time
import logging
import re
import threading
import select

class Vacuum:
    def __init__(self, maincomponent_id, subcomponent):
        self.init_success = False
        logging.basicConfig(filename='vacuum.log', level=logging.INFO, format='%(asctime)s %(message)s')
        logging.info("Initializing")

        # load config file
        self.loaded_data = None
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
            self.config_topic = "/Devices/" + maincomponent_id + "/" + subcomponent + "/" + "Config"
            self.query_config_topic = "/Devices/adc_agent/QueryConfig"
            self.analog_topic = "/Devices/" + maincomponent_id + "/" + subcomponent + "/" + "Analog"
            self.last_time = int(time.time())
            self.current_time = 0
            self.port_in_use = 0
            self.connection_state = False
            self.update_state = False
            self.sock = None
            self.client = None
            self.scheduled_report_ready = False
            self.scheduled_report_thread = None
            if self.loaded_data is not None:
                self.broker = self.loaded_data.get('broker', "10.0.1.200")
                self.port = int(self.loaded_data.get('broker_port', "1883"))
                self.target_address = self.loaded_data.get('target_address', "10.0.1.202")
                self.start_port = int(self.loaded_data.get('start_port', "4096"))
                self.end_port = int(self.loaded_data.get('end_port', "4101"))
                self.config_path = self.loaded_data.get('config_path', "Config.json")
                self.report_interval = int(self.loaded_data.get('report_interval', "5"))
                self.connect_retry_times = int(self.loaded_data.get('connect_retry_times', "3"))
                self.socket_timeout = int(self.loaded_data.get('socket_timeout', "3"))
            else:
                self.broker = "10.0.1.200"
                self.port = 1883
                self.target_address = "10.0.1.202"
                self.start_port = 4096
                self.end_port = 4101
                self.config_path = "Config.json"
                self.report_interval = 5
                self.connect_retry_times = 3
                self.socket_timeout = 3
        except Exception:
            logging.error("Initialize parameters fail.")
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
        if not self.socket_connect_with_retry():
            return
        # todo: may need to keep alive

        # init mqtt
        if not self.mqtt_client_init():
            return
        if not self.mqtt_connect():
            return

        self.start_scheduled_init()
        self.scheduled_report_ready = True
        self.init_success = True

    def socket_init(self) -> bool:
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            return True
        except socket.error as e:
            logging.error(f"Failed to create a socket. Error: {e}")
            return False

    def socket_connect_with_retry(self) -> bool:
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
        retry_times = 0
        while retry_times < self.connect_retry_times:
            retry_times += 1
            try:
                self.client.connect("localhost", 1883, 60)
                logging.info("Connect to broker success.")
                break
            except Exception as e:
                logging.error(f"Failed to connect to the broker: {e}.")
                if retry_times == self.connect_retry_times:
                    return False

        (subscribe_result, mid) = self.client.subscribe("/Devices/adc_agent/QueryConfig")
        self.client.subscribe("/Test")
        # self.client.subscribe("")   add more topics
        if subscribe_result == 0:
            logging.info("subscribe success.")
        else:
            logging.error(f"Failed to subscribe. Result code: {subscribe_result}")
            return False
        return True

    def start_scheduled_init(self):
        if self.scheduled_report_thread is not None:
            logging.error("Scheduled report already started.")
            return
        self.scheduled_report_thread = threading.Thread(target=self.scheduled_report)
        self.scheduled_report_thread.setDaemon(True)

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
        elif message.topic == '/Test' or message.topic == '/Try':  # suck or release
            message = "00000,CHECK_ANALOG#"  # todo: change to check analog cmd
            message = message.encode()
            if self.sock:
                # send cmd
                _, ready_to_write, _ = select.select([], [self.sock], [], self.socket_timeout)
                if ready_to_write[0]:
                    try:
                        if self.socket_send(message):
                            logging.info("Command sent.")
                        else:
                            logging.error("Failed to send command.")
                    except Exception as e:
                        logging.error(f"Failed to send command: {e}")
                        return
                else:
                    logging.error("Socket unable to write, timeout.")
                    return

                # recv reply
                ready_to_read, _, _ = select.select([self.sock], [], [], self.socket_timeout)
                data = None
                if ready_to_read[0]:
                    try:
                        data = self.sock.recv(1024)
                        logging.info("Reply received.")
                    except Exception as e:
                        logging.error(f"Failed to receive analog: {e}")
                        return
                else:
                    logging.error("Socket unable to read, timeout.")
                    return

                # handle data
                self.update_json(data)
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
                match = re.search(',REPORT_ANALOG,\s*(\d+)'
                                  '', data)
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
        message = "00000,QUERY_ANALOG#"  # todo: change to check analog cmd
        message = message.encode()
        last_time = time.time()
        while self.scheduled_report_ready and self.sock and self.client:
            while time.time()-last_time <= self.report_interval:
                time.sleep(1)
            last_time = time.time()

            # send cmd
            _, ready_to_write, _ = select.select([], [self.sock], [], self.socket_timeout)
            if ready_to_write:
                try:
                    if self.socket_send(message):
                        logging.info("Command sent.")
                    else:
                        logging.error("Failed to send command.")
                except Exception as e:
                    logging.error(f"Failed to send command: {e}")
                    continue
            else:
                logging.error("Socket unable to write, timeout.")
                continue
            time.sleep(0.1)

            # recv reply
            ready_to_read, _, _ = select.select([self.sock], [], [], self.socket_timeout)
            data = None
            if ready_to_read:
                try:
                    data = self.sock.recv(1024)
                    logging.info("Reply received.")
                except Exception as e:
                    logging.error(f"Failed to receive analog: {e}")
                    continue
                    # break
            else:
                logging.error("Socket unable to read, timeout.")
                continue
                # break

            logging.info(f"DATA = {data}.")
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

        logging.info("Thread end.")
        self.scheduled_report_ready = False

    def socket_send(self, message) -> bool:
        if not self.sock:
            logging.error("Socket client not exist.")
            return False

        for reconnect_retry_times in range(self.connect_retry_times):
            for send_retry_times in range(self.connect_retry_times):
                try:
                    self.sock.sendall(message)
                    return True
                except socket.error as e:
                    logging.error(f"Socket error: {e}, try {send_retry_times} times.")
                    time.sleep(1)
                except Exception as e:
                    logging.error(f"Failed to send command: {e}, try {send_retry_times} times.")
                    time.sleep(1)
            logging.error(f"Retry {self.connect_retry_times} times.")
            self.sock.close()
            self.connect_to_target()
        logging.error(f"Reconnect failed {self.connect_retry_times} times, send fail")
        return False

    def start_scheduled_report(self):
        if self.scheduled_report_thread.is_alive():
            logging.error("Report thread is already on.")
            return
        logging.info("Starting scheduled report.")
        self.scheduled_report_ready = True
        self.scheduled_report_thread.start()
        logging.info("Scheduled report start.")

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
    new = Vacuum("insider_transfer_DVI_1", "vacuum_1")
    if new.init_success:
        new.start()



