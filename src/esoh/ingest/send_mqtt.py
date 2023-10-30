import paho.mqtt.client as mqtt

import json
import logging
import ssl

logger = logging.getLogger(__name__)


class mqtt_connection():
    def __init__(self, mqtt_host, mqtt_topic, mqtt_username=None, mqtt_password=None):
        self.mqtt_host = mqtt_host
        self.mqtt_topic = mqtt_topic
        self.mqtt_port = 8883

        # Initiate MQTT Client
        self.pub_client = mqtt.Client(client_id="")
        self.pub_client.enable_logger(logger=logger)

        if mqtt_username and mqtt_password:
            self.pub_client.username_pw_set(mqtt_username, mqtt_password)
            logger.info("Set authentication for MQTT service")

        self.pub_client.tls_set(certfile=None,
                                keyfile=None,
                                cert_reqs=ssl.CERT_REQUIRED)

        # Connect with MQTT Broker
        self.pub_client.connect(self.mqtt_host, port=self.mqtt_port)

        logger.info(
            f"Established MQTT connection to {self.mqtt_host}, with topic {self.mqtt_topic}")

    def send_message(self, message: str):
        try:
            if isinstance(message, str):
                self.pub_client.publish(self.mqtt_topic, message)
                logger.debug(f"Sent mqtt to {self.mqtt_topic}")
            else:
                self.pub_client.publish(self.mqtt_topic, json.dumps(message))
                logger.debug(f"Sent mqtt to {self.mqtt_topic}")

        except Exception as e:
            logger.critical(str(e))
            raise
