import paho.mqtt.client as mqtt

import json
import logging

logger = logging.getLogger(__name__)


class mqtt_connection():
    def __init__(self, mqtt_host, mqtt_topic):
        self.mqtt_host = mqtt_host
        self.mqtt_topic = mqtt_topic

        # Initiate MQTT Client
        self.pub_client = mqtt.Client(client_id="")

        # Connect with MQTT Broker
        self.pub_client.connect(self.mqtt_host)

        logger.info(
            f"Established MQTT connection to {self.mqtt_host}, with topic {self.mqtt_topic}")

    def send_message(self, message: str):
        try:
            if isinstance(message, str):
                self.pub_client.publish(self.mqtt_topic, message)
            else:
                self.pub_client.publish(self.mqtt_topic, json.dumps(message))
        except Exception as e:
            print("Did exception")
            logger.critical(str(e))
            raise
