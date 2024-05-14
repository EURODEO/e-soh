import json
import logging
import random

from paho.mqtt import client as mqtt_client
from fastapi import HTTPException

logger = logging.getLogger(__name__)


def connect_mqtt(mqtt_conf: dict):
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client_id = f"publish-{random.randint(0, 1000)}"
    client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2, client_id)
    client.username_pw_set(mqtt_conf["username"], mqtt_conf["password"])
    client.on_connect = on_connect
    client.tls_set()
    client.connect(mqtt_conf["host"], 8883)
    return client


def send_message(topic: str, message: str, client: object):
    if len(topic) != 0:
        mqtt_topic = topic
    try:
        if isinstance(message, str):
            client.publish(mqtt_topic, message)
        else:
            client.publish(mqtt_topic, json.dumps(message))

    except Exception as e:
        logger.critical(str(e))
        raise HTTPException(status_code=500, detail="Failed to publish to mqtt")
