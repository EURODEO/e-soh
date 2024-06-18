import json
import logging
from paho.mqtt import client as mqtt_client
from fastapi import HTTPException

logger = logging.getLogger(__name__)


def connect_mqtt(mqtt_conf: dict):
    def on_connect(client, userdata, flags, rc, properties):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)
    client.username_pw_set(mqtt_conf["username"], mqtt_conf["password"])
    client.on_connect = on_connect
    client.tls_set()
    client.connect(mqtt_conf["host"], 8883)
    client.loop_start()
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
