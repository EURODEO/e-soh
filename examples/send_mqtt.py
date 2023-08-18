from create_mqtt_message import build_all_json_payloads_from_netCDF
import paho.mqtt.client as mqtt
import json
from datetime import datetime

import uuid

MQTT_HOST = ""
MQTT_TOPIC = "topic/test"



import paho.mqtt.client as mqtt
import json

path = "../test_data/air_temperature_gullingen_skisenter-parent.nc"

messages = build_all_json_payloads_from_netCDF(path)

# Initiate MQTT Client
pub_client = mqtt.Client(client_id="")

# Connect with MQTT Broker
pub_client.connect(MQTT_HOST)


for m in messages:
	pub_client.publish(MQTT_TOPIC, json.dumps(m))




 

