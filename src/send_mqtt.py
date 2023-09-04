from file_metadata_parser.extract_metadata_netcdf import build_all_json_payloads_from_netCDF

import paho.mqtt.client as mqtt
import xarray as xr

import json


MQTT_HOST = ""
MQTT_TOPIC = "topic/test"

path = "../test_data/air_temperature_gullingen_skisenter-parent.nc"

json_netcdf_def = "../schemas/netcdf_to_e_soh_message_metno.json"


ds = xr.load_dataset(path)
with open(json_netcdf_def, "r") as file:
    netcdf_def = json.load(file)

messages = build_all_json_payloads_from_netCDF(ds, json)

# Initiate MQTT Client
pub_client = mqtt.Client(client_id="")

# Connect with MQTT Broker
pub_client.connect(MQTT_HOST)


for m in messages:
    pub_client.publish(MQTT_TOPIC, json.dumps(m))
