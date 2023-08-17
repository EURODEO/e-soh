# e-soh-event-queue

## Examples
To create and send mqtt example messages with json payload, set up `MQTT_HOST` and `MQTT_TOPIC` in examples/send_mqtt.py

To validate json payload, first write the json payload to file with `write_example_json.py`. Then run the check-jsonschema command.
```
# install the check-jsonschema Python package
pip3 install check-jsonschema

# validate a WWIS2 Notification Message on the command line
check-jsonschema --schemafile ../schemas/e-soh-message-spec.json SN99938_meta.json
```