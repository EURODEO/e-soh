# e-soh-event-queue
## Enviornment variables
| Variable | Type | Default | Explenation |
| ---------| ---- | ------- | ----------- |
| MQTT_HOST | String | None | Set host for MQTT broker |
| MQTT_USERNAME | String | None | Set username for MQTT |
| MQTT_PASSWORD | String | None | Set password for MQTT |
| MQTT_TLS | Bool | False | Enable TLS for MQTT connection |
| MQTT_PORT | Int | 8883 | Set port for MQTT broker |


## Dev install
To install in dev mode run `pip install --editable .` from the top level of this repository.


## C++ dependencies
| Module          | Version |
| --------------- | ------- |
| libeccodes-data | 2.24.2  |
| rapidjson-dev   | 1.1.0   |
| pybind11-dev    | 2.9.1   |
