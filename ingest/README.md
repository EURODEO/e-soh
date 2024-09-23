# e-soh-event-queue
## Enviornment variables

| Variable                   | Default Value              | Description                                                                 |
|----------------------------|----------------------------|-----------------------------------------------------------------------------|
| `DSHOST`                   | `store`                    | Host address for the data store. Defaults to `store` if not set.            |
| `DSPORT`                   | `50050`                    | Port for the data store connection. Defaults to `50050` if not set.         |
| `MQTT_HOST`                |                            | Host address for the MQTT broker.                                           |
| `MQTT_USERNAME`            |                            | Username for authentication with the MQTT broker.                           |
| `MQTT_PASSWORD`            |                            | Password for authentication with the MQTT broker.                           |
| `MQTT_TLS`                 | `True`                     | Whether to use TLS (True/False) for the MQTT connection. Defaults to `True`.|
| `PROMETHEUS_MULTIPROC_DIR` | `/tmp/metrics`             | Directory for Prometheus multiprocess mode metrics. Defaults to `/tmp/metrics`. |
| `INGEST_LOGLEVEL`          |                            | Logging level for the ingestion process.                                     |
| `GUNICORN_CMD_ARGS`        |                            | Command-line arguments for configuring Gunicorn, a Python WSGI HTTP Server.  |
| `FASTAPI_ROOT_PATH`        |                            | If this api is behind proxy, this need to be set to the root path |


## Dev install
To install in dev mode run `pip install --editable .` from the top level of this repository.


## C++ dependencies
| Module          | Version |
| --------------- | ------- |
| libeccodes-data | 2.24.2  |
| rapidjson-dev   | 1.1.0   |
| pybind11-dev    | 2.9.1   |
