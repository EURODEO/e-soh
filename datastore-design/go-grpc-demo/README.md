# E-SOH datastore variant 1: gRPC service written in Go

## Overview

This directory contains code that demonstrates how the E-SOH datastore could
be implemented as a [gRPC](https://grpc.io/) service written in
[Go](https://go.dev/).

Currently the datastore server is using a TimescaleDB server as its only
storage backend (for both metadata and observations).

The code has been tested in the following environment:

### Service

|   |   |
|---|---|
| OS | [Ubuntu](https://ubuntu.com/) [22.04 Jammy](https://releases.ubuntu.com/jammy/) |
| [Docker](https://www.docker.com/) | 24.0.5 |
| [Docker Compose](https://www.docker.com/) | 2.20.2 |

### Python client example

|   |   |
|---|---|
| OS | Same as service |
| [Python](https://www.python.org/) | 3.11 |
| [grpcio-tools](https://grpc.io/docs/languages/python/quickstart/) | 1.56.2 |


## Using docker compose to manage the service

`docker compose up`

`docker compose down`

MORE DETAILS HERE!

## Environment variables

TO BE OBSOLETED BY A SECTION ABOUT ENVIRONMENT VARIABLES RELEVANT TO
DOCKER COMPOSE ONLY (to override fields in docker-compose.yml)

The following environment variables are supported:

Variable | Mandatory | Default value | Description
:--      | :--       | :--           | :--
`SERVERPORT`       | No  | `50050`            | Server port number
`TSDBHOST`         | No  | `localhost`        | TimescaleDB host
`TSDBPORT`         | No  | `5433`             | TimescaleDB port number
`TSDBUSER`         | No  | `postgres`         | TimescaleDB user name
`TSDBPASSWORD`     | No  | `mysecretpassword` | TimescaleDB password
`TSDBDBNAME`       | No  | `data`             | TimescaleDB database name
`TSDBRESET`        | No  | `false`            | Whether to reset the TimescaleDB database (drop, (re)create, define schema etc.)

## Testing the datastore service with gRPCurl

The datastore service can be tested with [gRPCurl](https://github.com/fullstorydev/grpcurl) like
this:

```text
$ grpcurl -plaintext -proto protobuf/datastore.proto 127.0.0.1:50050 list
datastore.Datastore
```

```text
$ grpcurl -plaintext -proto protobuf/datastore.proto 127.0.0.1:50050 describe
datastore.Datastore is a service:
service Datastore {
  rpc AddTimeSeries ( .datastore.AddTSRequest ) returns ( .datastore.AddTSResponse );
...
```

```text
$ grpcurl -plaintext -proto protobuf/datastore.proto 127.0.0.1:50050 describe .datastore.AddTSRequest
message AddTSRequest {
  int64 id = 1;
  .datastore.TSMetadata metadata = 2;
}
```

```text
$ grpcurl -plaintext -proto protobuf/datastore.proto 127.0.0.1:50050 describe .datastore.TSMetadata
datastore.TSMetadata is a message:
message TSMetadata {
  string station_id = 1;
...
```

```text
$ grpcurl -d '{"id": 1234, "metadata": {"station_id": "18700", "param_id": "211", "lat": 59.91, "lon": 10.75, "other1": "value1", "other2": "value2", "other3": "value3"}}' -plaintext -proto protobuf/datastore.proto 127.0.0.1:50050 datastore.Datastore.AddTimeSeries
...
```

```text
$ grpcurl -d '{"tsobs": [{"tsid": 1234, "obs": [{"time": 160, "value": 123.456, "metadata": {"field1": "value1", "field2": "value2"}}]}]}' -plaintext -proto protobuf/datastore.proto 127.0.0.1:50050 datastore.Datastore.PutObservations
...
```

```text
$ grpcurl -d '{"tsids": [1234, 5678, 9012], "fromtime": 156, "totime": 163}' -plaintext -proto protobuf/datastore.proto 127.0.0.1:50050 datastore.Datastore.GetObservations
...
```

## Testing the datastore service with a Python client

### Compiling the protobuf file

If necessary, compile the protobuf file first. The following command generates the files
`datastore_pb2.py` and `datastore_grpc.py` under `examples/clients/python/`:

```text
python -m grpc_tools.protoc --proto_path=protobuf protobuf/datastore.proto --python_out=examples/clients/python --grpc_python_out=examples/clients/python
```

### Running the client

The python client can be run like this:

```text
$ python examples/clients/python/client.py
calling AddTSRequest() ...
...
```

Testing the performance can be done with:
```bash
$ python -m cProfile -o <cprofile_output_file> <path_to_python_script>
```

Generate a dot graph / tree with:
```bash
$ gprof2dot --colour-nodes-by-selftime -f pstats <cprofile_output_file> | dot -Tpng -o <output_graph_file>
```
