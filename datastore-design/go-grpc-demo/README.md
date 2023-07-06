# E-SOH datastore variant 1: gRPC service written in Go

## Overview

This directory contains code that demonstrates how the E-SOH datastore could
be implemented as a [gRPC](https://grpc.io/) service written in
[Go](https://go.dev/).

The code has been tested in the following environment:

- OS: Ubuntu 18.04 Bionic
- Go: 1.20.5
- protoc: libprotoc 3.0.0

## Compiling the protobuf file

If necessary, compile the protobuf file first. The following command generates
the directory/file `datastore/datastore.pb.go`:

```text
protoc protobuf/datastore.proto --go_out=plugins=grpc:.
```

## Generating a go.sum file

If necessary, generate a `go.sum` file:

```text
go mod tidy
```

## Installing and running TimescaleDB in a docker container

Currently the datastore server is using a TimescaleDB server as its only storage backend (for both
metadata and observations). The following commands shows how TimescaleDB can be run in a docker
container:

```text
$ docker pull timescale/timescaledb-ha:pg15-latest
pg15-latest: Pulling from timescale/timescaledb-ha
095da3dbe359: Pull complete
...
```

```text
$ docker images -a
REPOSITORY                 TAG           IMAGE ID       CREATED         SIZE
timescale/timescaledb-ha   pg15-latest   ad39c4fbc5c4   2 months ago    3.37GB
...
```

```text
$ docker run -d --name timescaledb -p 5433:5432 -e POSTGRES_PASSWORD=mysecretpassword timescale/timescaledb-ha:pg15-latest
753a96bdd455e2819c44e529186979afbb759bfcd9674d2d3ca91b63466ad6ff
```

```text
$ docker ps -a
CONTAINER ID   IMAGE                                  COMMAND                  CREATED          STATUS          PORTS                                                           NAMES
753a96bdd455   timescale/timescaledb-ha:pg15-latest   "/docker-entrypoint.…"   32 seconds ago   Up 31 seconds   8008/tcp, 8081/tcp, 0.0.0.0:5433->5432/tcp, :::5433->5432/tcp   timescaledb
...
```

```text
$ PGPASSWORD=mysecretpassword psql -h localhost -U postgres
psql (15.3 (Ubuntu 15.3-1.pgdg18.04+1), server 15.2 (Ubuntu 15.2-1.pgdg22.04+1))
Type "help" for help.

postgres=#
```

## Compiling and running the datastore server

```text
$ go build -o server main/main.go && ./server
2023/07/05 15:16:41 starting server
```

## Environment variables

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

## Testing the datastore server with gRPCurl

The datastore server can be tested with [gRPCurl](https://github.com/fullstorydev/grpcurl) like
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
$ grpcurl -d '{"id": 1234, "metadata": {"field1": "value1", "field2": "value2", "field3": "value3"}}' -plaintext -proto protobuf/datastore.proto 127.0.0.1:50050 datastore.Datastore.AddTimeSeries
...
```

```text
$ grpcurl -d '{"tsobs": [{"tsid": 1234, "obs": [{"time": 10, "value": 123.456, "metadata": {"field1": "value1", "field2": "value2"}}]}]}' -plaintext -proto protobuf/datastore.proto 127.0.0.1:50050 datastore.Datastore.PutObservations
...
```

```text
$ grpcurl -d '{"tsids": [1234, 5678, 9012], "fromtime": 156, "totime": 163}' -plaintext -proto protobuf/datastore.proto 127.0.0.1:50050 datastore.Datastore.GetObservations
...
```

## Testing the datastore server with a Python client

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
