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

## Compiling and running the server

```text
$ go build -o server main/main.go && ./server
2023/07/05 15:16:41 starting server
```

## Environment variables

The following environment variables are supported:

Variable | Mandatory | Default value | Description
:--      | :--       | :--           | :--
`SERVERPORT`| No  | `50050` | Server port number

## Testing the server with gRPCurl

The server can be tested with [gRPCurl](https://github.com/fullstorydev/grpcurl) like this:

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
{
  "status": -1
}
```

```text
$ grpcurl -d '{"tsobs": [{"tsid": 1234, "obs": [{"time": 10, "value": 123.456, "metadata": {"field1": "value1", "field2": "value2"}}]}]}' -plaintext -proto protobuf/datastore.proto 127.0.0.1:50050 datastore.Datastore.PutObservations
{
  "status": -1
}
```

```text
$ grpcurl -d '{"tsids": [1234, 5678, 9012], "fromtime": 156, "totime": 163}' -plaintext -proto protobuf/datastore.proto 127.0.0.1:50050 datastore.Datastore.GetObservations
{
  "status": -1,
  "tsobs": [
    {
      "tsid": "1234",
      "obs": [
        {
          "time": "156",
          "value": 10,
          "metadata": {
            "field1": "value1 (0)",
...
```

## Testing the server with a Python client

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
