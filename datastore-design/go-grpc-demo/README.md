# E-SOH datastore variant 1: gRPC service written in Go

## Overview

This directory contains code that demonstrates how the E-SOH datastore could
be implemented as a gRPC service written in Go.

The code has been tested in the following environment:

- OS: Ubuntu 18.04 Bionic
- Go: 1.20.5
- protoc: libprotoc 3.0.0

## Compiling the protobuf file

If necessary, compile the protobuf file first:

```text
protoc protobuf/datastore.proto --go_out=plugins=grpc:.
```

(This generates the directory/file `datastore/datastore.pb.go`)

## Generating a go.sum file

If necessary, generate a `go.sum` file:

```text
go mod tidy
```

### Compiling and running the server

```text
go build -o server main/main.go && ./server
```

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
  rpc GetObservations ( .datastore.GetObsRequest ) returns ( .datastore.GetObsResponse );
  //rpc DescribeTimeSeries(DescribeTSRequest) returns (DescribeTSResponse);
  //rpc FindTimeSeries(FindTSRequest) returns (FindTSResponse);
  //rpc DeleteTimeSeries(DeleteTSRequest) returns (DeleteTSResponse);
  //rpc UpdateTimeSeries(UpdateTSRequest) returns (UpdateTSResponse);
  rpc PutObservations ( .datastore.PutObsRequest ) returns ( .datastore.PutObsResponse );
}
```

```text
$ grpcurl -d '{"id": 1234, "metadata": {"field1": "value1", "field2": "value2", "field3": "value3"}}' -plaintext -proto protobuf/datastore.proto 127.0.0.1:50050 datastore.Datastore.AddTimeSeries
{
  "status": -1
}
```

```text
$ grpcurl -d '{"tsobs": [{"tsid": 1234, "obs": [{"time": 10, "value": 123.456, "metadata": {"field1": "dummy1", "field2": "dummy2"}}]}]}' -plaintext -proto protobuf/datastore.proto 127.0.0.1:50050 datastore.Datastore.PutObservations
{
  "status": -1
}
```

```text
$ grpcurl -d '{"tsids": [1234, 5678, 9012], "from": 156, "to": 163}' -plaintext -proto protobuf/datastore.proto 127.0.0.1:50050 datastore.Datastore.GetObservations
...
```

## Testing the server with a Python client

Some [general documentation](https://grpc.io/docs/languages/python) of Python gRPC clients
can be useful as a background.

The python demo client can be run like this:

```text
<command line to go here>
```
