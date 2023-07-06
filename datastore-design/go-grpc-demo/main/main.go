package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"datastore/common"
	"datastore/datastore"
	"datastore/dsimpl"

	"google.golang.org/grpc"
)

// parseArgs parses and returns command-line arguments.
func parseArgs() *dsimpl.ServiceArgs {
	arg1 := flag.String("arg1", "value1", "argument 1 (just an example for now)")
	arg2 := flag.String("arg2", "value2", "argument 2 (just an example for now)")

	flag.Parse()

	return &dsimpl.ServiceArgs{
		Arg1: *arg1,
		Arg2: *arg2,
	}
}

func main() {
	args := parseArgs()

	// create gRPC server
	server := grpc.NewServer()

	// register service implementation
	var datastoreServer datastore.DatastoreServer = &dsimpl.ServiceInfo{
		Args: *args,
	}
	datastore.RegisterDatastoreServer(server, datastoreServer)

	// define network/port
	port := common.Getenv("SERVERPORT", "50050")
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatalf("net.Listen() failed: %v", err)
	}

	// serve incoming requests
	log.Printf("starting server\n")
	if err := server.Serve(listener); err != nil {
		log.Fatalf("server.Serve() failed: %v", err)
	}
}
