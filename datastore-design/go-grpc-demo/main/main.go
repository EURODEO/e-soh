package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	"datastore/datastore"
	"datastore/dsimpl"

	"google.golang.org/grpc"
)

// parseArgs parses and returns command-line arguments.
func parseArgs() *dsimpl.ServiceArgs {
	arg1 := flag.String("arg1", "dummy", "argument 1 (just an example for now)")
	arg2 := flag.String("arg2", "dummy", "argument 2 (just an example for now)")

	flag.Parse()

	return &dsimpl.ServiceArgs{
		Arg1: *arg1,
		Arg2: *arg2,
	}
}

// getenv returns the value of an environment variable or a default value if
// no such environment variable has been set.
func getenv(key string, defaultValue string) string {
	var value string
	var ok bool
	if value, ok = os.LookupEnv(key); !ok {
		value = defaultValue
	}
	return value
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
	port := getenv("SERVERPORT", "50050")
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
