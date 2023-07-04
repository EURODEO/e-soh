package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	"datastore/datastore"

	"google.golang.org/grpc"
)

type args struct {
	Arg1 string // for now
	Arg2 string // for now
}

type serviceInfo struct {
	args args
}

// --- BEGIN implementation of the Datastore service --------------------------

func (svcInfo *serviceInfo) AddTimeSeries(
	ctx context.Context, request *datastore.AddTSRequest) (
		*datastore.AddTSResponse, error) {

	fmt.Printf(
		"AddTimeSeries(); Id: %v; Metadata: %v\n", request.Id, request.Metadata)

	// TODO: add time series to storage backend

	response := datastore.AddTSResponse{
		Status: -1, // for now
	}

	return &response, nil
}

func (svcInfo *serviceInfo) PutObservations(
	ctx context.Context, request *datastore.PutObsRequest) (
		*datastore.PutObsResponse, error) {

	fmt.Printf("PutObservations(); Tsobs: %v\n", request.Tsobs)

	// TODO: add observations to storage backend

	response := datastore.PutObsResponse{
		Status: -1, // for now
	}

	return &response, nil
}

func (svcInfo *serviceInfo) GetObservations(
	ctx context.Context, request *datastore.GetObsRequest) (
		*datastore.GetObsResponse, error) {

	fmt.Printf(
		"GetObservations(); Tsids: %v; From: %v; To: %v\n",
		request.Tsids, request.From, request.To)

	if request.To < request.From {
		return nil, fmt.Errorf("To(%d) < From(%d)", request.To, request.From)
	}

	tsobs := []*datastore.TSObservations{}

	// --- BEGIN TODO: retrieve observations from storage backend -----------
	// ... for now:
	var nSteps int64 = 3
	max := func(x, y int64) int64 {
		if x > y {
			return x
		}
		return y
	}
	timeStep := max(1, (request.To - request.From) / nSteps)
	for _, tsid := range request.Tsids {
		obs := []*datastore.Observation{}
		obsTime := request.From
		for i := 0; obsTime <= request.To; i++ {
			obsVal := 10 + float64(i)
			obs = append(obs, &datastore.Observation{
				Time: obsTime,
				Value: obsVal,
				Metadata: &datastore.ObsMetadata{
					Field1: fmt.Sprintf("dummy1 (%d)", i),
					Field2: fmt.Sprintf("dummy2 (%d)", i),
				},
			})
			obsTime += timeStep
		}
		tsobs = append(tsobs, &datastore.TSObservations{
			Tsid: tsid,
			Obs: obs,
		})
	}
	// --- END TODO: retrieve observations from storage backend -----------

	response := datastore.GetObsResponse{
		Status: -1, // for now
		Tsobs: tsobs,
	}

	return &response, nil
}

// --- END implementation of the Datastore service --------------------------

// parseArgs parses and returns command-line arguments.
func parseArgs() *args {
	arg1 := flag.String("arg1", "dummy", "argument 1 (just an example for now)")
	arg2 := flag.String("arg2", "dummy", "argument 2 (just an example for now)")

	flag.Parse()

	return &args{
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
	var datastoreServer datastore.DatastoreServer = &serviceInfo{*args}
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
