package main

import (
	"context"
	"datastore/common"
	"datastore/datastore"
	"datastore/dsimpl"
	"datastore/storagebackend"
	"datastore/storagebackend/postgresql"
	"fmt"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func createStorageBackend() (storagebackend.StorageBackend, error) {
	var sbe storagebackend.StorageBackend

	// only PostgreSQL supported for now
	sbe, err := postgresql.NewPostgreSQL()
	if err != nil {
		return nil, fmt.Errorf("postgresql.NewPostgreSQL() failed: %v", err)
	}

	return sbe, nil
}

func main() {
	reqTimeLogger := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		reqTime := time.Since(start)
		if info.FullMethod != "/grpc.health.v1.Health/Check" {
			log.Printf("time for method %q: %d ms", info.FullMethod, reqTime.Milliseconds())
		}
		return resp, err
	}

	// create gRPC server
	server := grpc.NewServer(grpc.UnaryInterceptor(reqTimeLogger))
	grpc_health_v1.RegisterHealthServer(server, health.NewServer())

	// create storage backend
	sbe, err := createStorageBackend()
	if err != nil {
		log.Fatalf("createStorageBackend() failed: %v", err)
	}

	// register service implementation
	var datastoreServer datastore.DatastoreServer = &dsimpl.ServiceInfo{
		Sbe: sbe,
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
