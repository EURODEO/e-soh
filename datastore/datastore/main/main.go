package main

import (
	"context"
	"datastore/common"
	"datastore/datastore"
	"datastore/dsimpl"
	"datastore/storagebackend"
	"datastore/storagebackend/postgresql"
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net"
	"time"

	// gRPC
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/peer"

	// Monitoring
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	//"github.com/grpc-ecosystem/go-grpc-middleware"
	//"google.golang.org/grpc/ChainUnaryInterceptor"
	//"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	//"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	//"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	//"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/selector"
	//"github.com/prometheus/client_golang/prometheus"
	//"github.com/prometheus/client_golang/prometheus/promhttp"
	//"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	//"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	//"go.opentelemetry.io/otel/trace"

	_ "expvar"
	"net/http"
	_ "net/http/pprof"
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

	reqTimeLogger := func(
		ctx context.Context, req interface{}, info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		reqTime := time.Since(start)
		if info.FullMethod != "/grpc.health.v1.Health/Check" {
			var clientIp = "unknown"
			if p, ok := peer.FromContext(ctx); ok {
				clientIp = p.Addr.String()
			}
			log.Printf("time for method %q: %d ms. Client ip: %s", info.FullMethod, reqTime.Milliseconds(), clientIp)
		}
		return resp, err
	}

	//srvMetrics := grpcprom.NewServerMetrics()
	//reg := prometheus.NewRegistry()
	//reg.MustRegister(srvMetrics)
	//exemplarFromContext := func(ctx context.Context) prometheus.Labels {
	//	if span := trace.SpanContextFromContext(ctx); span.IsSampled() {
	//		return prometheus.Labels{"traceID": span.TraceID().String()}
	//	}
	//	return nil
	//}
	grpcMetrics := grpc_prometheus.NewServerMetrics()

	// create gRPC server with middleware
	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(reqTimeLogger),
		grpc.ChainUnaryInterceptor(
			grpcMetrics.UnaryServerInterceptor(),
		),
	)
	//server := grpc.NewServer(
	//	grpc.ChainUnaryInterceptor(reqTimeLogger),
	//	grpc.ChainUnaryInterceptor(
	//		// Order matters e.g. tracing interceptor have to create span first for the later exemplars to work.
	//		otelgrpc.UnaryServerInterceptor(),
	//		srvMetrics.UnaryServerInterceptor(grpcprom.WithExemplarFromContext(exemplarFromContext)),
	//		logging.UnaryServerInterceptor(interceptorLogger(rpcLogger), logging.WithFieldsFromContext(logTraceID)),
	//		recovery.UnaryServerInterceptor(recovery.WithRecoveryHandler(grpcPanicRecoveryHandler)),
	//	),
	//	grpc.ChainStreamInterceptor(
	//		otelgrpc.StreamServerInterceptor(),
	//		srvMetrics.StreamServerInterceptor(grpcprom.WithExemplarFromContext(exemplarFromContext)),
	//		logging.StreamServerInterceptor(interceptorLogger(rpcLogger), logging.WithFieldsFromContext(logTraceID)),
	//		recovery.StreamServerInterceptor(recovery.WithRecoveryHandler(grpcPanicRecoveryHandler)),
	//	),
	//)
	grpcMetrics.InitializeMetrics(server)
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

	go func() {
		log.Println("Starting HTTP server for Prometheus metrics on :8080")
		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(":8080", nil))
	}()

	// serve profiling info
	log.Printf("serving profiling info\n")
	go func() {
		http.ListenAndServe("0.0.0.0:6060", nil)
	}()

	// serve incoming requests
	log.Printf("starting server\n")
	if err := server.Serve(listener); err != nil {
		log.Fatalf("server.Serve() failed: %v", err)
	}
}
