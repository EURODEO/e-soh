package main

import (
	"context"
	"datastore/common"
	"datastore/datastore"
	"datastore/dsimpl"
	"datastore/storagebackend"
	"datastore/storagebackend/postgresql"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/trace"
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

	grpcMetrics := grpc_prometheus.NewServerMetrics(
		grpc_prometheus.WithServerHandlingTimeHistogram(
			grpc_prometheus.WithHistogramBuckets([]float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120}),
		),
	)
	reg := prometheus.NewRegistry()
	reg.MustRegister(grpcMetrics)
	exemplarFromContext := func(ctx context.Context) prometheus.Labels {
		if span := trace.SpanContextFromContext(ctx); span.IsSampled() {
			return prometheus.Labels{"traceID": span.TraceID().String()}
		}
		return nil
	}

	// create gRPC server with middleware
	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			reqTimeLogger,
			grpcMetrics.UnaryServerInterceptor(grpc_prometheus.WithExemplarFromContext(exemplarFromContext)),
		),
		grpc.ChainStreamInterceptor(
			grpcMetrics.StreamServerInterceptor(grpc_prometheus.WithExemplarFromContext(exemplarFromContext)),
		),
	)

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

	// serve profiling info
	log.Printf("serving profiling info\n")
	go func() {
		http.ListenAndServe("0.0.0.0:6060", nil)
	}()

	go func() {
		httpSrv := &http.Server{Addr: "0.0.0.0:8081"}
		m := http.NewServeMux()
		//log.Println("Starting HTTP server for Prometheus metrics on :8081")
		// Create HTTP handler for Prometheus metrics.
		m.Handle("/metrics", promhttp.HandlerFor(
			reg,
			promhttp.HandlerOpts{
				// Opt into OpenMetrics e.g. to support exemplars.
				EnableOpenMetrics: true,
			},
		))
		httpSrv.Handler = m
		log.Println("Starting HTTP server for Prometheus metrics on :8081")
		log.Fatal(httpSrv.ListenAndServe())
	}()

	// serve incoming requests
	log.Printf("starting server\n")
	if err := server.Serve(listener); err != nil {
		log.Fatalf("server.Serve() failed: %v", err)
	}
}
