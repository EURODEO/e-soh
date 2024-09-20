package promservermetrics

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"sync"
	"time"
)

var (
	UptimeCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "grpc_server_uptime_seconds",
		Help: "Total uptime of the gRPC server in seconds",
	})

	ActiveConnections = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "grpc_active_connections",
		Help: "Current number of active gRPC connections",
	})

	ResponseSizeSummary = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "grpc_response_size_summary_bytes",
			Help:       "Summary of response sizes in bytes for each gRPC method, with mean, min, and max",
			Objectives: map[float64]float64{0.0: 0.001, 1.0: 0.001}, // Track min (0.0 quantile) and max (1.0 quantile)
		},
		[]string{"method"},
	)

	responseSizeMu    sync.Mutex
	responseSizeSum   = make(map[string]float64)
	responseSizeCount = make(map[string]float64)
)

func TrackUptime() {
	for {
		UptimeCounter.Inc() // Increment the uptime every second
		time.Sleep(1 * time.Second)
	}
}

func ConnectionUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	ActiveConnections.Inc()       // Increment when a new unary request (connection) is opened
	defer ActiveConnections.Dec() // Decrement when the unary request (connection) is completed
	return handler(ctx, req)
}

func ResponseSizeUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	resp, err = handler(ctx, req)

	// Measure the response size if response is not nil
	if resp != nil {
		responseSize := float64(len(resp.(interface{ String() string }).String()))

		// Record the response size in the summary for quantile calculations
		ResponseSizeSummary.WithLabelValues(info.FullMethod).Observe(responseSize)

		// Calculate the total sum and count for mean calculation
		responseSizeMu.Lock()
		responseSizeSum[info.FullMethod] += responseSize
		responseSizeCount[info.FullMethod]++
		responseSizeMu.Unlock()
	}

	return resp, err
}
