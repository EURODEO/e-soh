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

	InFlightRequests = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "grpc_in_flight_requests",
		Help: "Current number of in-flight gRPC requests",
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
	// Increment the uptime every second
	for {
		UptimeCounter.Inc()
		time.Sleep(1 * time.Second)
	}
}

func InFlightRequestInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	InFlightRequests.Inc()       // Increment at the start of the request
	defer InFlightRequests.Dec() // Decrement at the end of the request
	return handler(ctx, req)
}

func ResponseSizeUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	resp, err = handler(ctx, req)

	if resp != nil {
		responseSize := float64(len(resp.(interface{ String() string }).String()))

		ResponseSizeSummary.WithLabelValues(info.FullMethod).Observe(responseSize)

		// Used a mutex to synchronise the access for the responseSizeSum and responseSizeCount.
		// To prevent race conditions and multiple goroutines accessing the variables at the same time.
		responseSizeMu.Lock()
		responseSizeSum[info.FullMethod] += responseSize
		responseSizeCount[info.FullMethod]++
		responseSizeMu.Unlock()
	}

	return resp, err
}
