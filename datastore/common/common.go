package common

import (
	"os"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// Getenv returns the value of an environment variable or a default value if
// no such environment variable has been set.
func Getenv(key string, defaultValue string) string {
	var value string
	var ok bool
	if value, ok = os.LookupEnv(key); !ok {
		value = defaultValue
	}
	return value
}

// Tstamp2float64secs returns the integer + fractional secs of
// a timestamp as a float64 value.
func Tstamp2float64Secs(tstamp *timestamppb.Timestamp) float64 {
	return float64(tstamp.GetSeconds()) + float64(tstamp.GetNanos())/1e9
}
