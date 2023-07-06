package common

import (
	"os"
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
