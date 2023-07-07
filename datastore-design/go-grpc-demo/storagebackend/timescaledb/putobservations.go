package timescaledb

import (
	"datastore/datastore"
	"fmt"

	_ "github.com/lib/pq"
)

// PutObservations ... (see documentation in StorageBackend interface)
func (sbe *TimescaleDB) PutObservations(request *datastore.PutObsRequest) error {
	// TODO
	return fmt.Errorf("TimescaleDB/PutObservations() not implemented yet")
}
