package timescaledb

import (
	"datastore/datastore"
	"fmt"

	_ "github.com/lib/pq"
)

// DeleteTimeSeries ... (see documentation in StorageBackend interface)
func (sbe *TimescaleDB) DeleteTimeSeries(request *datastore.DeleteTSRequest) error {

	for _, id := range request.Ids {
		// delete any time series with this id (including all associated rows
		// in the 'observations' table)
		cmd := "DELETE FROM time_series WHERE id = $1"
		_, err := sbe.Db.Exec(cmd, id)
		if err != nil {
			return fmt.Errorf("sbe.Db.Exec() failed: %v", err)
		}
	}

	return nil
}
