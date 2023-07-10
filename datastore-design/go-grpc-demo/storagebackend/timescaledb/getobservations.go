package timescaledb

import (
	"database/sql"
	"datastore/datastore"
	"fmt"

	_ "github.com/lib/pq"
)

// retrieveObsForTS retrieves into obs observations for time series tsID in
// open-ended range [fromTime, toTime>.
// Returns nil upon success, otherwise error.
func retrieveObsForTS(
	db *sql.DB, tsID int64, fromTime, toTime int64, obs *[]*datastore.Observation) error {

	rows, err := db.Query(fmt.Sprintf(`
		SELECT EXTRACT(EPOCH FROM tstamp)::INT, value, field1, field2 FROM observations
		WHERE ts_id = %d
		AND tstamp >= to_timestamp(%d)
		AND tstamp <  to_timestamp(%d)
		ORDER BY tstamp ASC
	`, tsID, fromTime, toTime))
	if err != nil {
		return fmt.Errorf("db.Query() failed: %v", err)
	}
	for rows.Next() {
		var (
			obsTime int64
			obsVal float64
			field1, field2 string
		)
		if err := rows.Scan(&obsTime, &obsVal, &field1, &field2); err != nil {
			return fmt.Errorf("rows.Scan() failed: %v", err)
		}
		(*obs) = append(*obs, &datastore.Observation{
			Time: obsTime,
			Value: obsVal,
			Metadata: &datastore.ObsMetadata{
				Field1: field1,
				Field2: field2,
			},
		})
	}

	return nil
}

// GetObservations ... (see documentation in StorageBackend interface)
func (sbe *TimescaleDB) GetObservations(request *datastore.GetObsRequest) (
	*datastore.GetObsResponse, error) {

	// TODO: validate request.Tsids (ensure it doesn't contains duplicates etc.)
	tsObs := make([]*datastore.TSObservations, len(request.Tsids))
	for i, tsID := range request.Tsids {
		obs := []*datastore.Observation{}
		if err := retrieveObsForTS(
			sbe.Db, tsID, request.Fromtime, request.Totime, &obs); err != nil {
			return nil, fmt.Errorf("retrieveObsForTS() failed (i: %d, tsID: %d): %v", i, tsID, err)
		}
		tsObs[i] = &datastore.TSObservations{
			Tsid: tsID,
			Obs: obs,
		}
	}

	return &datastore.GetObsResponse{Tsobs: tsObs}, nil
}
