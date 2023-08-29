package timescaledb

import (
	"database/sql"
	"datastore/datastore"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
)

// createInsertVals generates from tsObs two arrays:
//   - in valsExpr: the list of arguments to the VALUES clause in the SQL INSERT
//     statement, and
//   - in vals: the total, flat list of all values.
func createInsertVals(
	tsObs *datastore.TSObservations, valsExpr *[]string, vals *[]interface{}) {
	index := 0
	for _, obs := range tsObs.Obs {
		vals0 := []interface{}{
			tsObs.Tsid,
			float64(obs.Time.Seconds) + float64(obs.Time.Nanos)/1e9,
			obs.Value,
			obs.Metadata.Field1,
			obs.Metadata.Field2,
		}
		// TODO: only add observations that are within the valid time range
		// (typically: [now - 24h, now])
		*valsExpr = append(
			*valsExpr, fmt.Sprintf("($%d, to_timestamp($%d), $%d, $%d, $%d)",
				index+1, index+2, index+3, index+4, index+5))
		*vals = append(*vals, vals0...)
		index += len(vals0)
	}
}

// insertObsForTS inserts new observations and/or updates existing ones.
// Returns nil upon success, otherwise error.
func insertObsForTS(db *sql.DB, tsObs *datastore.TSObservations) error {
	var err error
	var rows *sql.Rows

	// ensure that the time series ID already exists
	rows, err = db.Query("SELECT id FROM time_series WHERE id = $1", tsObs.Tsid)
	if err != nil {
		return fmt.Errorf("db.Query() failed: %v", err)
	}
	if !rows.Next() {
		return fmt.Errorf("time series ID %d not found", tsObs.Tsid)
	}
	defer rows.Close()

	// insert or update (i.e. "upsert") observations for this time series ID

	valsExpr := []string{}
	vals := []interface{}{}
	createInsertVals(tsObs, &valsExpr, &vals)

	cmd := fmt.Sprintf(`
		INSERT INTO observations (ts_id, tstamp, value, field1, field2)
		VALUES %s
		ON CONFLICT ON CONSTRAINT observations_pkey DO UPDATE SET
		    value  = EXCLUDED.value,
			field1 = EXCLUDED.field1,
			field2 = EXCLUDED.field2
    `, strings.Join(valsExpr, ","))

	_, err = db.Exec(cmd, vals...)
	if err != nil {
		return fmt.Errorf("db.Exec() failed: %v", err)
	}

	// TODO: delete observations that have now become too old
	// (possibly this could be done less often in some background thread)

	return nil
}

// PutObservations ... (see documentation in StorageBackend interface)
func (sbe *TimescaleDB) PutObservations(request *datastore.PutObsRequest) error {
	for _, tsObs := range request.Tsobs {
		if err := insertObsForTS(sbe.Db, tsObs); err != nil {
			return fmt.Errorf("insertObsForTS() failed: %v", err)
		}
	}

	return nil
}
