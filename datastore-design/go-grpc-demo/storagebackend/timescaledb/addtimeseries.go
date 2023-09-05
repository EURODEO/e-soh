package timescaledb

import (
	"database/sql"
	"datastore/datastore"
	"fmt"

	_ "github.com/lib/pq"
)

// AddTimeSeries ... (see documentation in StorageBackend interface)
func (sbe *TimescaleDB) AddTimeSeries(request *datastore.AddTSRequest) error {
	var err error
	var rows *sql.Rows

    // ensure that the time series ID doesn't already exist
	rows, err = sbe.Db.Query("SELECT id FROM time_series WHERE id = $1", request.Id)
	if err != nil {
		return fmt.Errorf("sbe.Db.Query(1) failed: %v", err)
	}
	if rows.Next() {
		return fmt.Errorf("time series ID %d already exists", request.Id)
	}
	rows.Close()

	// ensure that the (station ID, param ID) combo doesn't already exist
	rows, err = sbe.Db.Query(
		"SELECT id FROM time_series WHERE station_id = $1 AND param_id = $2",
		request.Metadata.StationId, request.Metadata.ParamId)
	if err != nil {
		return fmt.Errorf("sbe.Db.Query(2) failed: %v", err)
	}
	if rows.Next() {
		var id int64
		err = rows.Scan(&id)
		if err != nil {
			return fmt.Errorf("rows.Scan(&id) failed: %v", err)
		}
		return fmt.Errorf(
			"(station_id, param_id) combo (%s, %s) already exists for time series ID %d",
			request.Metadata.StationId, request.Metadata.ParamId, id)
	}
	defer rows.Close()

	// insert new time series
    cmd := `
	    INSERT INTO time_series (id, station_id, param_id, pos, other1, other2, other3)
        VALUES ($1, $2, $3, ST_MakePoint($4, $5), $6, $7, $8)
    `
    _, err = sbe.Db.Exec(
	    cmd, request.Id, request.Metadata.StationId, request.Metadata.ParamId, request.Metadata.Lon,
		request.Metadata.Lat, request.Metadata.Other1, request.Metadata.Other2,
		request.Metadata.Other3)
	if err != nil {
		return fmt.Errorf("sbe.Db.Exec() failed: %v", err)
	}

	return nil
}
