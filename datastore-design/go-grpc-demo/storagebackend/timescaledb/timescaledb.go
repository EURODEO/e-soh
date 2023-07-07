package timescaledb

import (
	"database/sql"
	"datastore/common"
	"datastore/datastore"
	"fmt"
	"os/exec"

	_ "github.com/lib/pq"
)

// TimescaleDB is an implementation of the StorageBackend interface that
// keeps data in a TimescaleDB database.
type TimescaleDB struct {
	Db *sql.DB
}

// Description ... (see documentation in StorageBackend interface)
func (sbe *TimescaleDB) Description() string {
	return "TimescaleDB database"
}

// AddTimeSeries ... (see documentation in StorageBackend interface)
func (sbe *TimescaleDB) AddTimeSeries(request *datastore.AddTSRequest) error {
	var err error
	var rows *sql.Rows

    // check if time series ID already exists
	rows, err = sbe.Db.Query("SELECT id FROM time_series WHERE id = $1", request.Id)
	if err != nil {
		return fmt.Errorf("sbe.Db.Query(1) failed: %v", err)
	}
	if rows.Next() {
		return fmt.Errorf("time series ID %d already exists", request.Id)
	}

	// check if (station ID, param ID) combo already exists
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

// PutObservations ... (see documentation in StorageBackend interface)
func (sbe *TimescaleDB) PutObservations(request *datastore.PutObsRequest) error {
	// TODO
	return fmt.Errorf("TimescaleDB/PutObservations() not implemented yet")
}

// GetObservations ... (see documentation in StorageBackend interface)
func (sbe *TimescaleDB) GetObservations(request *datastore.GetObsRequest) (
	*datastore.GetObsResponse, error) {

	// TODO

	// tsobs := []*datastore.TSObservations{}

	// // --- BEGIN TODO: retrieve observations from database -----------
	// // ... for now:
	// var nSteps int64 = 3
	// max := func(x, y int64) int64 {
	// 	if x > y {
	// 		return x
	// 	}
	// 	return y
	// }
	// timeStep := max(1, (request.Totime - request.Fromtime) / nSteps)
	// for _, tsid := range request.Tsids {
	// 	obs := []*datastore.Observation{}
	// 	obsTime := request.Fromtime
	// 	for i := 0; obsTime <= request.Totime; i++ {
	// 		obsVal := 10 + float64(i)
	// 		obs = append(obs, &datastore.Observation{
	// 			Time: obsTime,
	// 			Value: obsVal,
	// 			Metadata: &datastore.ObsMetadata{
	// 				Field1: fmt.Sprintf("value1 (%d)", i),
	// 				Field2: fmt.Sprintf("value2 (%d)", i),
	// 			},
	// 		})
	// 		obsTime += timeStep
	// 	}
	// 	tsobs = append(tsobs, &datastore.TSObservations{
	// 		Tsid: tsid,
	// 		Obs: obs,
	// 	})
	// }
	// // --- END TODO: retrieve observations from storage backend -----------

	// return &datastore.GetObsResponse{
	// 	Status: -1, // for now
	// 	Tsobs: tsobs,
	// }, nil

	return nil, fmt.Errorf("TimescaleDB/GetObservations() not implemented yet")
}

// openDB opens database identified by host/port/user/password/dbname.
// Returns (DB, nil) upon success, otherwise (..., error).
func openDB(host, port, user, password, dbname string) (*sql.DB, error) {
	connInfo := fmt.Sprintf(
	    "host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
        host, port,	user, password, dbname)

	db, err := sql.Open("postgres", connInfo)
	if err != nil {
		return nil, fmt.Errorf("sql.Open() failed: %v", err)
	}

	return db, nil
}

// resetDB resets database identified by host/port/user/password/dbname
// by dropping, (re)creating, defining schema etc.
// Returns (DB, nil) upon success, otherwise (..., error).
func resetDB(host, port, user, password, dbname string) (
	*sql.DB, error) {
	execCmd := func(tag string, cmd *exec.Cmd) error {
		if cmbOutErr, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf(
				"cmd.CombinedOutput() failed for %s: %v: %s", tag, err,
				string(cmbOutErr))
		}
		return nil
	}

	// drop any existing database
	cmd := exec.Command(
		"dropdb", "-w", "-f", "--if-exists", "-h", host, "-p", port, "-U", user, dbname)
	if err := execCmd(cmd.Path, cmd); err != nil {
		return nil, fmt.Errorf("execCmd() failed: %v", err)
	}

	// (re)create database
	cmd = exec.Command("createdb", "-w", "-h", host, "-p", port, "-U", user, dbname)
	if err := execCmd(cmd.Path, cmd); err != nil {
		return nil, fmt.Errorf("execCmd() failed: %v", err)
	}

	// open database
	db, err := openDB(host, port, user, password, dbname)
	if err != nil {
		return nil, fmt.Errorf("openDB() failed: %v", err)
	}

	// create PostGIS extension
	_, err = db.Exec("CREATE EXTENSION postgis")
	if err != nil {
		return nil, fmt.Errorf("db.Exec(CREATE EXTENSION postgis) failed: %v", err)
	}

	// create time series table
	_, err = db.Exec(`
		CREATE TABLE time_series (
			id INTEGER PRIMARY KEY,
			station_id TEXT NOT NULL,
			param_id TEXT NOT NULL,
			UNIQUE (station_id, param_id),
			pos GEOGRAPHY(Point) NOT NULL,
			other1 TEXT,
			other2 TEXT,
			other3 TEXT)
		`)
	if err != nil {
		return nil, fmt.Errorf(
			"db.Exec(CREATE TABLE time_series ...) failed: %v", err)
	}

	// create observations table
	_, err = db.Exec(`
		CREATE TABLE observations (
			ts_id integer REFERENCES time_series(id) ON DELETE CASCADE,
			tstamp timestamp, -- obs time (NOT NULL, but implied by being part of PK)
			value double precision, -- obs value
			PRIMARY KEY (ts_id, tstamp))
		`)
	if err != nil {
		return nil, fmt.Errorf("db.Exec(CREATE TABLE observations ...) failed: %v", err)
	}

	// convert observations table to hypertable
	_, err = db.Exec(`
		SELECT create_hypertable(
			'observations', 'tstamp', chunk_time_interval => INTERVAL '1 hour')
		`)
	if err != nil {
		return nil, fmt.Errorf("db.Exec(SELECT create_hypertable ...) failed: %v", err)
	}

	return db, nil
}

// NewTimescaleDB creates a new TimescaleDB instance.
// Returns (instance, nil) upon success, otherwise (..., error).
func NewTimescaleDB() (*TimescaleDB, error) {
	sbe := new(TimescaleDB)

	host := common.Getenv("TSDBHOST", "localhost")
	port := common.Getenv("TSDBPORT", "5433")
	user := common.Getenv("TSDBUSER", "postgres")
	password := common.Getenv("TSDBPASSWORD", "mysecretpassword")
	dbname := common.Getenv("TSDBDBNAME", "data")

	var err error

	if common.Getenv("TSDBRESET", "false") == "true" { // reset
		sbe.Db, err = resetDB(host, port, user, password, dbname)
		if err != nil {
			return nil, fmt.Errorf("resetDB() failed: %v", err)
		}
	} else { // don't reset
		sbe.Db, err = openDB(host, port, user, password, dbname)
		if err != nil {
			return nil, fmt.Errorf("openDB() failed: %v", err)
		}
	}

	if err = sbe.Db.Ping(); err != nil  {
		return nil, fmt.Errorf("sbe.Db.Ping() failed: %v", err)
	}

	return sbe, nil
}
