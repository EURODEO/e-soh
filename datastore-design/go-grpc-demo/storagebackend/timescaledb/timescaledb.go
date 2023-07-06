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
	// TODO
	return fmt.Errorf("TimescaleDB/AddTimeSeries() not implemented yet")
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
	cmd = exec.Command(
		"createdb", "-w", "-h", host, "-p", port, "-U", user, dbname)
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
		return nil, fmt.Errorf("db.Exec() failed: %v", err)
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
