package timescaledb

import (
	"database/sql"
	"datastore/common"
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
			other1 TEXT, -- additional metadata to be defined
			other2 TEXT, -- ----''----
			other3 TEXT) -- ----''----
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
			PRIMARY KEY (ts_id, tstamp),
			field1 TEXT, -- additional metadata to be defined
			field2 TEXT) -- ----''----
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
