package timescaledb

import (
	"database/sql"
	"datastore/common"
	"fmt"

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

	sbe.Db, err = openDB(host, port, user, password, dbname)
	if err != nil {
		return nil, fmt.Errorf("openDB() failed: %v", err)
	}

	if err = sbe.Db.Ping(); err != nil  {
		return nil, fmt.Errorf("sbe.Db.Ping() failed: %v", err)
	}

	return sbe, nil
}
