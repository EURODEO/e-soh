package postgres

import (
	"database/sql"
	"datastore/common"
	"fmt"

	_ "github.com/lib/pq"
)

// Postgres is an implementation of the StorageBackend interface that accesses
// data through a Postgres database.
type Postgres struct {
	Db *sql.DB
}

// Description ... (see documentation in StorageBackend interface)
func (sbe *Postgres) Description() string {
	return "Postgres database"
}

// NewPostgres creates and returns a new Postgres instance.
func NewPostgres() (*Postgres, error) {
	sbe := new(Postgres)

	// get connection info
	host := common.Getenv("PSBHOST", "localhost")
	port := common.Getenv("PSBPORT", "5433")
	user := common.Getenv("PSBUSER", "postgres")
	password := common.Getenv("PSBPASSWORD", "")
	dbname := "data" // hard-coded for now

    // create and validate connection to Postgres server

	pgInfo := fmt.Sprintf(
	    "host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
        host, port, user, password, dbname)
	var err error
	sbe.Db, err = sql.Open("postgres", pgInfo)
	if err != nil {
		return nil, fmt.Errorf("sql.Open() failed: %v", err)
	}

	if err = sbe.Db.Ping(); err != nil  {
		return nil, fmt.Errorf("sbe.Db.Ping() failed: %v", err)
	}

	// connection ok

	return sbe, nil
}
