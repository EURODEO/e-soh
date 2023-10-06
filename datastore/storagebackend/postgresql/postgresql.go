package postgresql

import (
	"database/sql"
	"datastore/common"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
)

// PostgreSQL is an implementation of the StorageBackend interface that
// keeps data in a PostgreSQL database.
type PostgreSQL struct {
	Db *sql.DB
}

// Description ... (see documentation in StorageBackend interface)
func (sbe *PostgreSQL) Description() string {
	return "PostgreSQL database"
}

// openDB opens database identified by host/port/user/password/dbname.
// Returns (DB, nil) upon success, otherwise (..., error).
func openDB(host, port, user, password, dbname string) (*sql.DB, error) {
	connInfo := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", connInfo)
	if err != nil {
		return nil, fmt.Errorf("sql.Open() failed: %v", err)
	}

	return db, nil
}

// NewPostgreSQL creates a new PostgreSQL instance.
// Returns (instance, nil) upon success, otherwise (..., error).
func NewPostgreSQL() (*PostgreSQL, error) {
	sbe := new(PostgreSQL)

	host := common.Getenv("PGHOST", "localhost")
	port := common.Getenv("PGPORT", "5433")
	user := common.Getenv("PGUSER", "postgres")
	password := common.Getenv("PGPASSWORD", "mysecretpassword")
	dbname := common.Getenv("PGDBNAME", "data")

	var err error

	sbe.Db, err = openDB(host, port, user, password, dbname)
	if err != nil {
		return nil, fmt.Errorf("openDB() failed: %v", err)
	}

	if err = sbe.Db.Ping(); err != nil {
		return nil, fmt.Errorf("sbe.Db.Ping() failed: %v", err)
	}

	return sbe, nil
}

// getTSMdataCols returns time series metadata column names.
func getTSMdataCols() []string {
	return []string{
		// main section
		"version",
		"type",
		"title",
		"summary",
		"keywords",
		"keywords_vocabulary",
		"license",
		"conventions",
		"naming_authority",
		"creator_type",
		"creator_name",
		"creator_email",
		"creator_url",
		"institution",
		"project",
		"source",
		"platform",
		"platform_vocabulary",
		"standard_name",
		"unit",
		"instrument",
		"instrument_vocabulary",
		// links section
		"link_href",
		"link_rel",
		"link_type",
		"link_hreflang",
		"link_title",
	}
}

// createPlaceholders returns the list of n placeholder strings for
// values in a parameterized query, e.g. $1, to_timestamp($2), ..., $n.
// Items in formats must be strings containing exactly one "$%d" pattern,
// e.g. "$%d", "to_timestamp($%d)" etc.
func createPlaceholders(formats []string) []string {
	phs := []string{}
	for i, format := range formats {
		index := i + 1
		ph := fmt.Sprintf(format, index)
		phs = append(phs, ph)
	}
	return phs
}

// createSetFilter creates expression used in a WHERE clause for testing
// if the value in column colName is included in a set of string values.
// The filter is fully closed (--> return FALSE) if the set non-nil but empty.
// Returns expression, TRUE or FALSE.
func createSetFilter(colName string, vals []string) string {
	// assert(vals != nil)
	if len(vals) == 0 {
		return "FALSE" // set requested, but nothing will match
	}
	return fmt.Sprintf("(%s IN (%s))", colName, strings.Join(vals, ","))
}
