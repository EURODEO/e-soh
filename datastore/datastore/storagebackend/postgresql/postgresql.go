package postgresql

import (
	"database/sql"
	"datastore/common"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

var (
	cleanupInterval time.Duration
	lastCleanupTime time.Time
)

// initCleanupInterval initializes cleanupInterval.
func initCleanupInterval() {
	name := "CLEANUPINTERVAL"
	defaultVal := int64(86400)
	val0 := strings.ToLower(common.Getenv(name, fmt.Sprintf("%d", defaultVal)))

	val, err := strconv.ParseInt(val0, 10, 64)
	if err != nil {
		log.Printf(
			"WARNING: failed to parse %s as an int64: %s; falling back to default secs: %d",
			name, val0, defaultVal)
		val = defaultVal
	}

	cleanupInterval = time.Duration(val)
}

func init() { // automatically called once on program startup (on first import of this package)
	initCleanupInterval()
	lastCleanupTime = time.Time{}
}

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
	// ### TODO: modify to use reflection instead of explicit field referrals
	return []string{
		// links section (aka. non-string metadata ...)
		"link_href",
		"link_rel",
		"link_type",
		"link_hreflang",
		"link_title",
		// main section (aka. string metadata ...)
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

// cleanup performs various cleanup tasks, like removing old observations from the database.
func cleanup(db *sql.DB) error {

	// start transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("db.Begin() failed: %v", err)
	}
	defer tx.Rollback()

	// remove observations outside valid range
	loTime, hiTime := common.GetValidTimeRange()
	cmd := fmt.Sprintf(`
		DELETE FROM observation
		WHERE (obstime_instant < to_timestamp(%d))
		   OR (obstime_instant > to_timestamp(%d))
	`, loTime.Unix(), hiTime.Unix())
	_, err = tx.Exec(cmd)
	if err != nil {
		return fmt.Errorf("tx.Exec() failed: %v", err)
	}

	// DELETE FROM time_series WHERE <no FK refs from observation anymore> ... TODO!
	// DELETE FROM geo_points WHERE <no FK refs from observation anymore> ... TODO!

	// commit transaction
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("tx.Commit() failed: %v", err)
	}

	lastCleanupTime = time.Now()

	return nil
}

// considerCleanup considers if cleanup() should be called.
func considerCleanup(db *sql.DB) error {
	// call cleanup() if at least cleanupInterval has passed since the last time it was called
	if time.Duration(time.Now().Sub(lastCleanupTime)) > cleanupInterval {
		if err := cleanup(db); err != nil {
			return fmt.Errorf("cleanup() failed: %v", err)
		}
	}

	return nil
}
