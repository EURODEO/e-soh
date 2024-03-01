package postgresql

import (
	"database/sql"
	"datastore/common"
	"fmt"
	"regexp"
	"strings"
	"time"

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

// setTSUniqueMainCols extracts into tsStringMdataPBNamesUnique the columns comprising constraint
// unique_main in table time_series.
//
// Returns nil upon success, otherwise error.
func (sbe *PostgreSQL) setTSUniqueMainCols() error {

	query := `
		SELECT pg_get_constraintdef(c.oid)
		FROM pg_constraint c
		JOIN pg_namespace n ON n.oid = c.connamespace
		WHERE conrelid::regclass::text = 'time_series'
			AND conname = 'unique_main'
			AND contype = 'u'
	`

	/* typical example of running the above query:

	$ PGPASSWORD=mysecretpassword psql -h localhost -p 5433 -U postgres -d data -c \
	> "SELECT pg_get_constraintdef(c.oid) FROM pg_constraint c JOIN pg_namespace n
	> ON n.oid = c.connamespace WHERE conrelid::regclass::text = 'time_series'
	> AND conname = 'unique_main' AND contype = 'u'"
									              pg_get_constraintdef
	-----------------------------------------------------------------------------------------------
	-------------
	UNIQUE NULLS NOT DISTINCT (naming_authority, platform, standard_name, level, function, period,
		instrument)
		(1 row)

	*/

	row := sbe.Db.QueryRow(query)

	var result string
	err := row.Scan(&result)
	if err != nil {
		return fmt.Errorf("row.Scan() failed: %v", err)
	}

	pattern := `\((.*)\)`
	re := regexp.MustCompile(pattern)
	matches := re.FindStringSubmatch(result)
	if len(matches) != 2 {
		return fmt.Errorf("'%s' didn't match regexp pattern '%s'", result, pattern)
	}

	// create tsStringMdataPBNamesUnique
	tsStringMdataPBNamesUnique = strings.Split(matches[1], ",")
	for i := 0; i < len(tsStringMdataPBNamesUnique); i++ {
		tsStringMdataPBNamesUnique[i] = strings.TrimSpace(tsStringMdataPBNamesUnique[i])
	}

	return nil
}

// setUpsertTSInsertCmd sets upsertTSInsertCmd to be used by upsertTS.
func setUpsertTSInsertCmd() {

	cols := getTSMdataCols()

	formats := make([]string, len(cols))
	for i := 0; i < len(cols); i++ {
		formats[i] = "$%d"
	}

	updateExpr := []string{}
	for _, col := range getTSMdataColsUniqueCompl() {
		updateExpr = append(updateExpr, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
	}

	upsertTSInsertCmd = fmt.Sprintf(`
		INSERT INTO time_series (%s) VALUES (%s)
		ON CONFLICT ON CONSTRAINT unique_main DO UPDATE SET %s
		`,
		strings.Join(cols, ","),
		strings.Join(createPlaceholders(formats), ","),
		strings.Join(updateExpr, ","),
	)
}

// setUpsertTSSelectCmd sets upsertTSSelectCmd to be used by upsertTS.
func setUpsertTSSelectCmd() {

	whereExpr := []string{}
	for i, col := range getTSMdataColsUnique() {
		whereExpr = append(whereExpr, fmt.Sprintf("%s=$%d", col, i+1))
	}

	upsertTSSelectCmd = fmt.Sprintf(
		`SELECT id FROM time_series WHERE %s`, strings.Join(whereExpr, " AND "))
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

	err = sbe.setTSUniqueMainCols()
	if err != nil {
		return nil, fmt.Errorf("sbe.setTSUniqueMainCols() failed: %v", err)
	}

	setUpsertTSInsertCmd()
	setUpsertTSSelectCmd()

	return sbe, nil
}

// getTSMdataCols returns time series metadata column names.
func getTSMdataCols() []string {

	// initialize cols with non-string metadata
	cols := []string{
		"link_href",
		"link_rel",
		"link_type",
		"link_hreflang",
		"link_title",
	}

	// complete cols with string metadata (handleable with reflection)
	cols = append(cols, tsStringMdataPBNames...)

	return cols
}

// getTSMdataColsUnique returns the fields defined in constraint unique_main in table
// time_series.
func getTSMdataColsUnique() []string {
	return tsStringMdataPBNamesUnique
}

// getTSMdataColsUniqueCompl returns the complement of the set of fields defined in constraint
// unique_main in table time_series, i.e. getTSMdataCols() - getTSMdataColsUnique().
func getTSMdataColsUniqueCompl() []string {

	colSet := map[string]struct{}{}

	for _, col := range getTSMdataCols() { // start with all columns
		colSet[col] = struct{}{}
	}

	for _, col := range getTSMdataColsUnique() { // remove columns of the unique_main constraint
		delete(colSet, col)
	}

	// return remaining columns

	result := make([]string, len(colSet))
	i := 0
	for col := range colSet {
		result[i] = col
		i++
	}

	return result
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
	if time.Since(lastCleanupTime) > cleanupInterval {
		if err := cleanup(db); err != nil {
			return fmt.Errorf("cleanup() failed: %v", err)
		}
	}

	return nil
}
