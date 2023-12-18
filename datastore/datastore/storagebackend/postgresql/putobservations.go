package postgresql

import (
	"database/sql"
	"datastore/common"
	"datastore/datastore"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	putObsLimit int // max # of observations in a single call to PutObservations
)

func init() { // automatically called once on program startup (on first import of this package)
	initPutObsLimit()
}

// initPutObsLimit initializes putObsLimit from environment variable PUTOBSLIMIT.
func initPutObsLimit() {
	name := "PUTOBSLIMIT"
	defaultLimit := 100000
	limitS := common.Getenv(name, fmt.Sprintf("%d", defaultLimit))

	var err error
	putObsLimit, err = strconv.Atoi(limitS)
	if (err != nil) || (putObsLimit < 1) {
		log.Printf(
			"WARNING: failed to parse %s as a positive integer: %s; falling back to default: %d",
			name, limitS, defaultLimit)
		putObsLimit = defaultLimit
	}
}

// getTSColVals gets the time series metadata column values from tsMdata.
// Returns (column values, nil) upon success, otherwise (..., error).
func getTSColVals(tsMdata *datastore.TSMetadata) ([]interface{}, error) {

	colVals := []interface{}{}

	// --- BEGIN non-string metadata ---------------------------

	getLinkVals := func(key string) ([]string, error) {
		linkVals := []string{}
		for _, link := range tsMdata.GetLinks() {
			var val string
			switch key {
			case "href":
				val = link.GetHref()
			case "rel":
				val = link.GetRel()
			case "type":
				val = link.GetType()
			case "hreflang":
				val = link.GetHreflang()
			case "title":
				val = link.GetTitle()
			default:
				return nil, fmt.Errorf("unsupported link key: >%s<", key)
			}
			linkVals = append(linkVals, val)
		}
		return linkVals, nil
	}

	for _, key := range []string{"href", "rel", "type", "hreflang", "title"} {
		if linkVals, err := getLinkVals(key); err != nil {
			return nil, fmt.Errorf("getLinkVals() failed: %v", err)
		} else {
			colVals = append(colVals, pq.StringArray(linkVals))
		}
	}

	// --- END non-string metadata ---------------------------

	// --- BEGIN string metadata ---------------------------

	// ### TODO: modify to use reflection instead of explicit field referencing

	colVals = append(colVals, []interface{}{
		tsMdata.GetVersion(),
		tsMdata.GetType(),
		tsMdata.GetTitle(),
		tsMdata.GetSummary(),
		tsMdata.GetKeywords(),
		tsMdata.GetKeywordsVocabulary(),
		tsMdata.GetLicense(),
		tsMdata.GetConventions(),
		tsMdata.GetNamingAuthority(),
		tsMdata.GetCreatorType(),
		tsMdata.GetCreatorName(),
		tsMdata.GetCreatorEmail(),
		tsMdata.GetCreatorUrl(),
		tsMdata.GetInstitution(),
		tsMdata.GetProject(),
		tsMdata.GetSource(),
		tsMdata.GetPlatform(),
		tsMdata.GetPlatformVocabulary(),
		tsMdata.GetStandardName(),
		tsMdata.GetUnit(),
		tsMdata.GetInstrument(),
		tsMdata.GetInstrumentVocabulary(),
	}...)

	// --- END string metadata ---------------------------

	return colVals, nil
}

// getTimeSeriesID retrieves the ID of the row in table time_series that matches tsMdata,
// inserting a new row if necessary. The ID is first looked up in a cache in order to save
// unnecessary database access.
// Returns (ID, nil) upon success, otherwise (..., error).
func getTimeSeriesID(
	db *sql.DB, tsMdata *datastore.TSMetadata, cache map[string]int64) (int64, error) {

	colVals, err := getTSColVals(tsMdata)
	if err != nil {
		return -1, fmt.Errorf("getTSColVals() failed: %v", err)
	}

	var id int64 = -1

	// first try a cache lookup
	cacheKey := fmt.Sprintf("%v", colVals)
	if id, found := cache[cacheKey]; found {
		return id, nil
	}

	// then access database ...

	cols := getTSMdataCols()

	formats := make([]string, len(colVals))
	for i := 0; i < len(colVals); i++ {
		formats[i] = "$%d"
	}

	// Get a Tx for making transaction requests.
	tx, err := db.Begin()
	if err != nil {
		return -1, fmt.Errorf("db.Begin() failed: %v", err)
	}
	// Defer a rollback in case anything fails.
	defer tx.Rollback()

	// NOTE: the 'WHERE false' is a feature that ensures that another transaction cannot
	// delete the row
	insertCmd := fmt.Sprintf(`
		INSERT INTO time_series (%s) VALUES (%s)
		ON CONFLICT ON CONSTRAINT unique_main DO UPDATE SET %s = EXCLUDED.%s WHERE false
		`,
		strings.Join(cols, ","),
		strings.Join(createPlaceholders(formats), ","),
		cols[0],
		cols[0],
	)
	fmt.Printf("insertCmd: %s; len(cols): %d; len(phs): %d\n",
		insertCmd, len(cols), len(createPlaceholders(formats)))

	_, err = tx.Exec(insertCmd, colVals...)
	if err != nil {
		return -1, fmt.Errorf("tx.Exec() failed: %v", err)
	}

	whereExpr := []string{}
	for i, col := range getTSMdataCols() {
		whereExpr = append(whereExpr, fmt.Sprintf("%s=$%d", col, i+1))
	}

	selectCmd := fmt.Sprintf(`SELECT id FROM time_series WHERE %s`, strings.Join(whereExpr, " AND "))
	err = tx.QueryRow(selectCmd, colVals...).Scan(&id)
	if err != nil {
		return -1, fmt.Errorf("tx.QueryRow() failed: %v", err)
	}

	// Commit the transaction.
	if err = tx.Commit(); err != nil {
		return -1, fmt.Errorf("tx.Commit() failed: %v", err)
	}

	// cache ID
	cache[cacheKey] = id

	return id, nil
}

// getObsTime extracts the obs time from obsMdata.
// Returns (obs time, nil) upon success, otherwise (..., error).
func getObsTime(obsMdata *datastore.ObsMetadata) (*timestamppb.Timestamp, error) {
	if obsTime := obsMdata.GetObstimeInstant(); obsTime != nil {
		return obsTime, nil
	}
	return nil, fmt.Errorf("obsMdata.GetObstimeInstant()is nil")
}

// --- BEGIN a variant of getObsTime that also supports intervals ---------------------------------
// getObsTime extracts the obs time from obsMdata as either an instant time or the end of
// an interval.
// Returns (obs time, nil) upon success, otherwise (..., error).
/*
func getObsTime(obsMdata *datastore.ObsMetadata) (*timestamppb.Timestamp, error) {
	if obsTime := obsMdata.GetInstant(); obsTime != nil {
		return obsTime, nil
	}
	if obsTime := obsMdata.GetInterval().GetEnd(); obsTime != nil {
		return obsTime, nil
	}
	return nil, fmt.Errorf("obsMdata.GetInstant() and obsMdata.GetInterval().GetEnd() are both nil")
}
*/
// --- END a variant of getObsTime that also supports intervals ---------------------------------

// getGeoPointID retrieves the ID of the row in table geo_point that matches point,
// inserting a new row if necessary. The ID is first looked up in a cache in order to save
// unnecessary database access.
// Returns (ID, nil) upon success, otherwise (..., error).
func getGeoPointID(db *sql.DB, point *datastore.Point, cache map[string]int64) (int64, error) {

	var id int64 = -1

	// first try a cache lookup
	cacheKey := fmt.Sprintf("%v %v", point.GetLon(), point.GetLat())
	if id, found := cache[cacheKey]; found {
		return id, nil
	}

	// Get a Tx for making transaction requests.
	tx, err := db.Begin()
	if err != nil {
		return -1, fmt.Errorf("db.Begin() failed: %v", err)
	}
	// Defer a rollback in case anything fails.
	defer tx.Rollback()

	// NOTE: the 'WHERE false' is a feature that ensures that another transaction cannot
	// delete the row
	insertCmd := fmt.Sprintf(`
		INSERT INTO geo_point (point) VALUES (ST_MakePoint($1, $2)::geography)
		ON CONFLICT (point) DO UPDATE SET point = EXCLUDED.point WHERE false`,
	)

	_, err = tx.Exec(insertCmd, point.GetLon(), point.GetLat())
	if err != nil {
		return -1, fmt.Errorf("tx.Exec() failed: %v", err)
	}

	selectCmd := fmt.Sprintf(`
		SELECT id FROM geo_point WHERE point = ST_MakePoint($1, $2)::geography
		`,
	)

	err = tx.QueryRow(selectCmd, point.GetLon(), point.GetLat()).Scan(&id)
	if err != nil {
		return -1, fmt.Errorf("tx.QueryRow() failed: %v", err)
	}

	// Commit the transaction.
	if err = tx.Commit(); err != nil {
		return -1, fmt.Errorf("tx.Commit() failed: %v", err)
	}

	// cache ID
	cache[cacheKey] = id

	return id, nil
}

// createInsertVals generates from (tsID, obsTimes, gpIDs, and omds) two arrays:
//   - in valsExpr: the list of arguments to the VALUES clause in the SQL INSERT
//     statement, and
//   - in phVals: the total, flat list of all placeholder values.
func createInsertVals(
	tsID int64, obsTimes *[]*timestamppb.Timestamp, gpIDs *[]int64,
	omds *[]*datastore.ObsMetadata, valsExpr *[]string, phVals *[]interface{}) {
	// assert(len(*obsTimes) > 0)
	// assert(len(*obsTimes) == len(*gpIDs) == len(*omds))

	index := 0
	for i := 0; i < len(*obsTimes); i++ {
		valsExpr0 := fmt.Sprintf(`(
			$%d,
			to_timestamp($%d),
			$%d,
			$%d,
			to_timestamp($%d),
			$%d,
			$%d,
			$%d,
			$%d,
			$%d
			)`,
			index+1,
			index+2,
			index+3,
			index+4,
			index+5,
			index+6,
			index+7,
			index+8,
			index+9,
			index+10,
		)

		phVals0 := []interface{}{
			tsID,
			common.Tstamp2float64Secs((*obsTimes)[i]),
			(*omds)[i].GetId(),
			(*gpIDs)[i],
			common.Tstamp2float64Secs((*omds)[i].GetPubtime()),
			(*omds)[i].GetDataId(),
			(*omds)[i].GetHistory(),
			(*omds)[i].GetMetadataId(),
			(*omds)[i].GetProcessingLevel(),
			(*omds)[i].GetValue(),
		}

		*valsExpr = append(*valsExpr, valsExpr0)
		*phVals = append(*phVals, phVals0...)
		index += len(phVals0)
	}
}

// upsertObsForTS inserts new observations and/or updates existing ones.
// Returns nil upon success, otherwise error.
func upsertObsForTS(
	db *sql.DB, tsID int64, obsTimes *[]*timestamppb.Timestamp, gpIDs *[]int64,
	omds *[]*datastore.ObsMetadata) error {

	// assert(obsTimes != nil)
	if obsTimes == nil {
		return fmt.Errorf("precondition failed: obsTimes == nil")
	}

	// assert(len(*obsTimes) > 0)
	if len(*obsTimes) == 0 {
		return fmt.Errorf("precondition failed: len(*obsTimes) == 0")
	}

	// assert(len(*obsTimes) == len(*gpIDs) == len(*omds))
	// for now don't check explicitly for this precondition

	valsExpr := []string{}
	phVals := []interface{}{}
	createInsertVals(tsID, obsTimes, gpIDs, omds, &valsExpr, &phVals)

	cmd := fmt.Sprintf(`
		INSERT INTO observation (
			ts_id,
			obstime_instant,
			id,
			geo_point_id,
			pubtime,
			data_id,
			history,
			metadata_id,
			processing_level,
			value)
		VALUES %s
		ON CONFLICT ON CONSTRAINT observation_pkey DO UPDATE SET
	    	id = EXCLUDED.id,
	 		geo_point_id = EXCLUDED.geo_point_id,
	 		pubtime = EXCLUDED.pubtime,
	 		data_id = EXCLUDED.data_id,
	 		history = EXCLUDED.history,
	 		metadata_id = EXCLUDED.metadata_id,
	 		processing_level = EXCLUDED.processing_level,
	 		value = EXCLUDED.value
	`, strings.Join(valsExpr, ","))

	_, err := db.Exec(cmd, phVals...)
	if err != nil {
		return fmt.Errorf("db.Exec() failed: %v", err)
	}

	return nil
}

// PutObservations ... (see documentation in StorageBackend interface)
func (sbe *PostgreSQL) PutObservations(request *datastore.PutObsRequest) error {

	type tsInfo struct {
		obsTimes *[]*timestamppb.Timestamp
		gpIDs    *[]int64 // geo point IDs
		omds     *[]*datastore.ObsMetadata
	}

	tsInfos := map[int64]tsInfo{}

	tsIDCache := map[string]int64{}
	gpIDCache := map[string]int64{}

	loTime, hiTime := common.GetValidTimeRange()

	// reject call if # of observations exceeds limit
	if len(request.Observations) > putObsLimit {
		return fmt.Errorf(
			"too many observations in a single call: %d > %d",
			len(request.Observations), putObsLimit)
	}

	// populate tsInfos
	for _, obs := range request.Observations {

		obsTime, err := getObsTime(obs.GetObsMdata())
		if err != nil {
			return fmt.Errorf("getObsTime() failed: %v", err)
		}

		if obsTime.AsTime().Before(loTime) {
			return fmt.Errorf(
				"obs time too old: %v < %v (hiTime: %v; settings: %s)",
				obsTime.AsTime(), loTime, hiTime, common.GetValidTimeRangeSettings())
		}

		if obsTime.AsTime().After(hiTime) {
			return fmt.Errorf(
				"obs time too new: %v > %v (loTime: %v; settings: %s)",
				obsTime.AsTime(), hiTime, loTime, common.GetValidTimeRangeSettings())
		}

		tsID, err := getTimeSeriesID(sbe.Db, obs.GetTsMdata(), tsIDCache)
		if err != nil {
			return fmt.Errorf("getTimeSeriesID() failed: %v", err)
		}

		gpID, err := getGeoPointID(sbe.Db, obs.GetObsMdata().GetGeoPoint(), gpIDCache)
		if err != nil {
			return fmt.Errorf("getGeoPointID() failed: %v", err)
		}

		var obsTimes []*timestamppb.Timestamp
		var gpIDs []int64
		var omds []*datastore.ObsMetadata
		var tsInfo0 tsInfo
		var found bool
		if tsInfo0, found = tsInfos[tsID]; !found {
			obsTimes = []*timestamppb.Timestamp{}
			gpIDs = []int64{}
			omds = []*datastore.ObsMetadata{}
			tsInfos[tsID] = tsInfo{
				obsTimes: &obsTimes,
				gpIDs:    &gpIDs,
				omds:     &omds,
			}
			tsInfo0, found = tsInfos[tsID]
			// assert(found)
		}
		*tsInfo0.obsTimes = append(*tsInfo0.obsTimes, obsTime)
		*tsInfo0.gpIDs = append(*tsInfo0.gpIDs, gpID)
		*tsInfo0.omds = append(*tsInfo0.omds, obs.GetObsMdata())
	}

	// insert/update observations for each time series
	for tsID, tsInfo := range tsInfos {
		if err := upsertObsForTS(
			sbe.Db, tsID, tsInfo.obsTimes, tsInfo.gpIDs, tsInfo.omds); err != nil {
			return fmt.Errorf("upsertObsForTS()) failed: %v", err)
		}
	}

	if err := considerCleanup(sbe.Db); err != nil {
		log.Printf("WARNING: considerCleanup() failed: %v", err)
	}

	return nil
}
