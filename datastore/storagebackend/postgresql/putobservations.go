package postgresql

import (
	"database/sql"
	"datastore/common"
	"datastore/datastore"
	"fmt"
	"strings"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// getTSColVals gets the time series metadata column values from tsMdata.
// Returns (column values, nil) upon success, otherwise (..., error).
func getTSColVals (tsMdata *datastore.TSMetadata) ([]interface{}, error) {

	colVals := []interface{}{}

	// main section

	colVals = []interface{}{
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
	}

	// links section

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

	for _, key := range []string{"href", "rel", "type", "hreflang", "title"}  {
		if linkVals, err := getLinkVals(key); err != nil {
			return nil, fmt.Errorf("getLinkVals() failed: %v", err)
		} else {
			colVals = append(colVals, pq.StringArray(linkVals))
		}
	}

	return colVals, nil
}

// getTimeSeriesID retrieves the ID of the row in table time_series that matches tsMdata,
// inserting a new row (with latest_obs_time = obsTime) if necessary.
// Returns (ID, nil) upon success, otherwise (..., error).
func getTimeSeriesID(
	db *sql.DB, obsTime *timestamppb.Timestamp, tsMdata *datastore.TSMetadata) (int64, error) {

	colVals, err := getTSColVals(tsMdata)
	if err != nil {
		return -1, fmt.Errorf("getTSColVals() failed: %v", err)
	}

	whereExpr := []string{}
	for i, col := range getTSMdataCols() {
		whereExpr = append(whereExpr, fmt.Sprintf("%s=$%d", col, i + 1))
	}

	query := fmt.Sprintf(`SELECT id FROM time_series WHERE %s`, strings.Join(whereExpr, " AND "))
	rows, err := db.Query(query, colVals...)
	if err != nil {
		fmt.Printf("query: %+v\ncolVals: %+v\n", query, colVals)
		return -1, fmt.Errorf("db.Query() failed: %v", err)
	}
	defer rows.Close()

	var id int64 = -1
	nrows := 0
	for rows.Next() {
		nrows++
		if nrows > 1 { // ensure at most one matching row
			return -1, fmt.Errorf("more than one matching row")
		}
		// get ID from existing row
		if err := rows.Scan(&id); err != nil {
			return -1, fmt.Errorf("rows.Scan() failed: %v", err)
		}
	}
	if nrows == 0 {
		formats := []string{}
		for _ = range colVals {
			formats = append(formats, "$%d")
		} // TODO: check if we can initialize array with the same value without looping

		// get ID from new row
		cols := getTSMdataCols()
		cols = append(cols, "latest_obs_time")
		colVals = append(colVals, common.Tstamp2float64Secs(obsTime))
		formats = append(formats, "to_timestamp($%d)")

		cmd := fmt.Sprintf(
			`INSERT INTO time_series (%s) VALUES (%s) RETURNING id`,
			strings.Join(cols, ","),
			strings.Join(createPlaceholders(formats), ","))
		err := db.QueryRow(cmd, colVals...).Scan(&id)
		if err != nil {
			return -1, fmt.Errorf("db.QueryRow() failed: %v", err)
		}
	}

	return id, nil;
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
// inserting a new row (with latest_obs_time = obsTime) if necessary.
// Returns (ID, nil) upon success, otherwise (..., error).
func getGeoPointID(
	db *sql.DB, obsTime *timestamppb.Timestamp, point *datastore.Point) (int64, error) {

	query := fmt.Sprintf(`SELECT id FROM geo_point WHERE point=ST_MakePoint($1, $2)::geography`)
	rows, err := db.Query(query, point.GetLon(), point.GetLat())
	if err != nil {
		fmt.Printf("\nquery: >%s< (lon: %v, lat: %v)\n\n", query, point.GetLon(), point.GetLat())
		return -1, fmt.Errorf("db.Query() failed: %v", err)
	}
	defer rows.Close()

	var id int64 = -1
	nrows := 0
	for rows.Next() {
		nrows++
		if nrows > 1 { // ensure at most one matching row
			return -1, fmt.Errorf("more than one matching row")
		}
		// get ID from existing row
		if err := rows.Scan(&id); err != nil {
			return -1, fmt.Errorf("rows.Scan() failed: %v", err)
		}
	}
	if nrows == 0 {
		// get ID from new row
		cmd := fmt.Sprintf(`
			INSERT INTO geo_point (point, latest_obs_time)
			VALUES (ST_MakePoint($1, $2)::geography, to_timestamp($3)) RETURNING id
		`)
		err := db.QueryRow(
			cmd, point.GetLon(), point.GetLat(), common.Tstamp2float64Secs(obsTime)).Scan(&id)
		if err != nil {
			return -1, fmt.Errorf("db.QueryRow() failed: %v", err)
		}
	}

	return id, nil;
}

// updateLatestObsTime updates the latest_obs_time column in tables time_series and geo_point_id
// with obsTime. This value is then used to identify obsolete rows in those tables.
// Returns nil upon success, otherwise error.
func updateLatestObsTime(
	db *sql.DB, tsID, geoPointID int64, obsTime *timestamppb.Timestamp) error {

	obsTime0 := common.Tstamp2float64Secs(obsTime)

	updateInTable := func(table string, id int64) error {
		cmd := fmt.Sprintf(
			"UPDATE %s SET latest_obs_time=to_timestamp(%f) WHERE id=%d", table, obsTime0, id)
		if _, err := db.Exec(cmd); err != nil {
			return fmt.Errorf("db.Exec() failed: %v", err)
		}
		return nil
	}

	for _, info := range []struct{table string; id int64}{
		{"time_series", tsID},
		{"geo_point", geoPointID},
		} {
			if err := updateInTable(info.table, info.id); err != nil {
				return fmt.Errorf("updateInTable(%s) failed: %v", info.table, err)
			}
	}

	return nil
}

// insertOrUpdateObs inserts a new row or updates an existing row in table observation.
// Returns nil upon success, otherwise error.
func insertOrUpdateObs(
	db *sql.DB, tsID, geoPointID int64, obsTime *timestamppb.Timestamp,
	obsMdata *datastore.ObsMetadata) error {

	obsTime0 := common.Tstamp2float64Secs(obsTime)
	id := obsMdata.GetId()
	pubTime := common.Tstamp2float64Secs(obsMdata.GetPubtime())
	dataID := obsMdata.GetDataId()
	history := obsMdata.GetHistory()
	metadataID := obsMdata.GetMetadataId()
	processingLevel := obsMdata.GetProcessingLevel()
	value := obsMdata.GetValue()

	cmd := fmt.Sprintf(
		`INSERT INTO observation (
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
		 VALUES ($1, to_timestamp($2), $3, $4, to_timestamp($5), $6, $7, $8, $9, $10)
		 ON CONFLICT (ts_id, obstime_instant) DO UPDATE SET
		    id=EXCLUDED.id,
		 	geo_point_id=EXCLUDED.geo_point_id,
		 	pubtime=EXCLUDED.pubtime,
		 	data_id=EXCLUDED.data_id,
		 	history=EXCLUDED.history,
		 	metadata_id=EXCLUDED.metadata_id,
		 	processing_level=EXCLUDED.processing_level,
		 	value=EXCLUDED.value
	`) // NOTE that ts_id and obstime_instant form the PK and do not need to be updated

	if _, err := db.Exec(
		cmd, tsID, obsTime0,
		id, geoPointID, pubTime, dataID, history, metadataID, processingLevel, value,
	); err != nil {
		return fmt.Errorf("db.Exec() failed: %v", err)
	}

	return nil
}

// putObs inserts a new observation or updates an existing one.
// Returns nil upon success, otherwise error.
func putObs(db *sql.DB, obs *datastore.Metadata1) error {

	obsTime, err := getObsTime(obs.GetObsMdata())
	if err != nil {
		return fmt.Errorf("getObsTime() failed: %v", err)
	}

	tsID, err := getTimeSeriesID(db, obsTime, obs.GetTsMdata())
	if err != nil {
		return fmt.Errorf("getTimeSeriesID() failed: %v", err)
	}

	geoPointID, err := getGeoPointID(db, obsTime, obs.GetObsMdata().GetGeoPoint())
	if err != nil {
		return fmt.Errorf("getGeoPointID() failed: %v", err)
	}

	err = updateLatestObsTime(db, tsID, geoPointID, obsTime)
	if err != nil {
		return fmt.Errorf("updateLatestObsTime() failed: %v", err)
	}

	err = insertOrUpdateObs(db, tsID, geoPointID, obsTime, obs.GetObsMdata())
	if err != nil {
		return fmt.Errorf("insertOrUpdateObs() failed: %v", err)
	}

	return nil
}

// PutObservations ... (see documentation in StorageBackend interface)
func (sbe *PostgreSQL) PutObservations(request *datastore.PutObsRequest) error {

	for _, obs := range request.Observations {
		if err := putObs(sbe.Db, obs); err != nil {
			return fmt.Errorf("putObs() failed: %v", err)
		}
	}

	return nil
}
