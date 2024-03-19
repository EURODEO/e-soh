package postgresql

import (
	"database/sql"
	"datastore/common"
	"datastore/datastore"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/cridenour/go-postgis"
	"github.com/lib/pq"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// addStringMdata assigns the values of colVals to the corresponding struct fields in
// stringMdataGoNames (value i corresponds to field i ...). The struct is represented by rv.
// Returns nil upon success, otherwise error.
func addStringMdata(rv reflect.Value, stringMdataGoNames []string, colVals []interface{}) error {

	for i, goName := range stringMdataGoNames {
		var val string

		switch v := colVals[i].(type) {
		case string:
			val = v
		case sql.NullString:
			val = v.String
		case nil:
			val = ""
		default:
			return fmt.Errorf("colVals[%d] neither string, sql.NullString, nor nil: %v (type: %T)",
				i, colVals[i], colVals[i])
		}

		field := rv.Elem().FieldByName(goName)

		// NOTE: we assume the following assignment will never panic, hence we don't do
		// any pre-validation of field
		field.SetString(val)
	}

	return nil
}

// addWhereCondMatchAnyPattern appends to whereExpr an expression of the form
// "(cond1 OR cond2 OR ... OR condN)" where condi tests if the ith pattern in patterns matches
// colName. Matching is case-insensitive and an asterisk in a pattern matches zero or more
// arbitrary characters. The patterns with '*' replaced with '%' are appended to phVals.
func addWhereCondMatchAnyPattern(
	colName string, patterns []string, whereExpr *[]string, phVals *[]interface{}) {

	if (patterns == nil) || (len(patterns) == 0) {
		return
	}

	whereExprOR := []string{}

	index := len(*phVals)
	for _, ptn := range patterns {
		index++
		expr := fmt.Sprintf("(lower(%s) LIKE lower($%d))", colName, index)
		whereExprOR = append(whereExprOR, expr)
		*phVals = append(*phVals, strings.ReplaceAll(ptn, "*", "%"))
	}

	*whereExpr = append(*whereExpr, fmt.Sprintf("(%s)", strings.Join(whereExprOR, " OR ")))
}

// scanTSRow scans all columns from the current result row in rows and converts to a TSMetadata
// object.
// Returns (TSMetadata object, time series ID, nil) upon success, otherwise (..., ..., error).
func scanTSRow(rows *sql.Rows) (*datastore.TSMetadata, int64, error) {
	var (
		tsID         int64
		linkHref     pq.StringArray
		linkRel      pq.StringArray
		linkType     pq.StringArray
		linkHrefLang pq.StringArray
		linkTitle    pq.StringArray
	)

	// initialize colValPtrs with non-string metadata
	colValPtrs := []interface{}{
		&tsID,
		&linkHref,
		&linkRel,
		&linkType,
		&linkHrefLang,
		&linkTitle,
	}

	// complete colValPtrs with string metadata (handleable with reflection)
	colVals0 := make([]interface{}, len(tsStringMdataGoNames))
	for i := range tsStringMdataGoNames {
		colValPtrs = append(colValPtrs, &colVals0[i])
	}

	// scan row into column value pointers
	if err := rows.Scan(colValPtrs...); err != nil {
		return nil, -1, fmt.Errorf("rows.Scan() failed: %v", err)
	}

	// initialize tsMdata with non-string metadata
	links := []*datastore.Link{}
	for i := 0; i < len(linkHref); i++ {
		links = append(links, &datastore.Link{
			Href:     linkHref[i],
			Rel:      linkRel[i],
			Type:     linkType[i],
			Hreflang: linkHrefLang[i],
			Title:    linkTitle[i],
		})
	}
	tsMdata := datastore.TSMetadata{
		Links: links,
	}

	// complete tsMdata with string metadata (handleable with reflection)
	err := addStringMdata(reflect.ValueOf(&tsMdata), tsStringMdataGoNames, colVals0)
	if err != nil {
		return nil, -1, fmt.Errorf("addStringMdata() failed: %v", err)
	}

	return &tsMdata, tsID, nil
}

// includeField returns true iff ((incFields is empty) or (incFields contains field)).
func includeField(incFields common.StringSet, field string) bool {
	return (len(incFields) == 0) || incFields.Contains(field)
}

// getTSMetadata retrieves into tsMdatas metadata of time series in table time_series that match
// tsIDs. The keys of tsMdatas are the time series IDs. Fields to include (as non-NULL values) in
// the final result are defined in incFields.
//
// Returns nil upon success, otherwise error
func getTSMetadata(
	db *sql.DB, tsIDs []string, tsMdatas map[int64]*datastore.TSMetadata,
	incFields common.StringSet) error {

	tsColNames := getTSColNames()

	convTSCN := []string{}
	for _, col := range tsColNames {
		aliases := []string{}
		if strings.HasPrefix(col, "link_") {
			aliases = []string{"links"} // NOTE: 'links' is a "group" field, i.e. collectively
			// representing all columns starting with 'link_'
		}
		convTSCN = append(convTSCN, convertSelectCol(incFields, col, "", aliases...))
	}

	query := fmt.Sprintf(
		`SELECT id, %s FROM time_series WHERE %s`,
		strings.Join(convTSCN, ","),
		createSetFilter("id", tsIDs),
	)

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("db.Query() failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		tsMdata, tsID, err := scanTSRow(rows)
		if err != nil {
			return fmt.Errorf("scanTSRow() failed: %v", err)
		}

		tsMdatas[tsID] = tsMdata
	}

	return nil
}

// getTimeFilter derives from tspec the expression used in a WHERE clause for overall
// (i.e. not time series specific) filtering on obs time.
//
// Returns expression.
func getTimeFilter(tspec common.TemporalSpec) string {

	timeExpr := "TRUE" // by default, don't filter on obs time at all

	if tspec.IntervalMode {
		ti := tspec.Interval
		if ti != nil {
			timeExprs := []string{}
			if start := ti.GetStart(); start != nil {
				timeExprs = append(timeExprs, fmt.Sprintf(
					"obstime_instant >= to_timestamp(%f)", common.Tstamp2float64Secs(start)))
			}
			if end := ti.GetEnd(); end != nil {
				timeExprs = append(timeExprs, fmt.Sprintf(
					"obstime_instant < to_timestamp(%f)", common.Tstamp2float64Secs(end)))
			}
			if len(timeExprs) > 0 {
				timeExpr = fmt.Sprintf("(%s)", strings.Join(timeExprs, " AND "))
			}
		}
	}

	// restrict to current valid time range
	loTime, hiTime := common.GetValidTimeRange()
	timeExpr += fmt.Sprintf(" AND (obstime_instant >= to_timestamp(%d))", loTime.Unix())
	timeExpr += fmt.Sprintf(" AND (obstime_instant <= to_timestamp(%d))", hiTime.Unix())

	return timeExpr
}

type stringFilterInfo struct {
	colName  string
	patterns []string
}

// TODO: add filter info for non-string types

// getMdataFilter derives from stringFilterInfos the expression used in a WHERE clause for
// "match any" filtering on a set of attributes.
//
// The expression will be of the form
//
//	(
//	  ((<attr1 matches pattern1,1>) OR (<attr1 matches pattern1,2>) OR ...) AND
//	  ((<attr2 matches pattern2,1>) OR (<attr1 matches pattern2,2>) OR ...) AND
//	  ...
//	)
//
// Values to be used for query placeholders are appended to phVals.
//
// Returns expression.
func getMdataFilter(stringFilterInfos []stringFilterInfo, phVals *[]interface{}) string {

	whereExprAND := []string{}

	for _, sfi := range stringFilterInfos {
		addWhereCondMatchAnyPattern(
			sfi.colName, sfi.patterns, &whereExprAND, phVals)
	}

	whereExpr := "TRUE" // by default, don't filter
	if len(whereExprAND) > 0 {
		whereExpr = fmt.Sprintf("(%s)", strings.Join(whereExprAND, " AND "))
	}

	return whereExpr
}

// getGeoFilter derives from 'inside' the expression used in a WHERE clause for keeping
// observations inside this polygon.
//
// Values to be used for query placeholders are appended to phVals.
//
// Returns expression.
func getGeoFilter(inside *datastore.Polygon, phVals *[]interface{}) (string, error) {
	whereExpr := "TRUE" // by default, don't filter
	if inside != nil {  // get all points
		points := inside.Points

		equal := func(p1, p2 *datastore.Point) bool {
			return (p1.Lat == p2.Lat) && (p1.Lon == p2.Lon)
		}

		if (len(points) > 0) && !equal(points[0], points[len(points)-1]) {
			points = append(points, points[0]) // close polygon
		}

		if len(points) < 4 {
			return "", fmt.Errorf("polygon contains too few points")
		}

		// construct the polygon ring of the WKT representation
		// (see https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry;
		// note that only a single ring is supported for now)
		polygonRing := []string{}
		for _, point := range points {
			polygonRing = append(polygonRing, fmt.Sprintf("%f %f", point.Lon, point.Lat))
		}

		srid := "4326" // spatial reference system ID

		index := len(*phVals) + 1
		whereExpr = fmt.Sprintf(
			"ST_DWITHIN(point, ST_GeomFromText($%d, %s)::geography, 0.0)", index, srid)
		*phVals = append(*phVals, fmt.Sprintf("polygon((%s))", strings.Join(polygonRing, ",")))
	}

	return whereExpr, nil
}

// getTableNameFromField gets the database table name associated with fieldName.
//
// Returns (table name, nil) upon success, otherwise (..., error).
func getTableNameFromField(fieldName string) (string, error) {

	tableName, found := pb2table[fieldName]
	if !found {
		return "", fmt.Errorf(
			"no such field: %s; available fields: %s", fieldName, strings.Join(pb2tableKeys, ", "))
	}

	return tableName, nil
}

// getStringMdataFilter creates from 'request' the string metadata filter used for querying
// observations.
//
// Values to be used for query placeholders are appended to phVals.
//
// Returns upon success (string metadata filter used in a 'WHERE ... AND ...' clause (possibly
// just 'TRUE'), nil), otherwise (..., error).
func getStringMdataFilter(
	request *datastore.GetObsRequest, phVals *[]interface{}) (string, error) {

	stringFilterInfos := []stringFilterInfo{}

	for fieldName, ptnObj := range request.GetFilter() {
		tableName, err := getTableNameFromField(fieldName)
		if err != nil {
			return "", fmt.Errorf("getTableNameFromField() failed: %v", err)
		}
		patterns := ptnObj.GetValues()
		if len(patterns) > 0 {
			stringFilterInfos = append(stringFilterInfos, stringFilterInfo{
				colName:  fmt.Sprintf("%s.%s", tableName, fieldName),
				patterns: patterns,
			})
		}
	}

	return getMdataFilter(stringFilterInfos, phVals), nil
}

// createObsQueryVals creates from request and tspec values used for querying observations.
//
// Values to be used for query placeholders are appended to phVals.
//
// Upon success the function returns five values:
// - distinct spec, possibly just an empty string
// - time filter used in a 'WHERE ... AND ...' clause (possibly just 'TRUE')
// - geo filter ... ditto
// - string metadata ... ditto
// - nil,
// otherwise (..., ..., ..., error).
func createObsQueryVals(
	request *datastore.GetObsRequest, tspec common.TemporalSpec, phVals *[]interface{}) (
	string, string, string, string, error) {

	distinctSpec := ""
	if !tspec.IntervalMode {
		// 'latest' mode, so ensure that we select only one observation per time series
		// (which will be the most recent one thanks to '... ORDER BY ts_id, obstime_instant DESC')
		distinctSpec = "DISTINCT ON (ts_id)"
	}

	timeFilter := getTimeFilter(tspec)

	geoFilter, err := getGeoFilter(request.GetSpatialArea(), phVals)
	if err != nil {
		return "", "", "", "", fmt.Errorf("getGeoFilter() failed: %v", err)
	}

	stringMdataFilter, err := getStringMdataFilter(request, phVals)
	if err != nil {
		return "", "", "", "", fmt.Errorf("getStringMdataFilter() failed: %v", err)
	}

	return distinctSpec, timeFilter, geoFilter, stringMdataFilter, nil
}

// scanObsRow scans all columns from the current result row in rows and converts to an ObsMetadata
// object.
// Returns (ObsMetadata object, time series ID, nil) upon success, otherwise (..., ..., error).
func scanObsRow(rows *sql.Rows) (*datastore.ObsMetadata, int64, error) {
	var (
		tsID            int64
		obsTimeInstant0 time.Time
		pubTime0        sql.NullTime
		value           sql.NullString
		point           postgis.PointS
	)

	// initialize colValPtrs with non-string metadata
	colValPtrs := []interface{}{
		&tsID,
		&obsTimeInstant0,
		&pubTime0,
		&value,
		&point,
	}

	// complete colValPtrs with string metadata (handleable with reflection)
	colVals0 := make([]interface{}, len(obsStringMdataGoNames))
	for i := range obsStringMdataGoNames {
		colValPtrs = append(colValPtrs, &colVals0[i])
	}

	// scan row into column value pointers
	if err := rows.Scan(colValPtrs...); err != nil {
		return nil, -1, fmt.Errorf("rows.Scan() failed: %v", err)
	}

	// initialize obsMdata with non-string metadata and obs value
	obsMdata := datastore.ObsMetadata{
		Geometry: &datastore.ObsMetadata_GeoPoint{
			GeoPoint: &datastore.Point{
				Lon: point.X,
				Lat: point.Y,
			},
		},
		Obstime: &datastore.ObsMetadata_ObstimeInstant{
			ObstimeInstant: timestamppb.New(obsTimeInstant0),
		},
		Value: value.String,
	}
	if pubTime0.Valid {
		obsMdata.Pubtime = timestamppb.New(pubTime0.Time)
	}

	// complete obsMdata with string metadata (handleable with reflection)
	err := addStringMdata(reflect.ValueOf(&obsMdata), obsStringMdataGoNames, colVals0)
	if err != nil {
		return nil, -1, fmt.Errorf("addStringMdata() failed: %v", err)
	}

	return &obsMdata, tsID, nil
}

// getIncRespFields extracts the set of included response fields from request.
//
// Returns (set of included response fields, nil) upon success, otherwise (..., error)
func getIncRespFields(request *datastore.GetObsRequest) (common.StringSet, error) {

	fields := common.StringSet{}
	for _, field := range request.GetIncludedResponseFields() {
		if !supIncRespFields.Contains(field) {
			return nil, fmt.Errorf(
				"'%s' not among supported fields: %s",
				field, supIncRespFieldsCSV)
		}
		fields[field] = struct{}{}
	}

	return fields, nil
}

// convertSelectCol decides what to extract from a column in a SELECT statement.
//
// Returns '<colName>' if colName (or one of its aliases) is defined - with prefix removed -
// in incFields,
// otherwise 'NULL' to indicate that we're not interested in the column value.
func convertSelectCol(
	incFields common.StringSet, colName, prefix string, aliases ...string) string {

	aliases = append(aliases, colName)
	for _, alias := range aliases {
		if includeField(incFields, strings.TrimPrefix(alias, prefix)) {
			return colName // colName included (either directly or through an alias)
		}
	}

	return "NULL" // colName not included
}

// getObs gets into obs all observations that match request and tspec. Fields to include
// (as non-NULL values) in the final result are defined in incFields.
//
// Returns nil upon success, otherwise error.
func getObs(
	db *sql.DB, request *datastore.GetObsRequest, tspec common.TemporalSpec,
	obs *[]*datastore.Metadata2, incFields common.StringSet) error {

	// get values needed for query
	phVals := []interface{}{} // placeholder values
	distinctSpec, timeFilter, geoFilter, stringMdataFilter, err := createObsQueryVals(
		request, tspec, &phVals)
	if err != nil {
		return fmt.Errorf("createQueryVals() failed: %v", err)
	}

	// convert obsStringMdataCols according to incFields
	convOSMC := []string{}
	for _, col := range obsStringMdataCols {
		convOSMC = append(convOSMC, convertSelectCol(incFields, col, "observation."))
	}

	// define and execute query
	query := fmt.Sprintf(`
		SELECT %s
		    ts_id,
			obstime_instant,
			%s,
			%s,
			point,
			%s
		FROM observation
		JOIN time_series on time_series.id = observation.ts_id
		JOIN geo_point ON observation.geo_point_id = geo_point.id
		WHERE %s AND %s AND %s
		ORDER BY ts_id, obstime_instant DESC
		`,
		distinctSpec,
		convertSelectCol(incFields, "pubtime", "observation."),
		convertSelectCol(incFields, "value", "observation."),
		strings.Join(convOSMC, ","),
		timeFilter,
		geoFilter,
		stringMdataFilter)

	rows, err := db.Query(query, phVals...)
	if err != nil {
		return fmt.Errorf("db.Query() failed: %v", err)
	}
	defer rows.Close()

	obsMdatas := make(map[int64][]*datastore.ObsMetadata) // observations per time series ID

	// scan rows
	for rows.Next() {
		obsMdata, tsID, err := scanObsRow(rows)
		if err != nil {
			return fmt.Errorf("scanObsRow() failed: %v", err)
		}

		// Handle obstime_instant as a special case instead of using convertSelectCol. Its value is
		// always loaded in the above SQL call since it is referred to in the 'ORDER BY' clause.
		if !includeField(incFields, "obstime_instant") {
			obsMdata.Obstime = nil
		}

		// Handle geo_point as a special case instead of using convertSelectCol. Its value is always
		// loaded in the above SQL call, since the postgis package does not support a nullable type
		// (such as postgis.NullPointS in addition to postgis.PointS). A NULL value (generated by
		// convertSelectCol) would have caused rows.Scan() to fail in scanObsRow().
		if !includeField(incFields, "geo_point") {
			obsMdata.Geometry = nil
		}

		// prepend obs to time series to get chronological order
		obsMdatas[tsID] = append([]*datastore.ObsMetadata{obsMdata}, obsMdatas[tsID]...)
	}

	// get time series
	tsMdatas := map[int64]*datastore.TSMetadata{}
	tsIDs := []string{}
	for tsID := range obsMdatas {
		tsIDs = append(tsIDs, fmt.Sprintf("%d", tsID))
	}
	if err = getTSMetadata(db, tsIDs, tsMdatas, incFields); err != nil {
		return fmt.Errorf("getTSMetadata() failed: %v", err)
	}

	// assemble final output
	for tsID, obsMdata := range obsMdatas {
		*obs = append(*obs, &datastore.Metadata2{
			TsMdata:  tsMdatas[tsID],
			ObsMdata: obsMdata,
		})
	}

	return nil
}

// GetObservations ... (see documentation in StorageBackend interface)
func (sbe *PostgreSQL) GetObservations(
	request *datastore.GetObsRequest, tspec common.TemporalSpec) (
	*datastore.GetObsResponse, error) {

	var err error

	incFields, err := getIncRespFields(request)
	if err != nil {
		return nil, fmt.Errorf("getIncRespFields() failed: %v", err)
	}

	obs := []*datastore.Metadata2{}
	if err = getObs(
		sbe.Db, request, tspec, &obs, incFields); err != nil {
		return nil, fmt.Errorf("getObs() failed: %v", err)
	}

	return &datastore.GetObsResponse{Observations: obs}, nil
}
