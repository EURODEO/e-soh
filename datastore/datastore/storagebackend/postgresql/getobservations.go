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
	_ "github.com/lib/pq"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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

// getTSMetadata retrieves into tsMdata metadata of time series in table time_series that match
// tsIDs. The keys of tsMdata are the time series IDs.
// Returns nil upon success, otherwise error
func getTSMetadata(db *sql.DB, tsIDs []string, tsMdata map[int64]*datastore.TSMetadata) error {

	query := fmt.Sprintf(
		`SELECT id, %s FROM time_series WHERE %s`,
		strings.Join(getTSMdataCols(), ","),
		createSetFilter("id", tsIDs),
	)

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("db.Query() failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tsID int64
		var tsMdata0 datastore.TSMetadata

		linkHref := pq.StringArray{}
		linkRel := pq.StringArray{}
		linkType := pq.StringArray{}
		linkHrefLang := pq.StringArray{}
		linkTitle := pq.StringArray{}

		if err := rows.Scan(
			&tsID,
			&tsMdata0.Version,
			&tsMdata0.Type,
			&tsMdata0.Title,
			&tsMdata0.Summary,
			&tsMdata0.Keywords,
			&tsMdata0.KeywordsVocabulary,
			&tsMdata0.License,
			&tsMdata0.Conventions,
			&tsMdata0.NamingAuthority,
			&tsMdata0.CreatorType,
			&tsMdata0.CreatorName,
			&tsMdata0.CreatorEmail,
			&tsMdata0.CreatorUrl,
			&tsMdata0.Institution,
			&tsMdata0.Project,
			&tsMdata0.Source,
			&tsMdata0.Platform,
			&tsMdata0.PlatformVocabulary,
			&tsMdata0.StandardName,
			&tsMdata0.Unit,
			&tsMdata0.Instrument,
			&tsMdata0.InstrumentVocabulary,
			&linkHref,
			&linkRel,
			&linkType,
			&linkHrefLang,
			&linkTitle,
		); err != nil {
			return fmt.Errorf("rows.Scan() failed: %v", err)
		}

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
		tsMdata0.Links = links

		tsMdata[tsID] = &tsMdata0
	}

	return nil
}

// getTimeFilter derives from ti the expression used in a WHERE clause for filtering on obs time.
// Returns expression.
func getTimeFilter(ti *datastore.TimeInterval) string {
	timeExpr := "TRUE" // by default, don't filter on obs time at all

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
// TODO: add filter infos for other types than string

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

type stringFieldInfo struct {
	field reflect.StructField
	method reflect.Value
	methodName string
}

// getObs gets into obs all observations that match request.
// Returns nil upon success, otherwise error.
func getObs(db *sql.DB, request *datastore.GetObsRequest, obs *[]*datastore.Metadata2) error {

	phVals := []interface{}{} // placeholder values

	// --- BEGIN get temporal and spatial search expressions ----------------

	timeExpr := getTimeFilter(request.GetInterval())

	geoExpr, err := getGeoFilter(request.Inside, &phVals)
	if err != nil {
		return fmt.Errorf("getGeoFilter() failed: %v", err)
	}

	// --- END get temporal and spatial search expressions ----------------

	// --- BEGIN get search expression for string attributes ----------------

	rv := reflect.ValueOf(request)

	stringFilterInfos := []stringFilterInfo{}

	stringFieldInfos := []stringFieldInfo{}

	addStringFields := func(s interface{}) {
		for _, field := range reflect.VisibleFields(reflect.TypeOf(s)) {
			mtdName := fmt.Sprintf("Get%s", field.Name)
			mtd := rv.MethodByName(mtdName)
			if field.IsExported() && (field.Type.Kind() == reflect.String) && (mtd.IsValid()) {
				stringFieldInfos = append(stringFieldInfos, stringFieldInfo{
					field: field,
					method: mtd,
					methodName: mtdName,
				})
			}
		}
	}
	addStringFields(datastore.TSMetadata{})
	addStringFields(datastore.ObsMetadata{})

	for _, sfInfo := range stringFieldInfos {
		patterns, ok := sfInfo.method.Call([]reflect.Value{})[0].Interface().([]string)
		if !ok {
			return fmt.Errorf(
				"sfInfo.method.Call() failed for method %s; failed to return []string",
				sfInfo.methodName)
		}
		if len(patterns) > 0 {
			stringFilterInfos = append(stringFilterInfos, stringFilterInfo{
				colName: common.ToSnakeCase(sfInfo.field.Name),
				patterns: patterns,
			})
		}
	}

	mdataExpr := getMdataFilter(stringFilterInfos, &phVals)

	// --- END get search expression for string attributes ----------------

	query := fmt.Sprintf(`
		SELECT ts_id, observation.id, geo_point_id, pubtime, data_id, history, metadata_id,
			obstime_instant, processing_level, value, point
		FROM observation
		    JOIN geo_point gp ON observation.geo_point_id = gp.id
			JOIN time_series ts on ts.id = observation.ts_id
		WHERE %s AND %s AND %s
		ORDER BY ts_id, obstime_instant
	`, timeExpr, mdataExpr, geoExpr)

	rows, err := db.Query(query, phVals...)
	if err != nil {
		return fmt.Errorf("db.Query() failed: %v", err)
	}
	defer rows.Close()

	obsMap := make(map[int64][]*datastore.ObsMetadata)
	for rows.Next() {
		var (
			tsID            int64
			id              string
			gpID            int64
			pubTime0        time.Time
			dataID          string
			history         string
			metadataID      string
			obsTimeInstant0 time.Time
			processingLevel string
			value           string
			point           postgis.PointS
		)
		if err := rows.Scan(&tsID, &id, &gpID, &pubTime0, &dataID, &history, &metadataID,
			&obsTimeInstant0, &processingLevel, &value, &point); err != nil {
			return fmt.Errorf("rows.Scan() failed: %v", err)
		}

		obsMdata := &datastore.ObsMetadata{
			Id: id,
			Geometry: &datastore.ObsMetadata_GeoPoint{
				GeoPoint: &datastore.Point{
					Lon: point.X,
					Lat: point.Y},
			},
			Pubtime:    timestamppb.New(pubTime0),
			DataId:     dataID,
			History:    history,
			MetadataId: metadataID,
			Obstime: &datastore.ObsMetadata_ObstimeInstant{
				ObstimeInstant: timestamppb.New(obsTimeInstant0),
			},
			ProcessingLevel: processingLevel,
			Value:           value,
		}
		obsMap[tsID] = append(obsMap[tsID], obsMdata)
	}

	// get time series
	tsMdata := map[int64]*datastore.TSMetadata{}
	tsIDs := []string{}
	for id := range obsMap {
		tsIDs = append(tsIDs, fmt.Sprintf("%d", id))
	}
	if err = getTSMetadata(db, tsIDs, tsMdata); err != nil {
		return fmt.Errorf("getTSMetadata() failed: %v", err)
	}

	for tsID, obsMdata := range obsMap {
		*obs = append(*obs, &datastore.Metadata2{
			TsMdata:  tsMdata[tsID],
			ObsMdata: obsMdata,
		})
	}

	return nil
}

// GetObservations ... (see documentation in StorageBackend interface)
func (sbe *PostgreSQL) GetObservations(request *datastore.GetObsRequest) (
	*datastore.GetObsResponse, error) {

	var err error

	obs := []*datastore.Metadata2{}
	if err = getObs(
		sbe.Db, request, &obs); err != nil {
		return nil, fmt.Errorf("getObs() failed: %v", err)
	}

	return &datastore.GetObsResponse{Observations: obs}, nil
}
