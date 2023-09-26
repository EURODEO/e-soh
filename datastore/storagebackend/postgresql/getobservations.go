package postgresql

import (
	"database/sql"
	"datastore/common"
	"datastore/datastore"
	"fmt"
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

// getTimeSeries retrieves into timeSeries all time series in table time_series that match requested
// metadata filters. If no metadata filters are specified, all available time series are retrieved.
// Returns nil upon success, otherwise error.
func getTimeSeries(
	db *sql.DB, request *datastore.GetObsRequest,
	timeSeries map[int64]*datastore.TSMetadata) error {

	phVals := []interface{}{} // placeholder values
	whereExpr := getMdataFilter(request, []filterInfo{
		{"platform", request.GetPlatforms()},
		{"standard_name", request.GetStandardNames()},
		{"instrument", request.GetInstruments()},
		// TODO: add search filters for more time_series columns
	}, &phVals)

	query := fmt.Sprintf(
		`SELECT id, %s FROM time_series WHERE %s`, strings.Join(getTSMdataCols(), ","), whereExpr)

	rows, err := db.Query(query, phVals...)
	if err != nil {
		fmt.Printf("query: %v\n", query)
		fmt.Printf("phVals: %v\n", phVals)
		return fmt.Errorf("db.Query() failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tsID int64
		var tsMdata datastore.TSMetadata

		linkHref := pq.StringArray{}
		linkRel := pq.StringArray{}
		linkType := pq.StringArray{}
		linkHrefLang := pq.StringArray{}
		linkTitle := pq.StringArray{}

		if err := rows.Scan(
			&tsID,
			&tsMdata.Version,
			&tsMdata.Type,
			&tsMdata.Title,
			&tsMdata.Summary,
			&tsMdata.Keywords,
			&tsMdata.KeywordsVocabulary,
			&tsMdata.License,
			&tsMdata.Conventions,
			&tsMdata.NamingAuthority,
			&tsMdata.CreatorType,
			&tsMdata.CreatorName,
			&tsMdata.CreatorEmail,
			&tsMdata.CreatorUrl,
			&tsMdata.Institution,
			&tsMdata.Project,
			&tsMdata.Source,
			&tsMdata.Platform,
			&tsMdata.PlatformVocabulary,
			&tsMdata.StandardName,
			&tsMdata.Unit,
			&tsMdata.Instrument,
			&tsMdata.InstrumentVocabulary,
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
				Href: linkHref[i],
				Rel: linkRel[i],
				Type: linkType[i],
				Hreflang: linkHrefLang[i],
				Title: linkTitle[i],
			})
		}
		tsMdata.Links = links

		timeSeries[tsID] = &tsMdata
	}

	return nil
}

// getGeoPoints retrieves into geoPoints all points in table geo_point inside polygon.
// Returns nil upon success, otherwise error.
func getGeoPoints(
	db *sql.DB, polygon *datastore.Polygon, geoPoints map[int64]*datastore.Point) error {

	if polygon == nil {
		return fmt.Errorf("polygon == nil")
	}

	points := polygon.Points

	equal := func(p1, p2 *datastore.Point) bool {
		return (p1.Lat == p2.Lat) && (p1.Lon == p2.Lon)
	}

	if (len(points) > 0) && !equal(points[0], points[len(points)-1]) {
		points = append(points, points[0]) // close polygon
	}

	if len(points) < 4 {
		return fmt.Errorf("polygon contains too few points")
	}

	// construct the polygon ring of the WKT representation
    // (see https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry;
    // note that only a single ring is supported for now)
    polygonRing := []string{}
    for _, point := range points {
   		polygonRing = append(polygonRing, fmt.Sprintf("%f %f", point.Lon, point.Lat))
    }

	srid := "4326" // spatial reference system ID
	whereExpr := fmt.Sprintf(
		"ST_DWITHIN(point, ST_GeomFromText($1, %s)::geography, 0.0)", srid)

	query := fmt.Sprintf(`
		SELECT id, point FROM geo_point WHERE %s`, whereExpr)

	rows, err := db.Query(query, fmt.Sprintf("polygon((%s))", strings.Join(polygonRing, ",")))
	if err != nil {
		return fmt.Errorf("db.Query() failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var gpID int64
		var point postgis.PointS

		if err := rows.Scan(&gpID, &point); err != nil {
			return fmt.Errorf("rows.Scan() failed: %v", err)
		}

		geoPoints[gpID] = &datastore.Point{
			Lon: point.X,
			Lat: point.Y,
		}
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

	return timeExpr
}

type filterInfo struct {
	colName string
	patterns []string // NOTE: only []string supported for now
}

// getMdataFilter derives from request and filterInfos the expression used in a WHERE clause
// for "match any" filtering on a set of attributes.
// The expression will be of the form
//   (
//     ((<attr1 matches pattern1,1>) OR (<attr1 matches pattern1,2>) OR ...) AND
//     ((<attr2 matches pattern2,1>) OR (<attr1 matches pattern2,2>) OR ...) AND
//     ...
//   )
// Values to be used for query placeholders are appended to phVals.
// Returns expression.
func getMdataFilter(
	request *datastore.GetObsRequest, filterInfos []filterInfo,
	phVals *[]interface{}) string {

	whereExprAND := []string{}

	for _, fi := range filterInfos {
		addWhereCondMatchAnyPattern(
			fi.colName, fi.patterns, &whereExprAND, phVals)
	}

	whereExpr := "TRUE" // by default, don't filter
	if len(whereExprAND) > 0 {
		whereExpr = fmt.Sprintf("(%s)", strings.Join(whereExprAND, " AND "))
	}

	return whereExpr
}

// getObs gets into obs all observations from table observation that match time range and other
// metadata in request, time series in timeSeries, and geo points in geoPoints (disabling geo
// filtering altogether if gpInfos is nil).
// Returns nil upon success, otherwise error.
func getObs(
	db *sql.DB, request *datastore.GetObsRequest, timeSeries map[int64]*datastore.TSMetadata,
	geoPoints map[int64]*datastore.Point, obs *[]*datastore.Metadata2) error {

	phVals := []interface{}{} // placeholder values

	tsIDs := []string{}
	for id := range timeSeries {
		tsIDs = append(tsIDs, fmt.Sprintf("%d", id))
	}

	gpIDs := []string{}
	if geoPoints != nil {
		for id := range geoPoints {
			gpIDs = append(gpIDs, fmt.Sprintf("%d", id))
		}
	} else {
		gpIDs = nil
	}

	timeExpr := getTimeFilter(request.GetInterval())

	obsMdataExpr := getMdataFilter(request, []filterInfo{
		{"processing_level", request.GetProcessingLevels()},
		// TODO: add search filters for more observation columns
	}, &phVals)

	query := fmt.Sprintf(`
		SELECT ts_id, id, geo_point_id, pubtime, data_id, history, metadata_id, obstime_instant,
		    processing_level, value
		FROM observation
		WHERE %s AND %s AND %s AND %s
		ORDER BY ts_id, obstime_instant
	`,
	createSetFilter("ts_id", tsIDs),
	createSetFilter("geo_point_id", gpIDs),
	timeExpr, obsMdataExpr)

	rows, err := db.Query(query, phVals...)
	if err != nil {
		return fmt.Errorf("db.Query() failed: %v", err)
	}
	defer rows.Close()

	currTsID := int64(-1)
	for rows.Next() {
		var (
			tsID int64
			id string
			gpID int64
			pubTime0 time.Time
			dataID string
			history string
			metadataID string
			obsTimeInstant0 time.Time
			processingLevel string
			value string
		)
		if err := rows.Scan(&tsID, &id, &gpID, &pubTime0, &dataID, &history, &metadataID,
			&obsTimeInstant0, &processingLevel, &value); err != nil {
			return fmt.Errorf("rows.Scan() failed: %v", err)
		}
		if (len(*obs) == 0) || (tsID != currTsID) { // add new time series
			currTsID = tsID
			tsMdata, found := timeSeries[tsID]
			if !found {
				return fmt.Errorf("timeSeries[%d] not found", tsID)
			}
			*obs = append(*obs, &datastore.Metadata2{
				TsMdata: tsMdata,
				ObsMdata: []*datastore.ObsMetadata{},
			})
		}

		// add observation to current time series

		obsMdata := &datastore.ObsMetadata{
			Id: id,
			Geometry: &datastore.ObsMetadata_GeoPoint{
				GeoPoint: geoPoints[gpID],
			},
			Pubtime: timestamppb.New(pubTime0),
			DataId: dataID,
			History: history,
			MetadataId: metadataID,
			Obstime: &datastore.ObsMetadata_ObstimeInstant{
				ObstimeInstant: timestamppb.New(obsTimeInstant0),
			},
			ProcessingLevel: processingLevel,
			Value: value,
		}
		last := len(*obs) - 1
		(*obs)[last].ObsMdata = append((*obs)[last].ObsMdata, obsMdata)
	}

	return nil
}

// GetObservations ... (see documentation in StorageBackend interface)
func (sbe *PostgreSQL) GetObservations(request *datastore.GetObsRequest) (
	*datastore.GetObsResponse, error) {

	var err error

	timeSeries := map[int64]*datastore.TSMetadata{}
	if err = getTimeSeries(sbe.Db, request, timeSeries); err != nil {
		return nil, fmt.Errorf("getTimeSeries() failed: %v", err)
	}

	geoPoints := map[int64]*datastore.Point{}
	if inside := request.GetInside(); inside != nil {
		if err = getGeoPoints(sbe.Db, inside, geoPoints); err != nil {
			return nil, fmt.Errorf("getGeoPoints() failed: %v", err)
		}
	} else {
		geoPoints = nil // no geo search requested; disable filter altogether
	}

	obs := []*datastore.Metadata2{}
	if err = getObs(sbe.Db, request, timeSeries, geoPoints, &obs); err != nil {
		return nil, fmt.Errorf("getObs() failed: %v", err)
	}

	return &datastore.GetObsResponse{Observations: obs}, nil
}
