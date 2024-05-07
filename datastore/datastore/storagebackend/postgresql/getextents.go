package postgresql

import (
	"database/sql"
	"datastore/common"
	"datastore/datastore"
	"fmt"

	_ "github.com/lib/pq"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// createExtQueryVals creates from request values used for querying extentions.
//
// Values to be used for query placeholders are appended to phVals.
//
// Upon success the function returns two values:
// - time filter used in a 'WHERE ... AND ...' clause
// - string metadata ... ditto
// - nil,
// otherwise (..., ..., error).
func createExtQueryVals(request *datastore.GetExtentsRequest, phVals *[]interface{}) (
	string, string, error) {

	loTime, hiTime := common.GetValidTimeRange()
	timeFilter := fmt.Sprintf(`
		((obstime_instant >= to_timestamp(%d)) AND (obstime_instant <= to_timestamp(%d)))
	`, loTime.Unix(), hiTime.Unix())

	stringMdataFilter, err := getStringMdataFilter(request.GetFilter(), phVals)
	if err != nil {
		return "", "", fmt.Errorf("getStringMdataFilter() failed: %v", err)
	}

	return timeFilter, stringMdataFilter, nil
}

// GetExtents ... (see documentation in StorageBackend interface)
func (sbe *PostgreSQL) GetExtents(request *datastore.GetExtentsRequest) (
	*datastore.GetExtentsResponse, codes.Code, string) {

	// get values needed for query
	phVals := []interface{}{} // placeholder values
	timeFilter, stringMdataFilter, err := createExtQueryVals(request, &phVals)
	if err != nil {
		return nil, codes.Internal, fmt.Sprintf("createQueryVals() failed: %v", err)
	}

	query := fmt.Sprintf(`
		SELECT temp_min, temp_max,
			ST_XMin(spat_ext), ST_YMin(spat_ext), ST_XMax(spat_ext), ST_YMax(spat_ext)
		FROM (
			SELECT min(obstime_instant) AS temp_min, max(obstime_instant) AS temp_max,
				ST_Extent(point::geometry) AS spat_ext
			FROM observation
			JOIN time_series ON observation.ts_id = time_series.id
			JOIN geo_point ON observation.geo_point_id = geo_point.id
			WHERE %s AND %s
		) t
	`, timeFilter, stringMdataFilter)

	row := sbe.Db.QueryRow(query, phVals...)

	var (
		start, end             sql.NullTime
		xmin, ymin, xmax, ymax float64
	)

	err = row.Scan(&start, &end, &xmin, &ymin, &xmax, &ymax)
	if !start.Valid { // indicates no matching rows found!
		return nil, codes.NotFound, "no matching data to compute extensions for"
	} else if err != nil {
		return nil, codes.Internal, fmt.Sprintf("row.Scan() failed: %v", err)
	}

	return &datastore.GetExtentsResponse{
		TemporalExtent: &datastore.TimeInterval{
			Start: timestamppb.New(start.Time),
			End:   timestamppb.New(end.Time),
		},
		SpatialExtent: &datastore.BoundingBox{
			Left:   xmin,
			Bottom: ymin,
			Right:  xmax,
			Top:    ymax,
		},
	}, codes.OK, ""
}
