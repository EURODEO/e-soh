package timescaledb

import (
	"datastore/datastore"
	"fmt"
	"strings"

	postgis "github.com/cridenour/go-postgis"
	_ "github.com/lib/pq"
)

// matchAnyValCond creates the sub-condition of a WHERE clause that checks if string column 'name'
// matches any of a set of values. Placeholder values are appended to phVals.
// Returns sub-condition (with placeholders) to be appended to a WHERE clause.
func matchAnyValCond(phVals *[]interface{}, name string, values []string) string {

	if len(values) == 0 {
		return ""
	}

	inArgs := []string{}
	phIndex := len(*phVals) + 1 // ensure placeholders start at $1, not $0
	for _, value := range values {
		inArgs = append(inArgs, fmt.Sprintf("$%d", phIndex))
		*phVals = append(*phVals, value)
		phIndex++
	}

	return fmt.Sprintf(" AND %s IN (%s)", name, strings.Join(inArgs, ","))
}

func equal(p1, p2 *datastore.Point) bool {
	return (p1.Lat == p2.Lat) && (p1.Lon == p2.Lon)
}

// insidePolygonCond creates a sub-condition of a WHERE clause that checks if geo point column
// 'name' is contained in a geo polygon.
// Returns (the sub-condition to be appended to a WHERE clause, nil) upon success, otherwise
// (..., error).
func insidePolygonCond(name string, polygon0 *datastore.Polygon) (string, error) {

	if polygon0 == nil {
		// absent polygon => disable filtering (note: "" is equivalent to " AND TRUE")
		return "", nil
	}

	points := polygon0.Points

	// validate polygon and close it if necessary
	if len(points) < 3 {
		return "", fmt.Errorf("polygon must contain at least 3 points; found %d", len(points))
	}
	if equal(points[0], points[len(points) - 1]) { // endpoints equal -> polygon already closed
		if len(points) == 3 {
			return "", fmt.Errorf(
				"polygon must contain at least 4 points when endpoints are equal; found %d",
				len(points))
		}
	} else { // endpoints different, so close polygon
		points = append(points, points[0])
	}

	// assert(equal(points[0], points[len(points) - 1]))

	// construct the polygon ring of the WKT representation
	// (see https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry;
	// note that only a single ring is supported for now)
	polygonRing := []string{}
	for _, point := range points  {
		polygonRing = append(polygonRing, fmt.Sprintf("%f %f", point.Lon, point.Lat))
	}

	srid := "4326" // spatial reference system ID

	return fmt.Sprintf(
		" AND ST_WITHIN(ST_SetSRID(%s::geometry, %s), ST_GeomFromText('polygon((%s))', %s))",
		name, srid, strings.Join(polygonRing, ""), srid), nil
}

// FindTimeSeries ... (see documentation in StorageBackend interface)
func (sbe *TimescaleDB) FindTimeSeries(request *datastore.FindTSRequest) (
	*datastore.FindTSResponse, error) {

	query := `
		SELECT id, station_id, param_id, pos, other1, other2, other3 FROM time_series WHERE TRUE
	`

	phVals := []interface{}{} // placeholder values

	query += matchAnyValCond(&phVals, "station_id", request.StationIds)

	query += matchAnyValCond(&phVals, "param_id", request.ParamIds)

	if cond, err := insidePolygonCond("pos", request.Inside); err != nil {
		return nil, fmt.Errorf("insidePolygonCond() failed: %v", err)
	} else {
		query += cond
	}

	// TODO: add more filters

	rows, err := sbe.Db.Query(query, phVals...)
	if err != nil {
		return nil, fmt.Errorf("sbe.Db.Query() failed: %v", err)
	}
	defer rows.Close()

	tseries := []*datastore.TimeSeries{}

	for rows.Next() {
		var (
			tsID int64
			stationID string
			paramID string
			pos postgis.PointS
			other1 string
			other2 string
			other3 string
		)

		if err := rows.Scan(
			&tsID, &stationID, &paramID, &pos, &other1, &other2, &other3); err != nil {
			return nil, fmt.Errorf("rows.Scan() failed: %v", err)
		}

		tseries = append(tseries, &datastore.TimeSeries{
			Id: tsID,
			Metadata: &datastore.TSMetadata{
				StationId: stationID,
				ParamId: paramID,
				Pos: &datastore.Point{
					Lat: pos.Y,
					Lon: pos.X,
				},
				Other1: other1,
				Other2: other2,
				Other3: other3,
			},
		})
	}

	return &datastore.FindTSResponse{Tseries: tseries}, nil
}
