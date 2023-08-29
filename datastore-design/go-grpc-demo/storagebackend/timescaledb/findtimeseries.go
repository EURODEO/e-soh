package timescaledb

import (
	"datastore/datastore"
	"fmt"

	postgis "github.com/cridenour/go-postgis"
	_ "github.com/lib/pq"
)

// matchAnyValCond creates the sub-condition of a WHERE clause that checks if string column 'name'
// matches any of a set of values.
// Returns the sub-condition to be appended to a WHERE clause.
func matchAnyValCond(name string, values []string) string {
	if len(values) == 0 {
		return ""
	}
	inArg := ""
	for i, value := range values {
		if i > 0 {
			inArg += ","
		}
		inArg += fmt.Sprintf("'%s'", value)
	}
	return fmt.Sprintf(" AND %s IN (%s)", name, inArg)
}

func equal(p1, p2 *datastore.Point) bool {
	return (p1.Lat == p2.Lat) && (p1.Lon == p2.Lon)
}

// insidePolygonCond creates a sub-condition of a WHERE clause that checks if geo point column
// 'name' is contained in a geo polygon.
// Returns the sub-condition to be appended to a WHERE clause.
func insidePolygonCond(name string, polygon0 *datastore.Polygon) (string, error) {

	if polygon0 == nil {
		// absent polygon => disable filtering (note: "" is equivalent to " AND TRUE")
		return "", nil
	}

	points := polygon0.Points

	// validate
	if len(points) < 3 {
		return "", fmt.Errorf("polygon must contain at least 3 points; found %d", len(points))
	}
	if equal(points[0], points[len(points) - 1]) {
		return "", fmt.Errorf("polygon endpoints must be different")
	}

	// construct the polygon ring of the WKT representation
	// (see https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry;
	// note that only a single ring is supported for now)
	polygonRing := ""
	points = append(points, points[0]) // close polygon
	for i, point := range points  {
		if i > 0 {
			polygonRing += ","
		}
		polygonRing += fmt.Sprintf("%f %f", point.Lon, point.Lat)
	}

	srid := "4326" // spatial reference system ID

	return fmt.Sprintf(
		" AND ST_WITHIN(ST_SetSRID(%s::geometry, %s), ST_GeomFromText('polygon((%s))', %s))",
		name, srid, polygonRing, srid), nil
}

// FindTimeSeries ... (see documentation in StorageBackend interface)
func (sbe *TimescaleDB) FindTimeSeries(request *datastore.FindTSRequest) (
	*datastore.FindTSResponse, error) {

	query := `
		SELECT id, station_id, param_id, pos, other1, other2, other3 FROM time_series WHERE TRUE
	`

	query += matchAnyValCond("station_id", request.StationIds)
	query += matchAnyValCond("param_id", request.ParamIds)
	if inPolyCond, err := insidePolygonCond("pos", request.Inside); err != nil {
		return nil, fmt.Errorf("insidePolygonCond() failed: %v", err)
	} else {
		query += inPolyCond
	}
	// TODO: add more filters

	rows, err := sbe.Db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("sbe.Db.Query() failed: %v", err)
	}

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
