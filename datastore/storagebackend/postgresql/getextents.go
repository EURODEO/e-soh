package postgresql

import (
	"database/sql"
	"datastore/datastore"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// getTimeExtent gets the current time extent of all observations in the storage.
func getTimeExtent(db *sql.DB) (*datastore.TimeInterval, error) {
	query := "SELECT min(obstime_instant), max(obstime_instant) FROM observation"
	row := db.QueryRow(query)

	var start, end time.Time

	err := row.Scan(&start, &end)
	if err != nil {
		return nil, fmt.Errorf("row.Scan() failed: %v", err)
	}

	return &datastore.TimeInterval{
		Start: timestamppb.New(start),
		End:   timestamppb.New(end),
	}, nil
}

// getGeoExtent gets the current geo extent of all observations in the storage.
func getGeoExtent(db *sql.DB) (*datastore.BoundingBox, error) {
	query := `
		SELECT ST_XMin(ext), ST_YMin(ext), ST_XMax(ext), ST_YMax(ext)
		FROM (SELECT ST_Extent(point::geometry) AS ext FROM geo_point) t
	`
	row := db.QueryRow(query)

	var xmin, ymin, xmax, ymax float64

	err := row.Scan(&xmin, &ymin, &xmax, &ymax)
	if err != nil {
		return nil, fmt.Errorf("row.Scan() failed: %v", err)
	}

	return &datastore.BoundingBox{
		XMin: xmin,
		YMin: ymin,
		XMax: xmax,
		YMax: ymax,
	}, nil
}

// GetExtents ... (see documentation in StorageBackend interface)
func (sbe *PostgreSQL) GetExtents(request *datastore.GetExtentsRequest) (
	*datastore.GetExtentsResponse, error) {

	var err error

	timeExtent, err := getTimeExtent(sbe.Db)
	if err != nil {
		return nil, fmt.Errorf("getTimeExtent() failed: %v", err)
	}

	geoExtent, err := getGeoExtent(sbe.Db)
	if err != nil {
		return nil, fmt.Errorf("getGeoExtent() failed: %v", err)
	}

	return &datastore.GetExtentsResponse{TimeExtent: timeExtent, GeoExtent: geoExtent}, nil
}
