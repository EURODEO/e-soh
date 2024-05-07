package postgresql

import (
	"database/sql"
	"datastore/datastore"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// getTemporalExtent gets the current temporal extent of all observations in the storage.
func getTemporalExtent(db *sql.DB) (*datastore.TimeInterval, error) {
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

// getSpatialExtent gets the current horizontally spatial extent of all observations in the storage.
func getSpatialExtent(db *sql.DB) (*datastore.BoundingBox, error) {
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
		Left:   xmin,
		Bottom: ymin,
		Right:  xmax,
		Top:    ymax,
	}, nil
}

// GetExtents ... (see documentation in StorageBackend interface)
func (sbe *PostgreSQL) GetExtents(request *datastore.GetExtentsRequest) (
	*datastore.GetExtentsResponse, codes.Code, string) {

	var err error

	temporalExtent, err := getTemporalExtent(sbe.Db)
	if err != nil {
		return nil, codes.Internal, fmt.Sprintf("getTemporalExtent() failed: %v", err)
	}

	spatialExtent, err := getSpatialExtent(sbe.Db)
	if err != nil {
		return nil, codes.Internal, fmt.Sprintf("getSpatialExtent() failed: %v", err)
	}

	return &datastore.GetExtentsResponse{
		TemporalExtent: temporalExtent,
		SpatialExtent:  spatialExtent,
	}, codes.OK, ""
}
