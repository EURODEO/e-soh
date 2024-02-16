package dsimpl

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"datastore/common"
	"datastore/datastore"
)

// getTemporalSpec derives and validates a temporal specification from request.
//
// Returns (spec, nil) upon success, otherwise (..., error).
func getTemporalSpec(request *datastore.GetObsRequest) (common.TemporalSpec, error) {

	tspec := common.TemporalSpec{}

	// get mode
	tspec.IntervalMode = true // assume 'interval' mode by default
	if tmode0 := request.GetTemporalMode(); tmode0 != "" {
		tmode := strings.ToLower(strings.TrimSpace(tmode0))
		switch {
		case tmode == "latest":
			tspec.IntervalMode = false
		case tmode != "interval":
			return common.TemporalSpec{}, fmt.Errorf(
				"expected either 'interval' or 'latest' for temporal_mode; found '%s'", tmode)
		}
	}

	if tspec.IntervalMode { // validate and initialize for 'interval' mode

		ti := request.GetTemporalInterval()

		// do general validation of interval
		if ti != nil {
			if ti.Start != nil && ti.End != nil {
				if ti.End.AsTime().Before(ti.Start.AsTime()) {
					return common.TemporalSpec{}, fmt.Errorf("end(%v) < start(%v)", ti.End, ti.Start)
				}
			}
		}

		tspec.Interval = ti // valid (but possibly nil to specify a fully open interval!)

	} else { // validate and initialize for 'latest' mode

		// latest_limit ...
		if limit0 := request.GetLatestLimit(); limit0 != "" {
			limit, err := strconv.Atoi(limit0)
			if err != nil {
				return common.TemporalSpec{},
					fmt.Errorf("failed to convert latest_limit to an int: %v", limit0)
			}

			if limit < 0 {
				return common.TemporalSpec{},
					fmt.Errorf("latest_limit cannot be negative: %d", limit)
			}

			tspec.LatestLimit = limit // valid

		} else {
			// by default return the single latest obs for a time series
			tspec.LatestLimit = 1 // default
		}

		// latest_maxage ...
		if maxage0 := request.GetLatestMaxage(); maxage0 != "" {
			// ### TODO! (convert maxage0 (an ISO-8601 period) into a time.Duration and assign to
			// tspec.LatestMaxage)
			return common.TemporalSpec{}, fmt.Errorf("latest_maxage unimplemented")
		} else {
			// by default disable filtering on maxage, i.e. don't consider any observation as too
			// old
			tspec.LatestMaxage = time.Duration(-1)
		}
	}

	return tspec, nil
}

func (svcInfo *ServiceInfo) GetObservations(
	ctx context.Context, request *datastore.GetObsRequest) (
	*datastore.GetObsResponse, error) {

	tspec, err := getTemporalSpec(request)
	if err != nil {
		return nil, fmt.Errorf("dsimpl.GetTemporalSpec() failed: %v", err)
	}

	response, err := svcInfo.Sbe.GetObservations(request, tspec)
	if err != nil {
		return nil, fmt.Errorf("svcInfo.Sbe.GetObservations() failed: %v", err)
	}

	return response, nil
}
