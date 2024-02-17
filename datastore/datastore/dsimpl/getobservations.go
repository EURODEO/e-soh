package dsimpl

import (
	"context"
	"fmt"
	"strings"

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
