package dsimpl

import (
	"context"
	"fmt"

	"datastore/common"
	"datastore/datastore"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// getTemporalSpec derives and validates a temporal specification from request.
//
// Returns (spec, nil) upon success, otherwise (..., error).
func getTemporalSpec(request *datastore.GetObsRequest) (common.TemporalSpec, error) {

	tspec := common.TemporalSpec{
		Latest:   request.GetTemporalLatest(),
		Interval: request.GetTemporalInterval(),
	}

	ti := tspec.Interval
	if ti != nil { // validate
		if ti.Start != nil && ti.End != nil {
			if ti.End.AsTime().Before(ti.Start.AsTime()) {
				return common.TemporalSpec{}, fmt.Errorf("end(%v) < start(%v)", ti.End, ti.Start)
			}
		}
	}

	return tspec, nil
}

func (svcInfo *ServiceInfo) GetObservations(
	ctx context.Context, request *datastore.GetObsRequest) (
	*datastore.GetObsResponse, error) {

	tspec, err := getTemporalSpec(request)
	if err != nil {
		return nil, status.Error(
			codes.Internal, fmt.Sprintf("getTemporalSpec() failed: %v", err))
	}

	response, errCode, reason := svcInfo.Sbe.GetObservations(request, tspec)
	if errCode != codes.OK {
		return nil, status.Error(
			errCode, fmt.Sprintf("svcInfo.Sbe.GetObservations() failed: %s", reason))
	}

	return response, nil
}
