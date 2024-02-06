package dsimpl

import (
	"context"
	"fmt"

	"datastore/datastore"
)

func (svcInfo *ServiceInfo) GetObservations(
	ctx context.Context, request *datastore.GetObsRequest) (
	*datastore.GetObsResponse, error) {

	// do general validation of any obs time interval
	if ti := request.GetTemporalInterval(); ti != nil {
		if ti.Start != nil && ti.End != nil {
			if ti.End.AsTime().Before(ti.Start.AsTime()) {
				return nil, fmt.Errorf("end(%v) < start(%v)", ti.End, ti.Start)
			}
		}
	}

	response, err := svcInfo.Sbe.GetObservations(request)
	if err != nil {
		return nil, fmt.Errorf("svcInfo.Sbe.GetObservations() failed: %v", err)
	}

	return response, nil
}
