package dsimpl

import (
	"context"
	"fmt"

	"datastore/datastore"
)

func (svcInfo *ServiceInfo) GetObservations(
	ctx context.Context, request *datastore.GetObsRequest) (
	*datastore.GetObsResponse, error) {

	if request.Totime.AsTime().Before(request.Fromtime.AsTime()) {
		return nil, fmt.Errorf("To(%v) < From(%v)", request.Totime,
			request.Fromtime)
	}

	response, err := svcInfo.Sbe.GetObservations(request)
	if err != nil {
		return nil, fmt.Errorf("svcInfo.Sbe.GetObservations() failed: %v", err)
	}

	return response, nil
}
