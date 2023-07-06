package dsimpl

import (
	"context"
	"fmt"

	"datastore/datastore"
)

func (svcInfo *ServiceInfo) PutObservations(
	ctx context.Context, request *datastore.PutObsRequest) (
		*datastore.PutObsResponse, error) {

	err := svcInfo.Sbe.PutObservations(request)
	if err != nil {
		return nil, fmt.Errorf("svcInfo.Sbe.PutObservations() failed: %v", err)
	}

	return &datastore.PutObsResponse{
		Status: -1, // for now
	}, nil
}
