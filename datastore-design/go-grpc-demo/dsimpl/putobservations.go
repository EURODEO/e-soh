package dsimpl

import (
	"context"
	"fmt"

	"datastore/datastore"
)

func (svcInfo *ServiceInfo) PutObservations(
	ctx context.Context, request *datastore.PutObsRequest) (
		*datastore.PutObsResponse, error) {

	fmt.Printf("PutObservations(); Tsobs: %v\n", request.Tsobs)

	// TODO: add observations to storage backend

	response := datastore.PutObsResponse{
		Status: -1, // for now
	}

	return &response, nil
}
