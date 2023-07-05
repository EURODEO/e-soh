package dsimpl

import (
	"context"
	"fmt"

	"datastore/datastore"
)

func (svcInfo *ServiceInfo) AddTimeSeries(
	ctx context.Context, request *datastore.AddTSRequest) (
		*datastore.AddTSResponse, error) {

	fmt.Printf(
		"AddTimeSeries(); Id: %v; Metadata: %v\n", request.Id, request.Metadata)

	// TODO: add time series to storage backend

	response := datastore.AddTSResponse{
		Status: -1, // for now
	}

	return &response, nil
}
