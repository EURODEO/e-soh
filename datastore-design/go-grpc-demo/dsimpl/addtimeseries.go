package dsimpl

import (
	"context"
	"fmt"

	"datastore/datastore"
)

func (svcInfo *ServiceInfo) AddTimeSeries(
	ctx context.Context, request *datastore.AddTSRequest) (
		*datastore.AddTSResponse, error) {

	err := svcInfo.Sbe.AddTimeSeries(request)
	if err != nil {
		return nil, fmt.Errorf("svcInfo.Sbe.AddTimeSeries() failed: %v", err)
	}

	return &datastore.AddTSResponse{
		Status: -1, // for now
	}, nil
}
