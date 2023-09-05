package dsimpl

import (
	"context"
	"fmt"

	"datastore/datastore"
)

func (svcInfo *ServiceInfo) DeleteTimeSeries(
	ctx context.Context, request *datastore.DeleteTSRequest) (*datastore.DeleteTSResponse, error) {

	err := svcInfo.Sbe.DeleteTimeSeries(request)
	if err != nil {
		return nil, fmt.Errorf("svcInfo.Sbe.DeleteTimeSeries() failed: %v", err)
	}

	return &datastore.DeleteTSResponse{
		Status: -1, // for now
	}, nil
}
