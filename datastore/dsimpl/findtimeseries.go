package dsimpl

import (
	"context"
	"fmt"

	"datastore/datastore"
)

func (svcInfo *ServiceInfo) FindTimeSeries(
	ctx context.Context, request *datastore.FindTSRequest) (
	*datastore.FindTSResponse, error) {

	response, err := svcInfo.Sbe.FindTimeSeries(request)
	if err != nil {
		return nil, fmt.Errorf("svcInfo.Sbe.FindTimeSeries() failed: %v", err)
	}

	return response, nil
}
