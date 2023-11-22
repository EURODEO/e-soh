package dsimpl

import (
	"context"
	"fmt"

	"datastore/datastore"
)

func (svcInfo *ServiceInfo) GetExtents(
	ctx context.Context, request *datastore.GetExtentsRequest) (
	*datastore.GetExtentsResponse, error) {

	response, err := svcInfo.Sbe.GetExtents(request)
	if err != nil {
		return nil, fmt.Errorf("svcInfo.Sbe.GetExtents() failed: %v", err)
	}

	return response, nil
}
