package dsimpl

import (
	"context"
	"fmt"

	"datastore/datastore"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (svcInfo *ServiceInfo) GetExtents(
	ctx context.Context, request *datastore.GetExtentsRequest) (
	*datastore.GetExtentsResponse, error) {

	response, errCode, reason := svcInfo.Sbe.GetExtents(request)
	if errCode != codes.OK {
		return nil, status.Error(
			errCode, fmt.Sprintf("svcInfo.Sbe.GetExtents() failed: %s", reason))
	}

	return response, nil
}
