package dsimpl

import (
	"context"
	"fmt"

	"datastore/datastore"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (svcInfo *ServiceInfo) PutObservations(
	ctx context.Context, request *datastore.PutObsRequest) (
	*datastore.PutObsResponse, error) {

	errCode, reason := svcInfo.Sbe.PutObservations(request)
	if errCode != codes.OK {
		return nil, status.Error(
			errCode, fmt.Sprintf("svcInfo.Sbe.PutObservations() failed: %s", reason))
	}

	return &datastore.PutObsResponse{
		Status: -1, // for now
	}, nil
}
