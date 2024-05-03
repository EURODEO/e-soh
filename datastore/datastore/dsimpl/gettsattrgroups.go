package dsimpl

import (
	"context"
	"fmt"

	"datastore/datastore"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (svcInfo *ServiceInfo) GetTSAttrGroups(
	ctx context.Context, request *datastore.GetTSAGRequest) (*datastore.GetTSAGResponse, error) {

	// ensure that at least one attribute is specified
	if len(request.Attrs) == 0 {
		return nil, status.Error(codes.InvalidArgument, "no attributes specified")
	}

	response, errCode, reason := svcInfo.Sbe.GetTSAttrGroups(request)
	if errCode != codes.OK {
		return nil, status.Error(
			errCode, fmt.Sprintf("svcInfo.Sbe.GetTSAttrGroups() failed: %s", reason))
	}

	return response, nil
}
