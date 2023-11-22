package dsimpl

import (
	"context"
	"fmt"

	"datastore/datastore"
)

func (svcInfo *ServiceInfo) GetTSAttrGroups(
	ctx context.Context, request *datastore.GetTSAGRequest) (*datastore.GetTSAGResponse, error) {

	// ensure that at least one attribute is specified
	if len(request.Attrs) == 0 {
		return nil, fmt.Errorf("no attributes specified")
	}

	response, err := svcInfo.Sbe.GetTSAttrGroups(request)
	if err != nil {
		return nil, fmt.Errorf("svcInfo.Sbe.GetTSAttrGroups() failed: %v", err)
	}

	return response, nil
}
