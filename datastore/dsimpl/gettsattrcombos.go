package dsimpl

import (
	"context"
	"fmt"

	"datastore/datastore"
)

func (svcInfo *ServiceInfo) GetTSAttrCombos(
	ctx context.Context, request *datastore.GetTSACRequest) (*datastore.GetTSACResponse, error) {

	// ensure that at least one attribute is specified
	if len(request.Attrs) == 0 {
		return nil, fmt.Errorf("no attributes specified")
	}

	response, err := svcInfo.Sbe.GetTSAttrCombos(request)
	if err != nil {
		return nil, fmt.Errorf("svcInfo.Sbe.GetTSAttrCombos() failed: %v", err)
	}

	return response, nil
}
