package dsimpl

import (
	"context"
	"fmt"

	"datastore/datastore"
)

func (svcInfo *ServiceInfo) GetObservations(
	ctx context.Context, request *datastore.GetObsRequest) (
		*datastore.GetObsResponse, error) {

	fmt.Printf(
		"GetObservations(); Tsids: %v; From: %v; To: %v\n",
		request.Tsids, request.From, request.To)

	if request.To < request.From {
		return nil, fmt.Errorf("To(%d) < From(%d)", request.To, request.From)
	}

	tsobs := []*datastore.TSObservations{}

	// --- BEGIN TODO: retrieve observations from storage backend -----------
	// ... for now:
	var nSteps int64 = 3
	max := func(x, y int64) int64 {
		if x > y {
			return x
		}
		return y
	}
	timeStep := max(1, (request.To - request.From) / nSteps)
	for _, tsid := range request.Tsids {
		obs := []*datastore.Observation{}
		obsTime := request.From
		for i := 0; obsTime <= request.To; i++ {
			obsVal := 10 + float64(i)
			obs = append(obs, &datastore.Observation{
				Time: obsTime,
				Value: obsVal,
				Metadata: &datastore.ObsMetadata{
					Field1: fmt.Sprintf("dummy1 (%d)", i),
					Field2: fmt.Sprintf("dummy2 (%d)", i),
				},
			})
			obsTime += timeStep
		}
		tsobs = append(tsobs, &datastore.TSObservations{
			Tsid: tsid,
			Obs: obs,
		})
	}
	// --- END TODO: retrieve observations from storage backend -----------

	response := datastore.GetObsResponse{
		Status: -1, // for now
		Tsobs: tsobs,
	}

	return &response, nil
}
