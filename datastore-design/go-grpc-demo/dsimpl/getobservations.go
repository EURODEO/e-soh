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
		"GetObservations(); Tsids: %v; Fromtime: %v; Totime: %v\n",
		request.Tsids, request.Fromtime, request.Totime)

	if request.Totime < request.Fromtime {
		return nil, fmt.Errorf("To(%d) < From(%d)", request.Totime,
			request.Fromtime)
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
	timeStep := max(1, (request.Totime - request.Fromtime) / nSteps)
	for _, tsid := range request.Tsids {
		obs := []*datastore.Observation{}
		obsTime := request.Fromtime
		for i := 0; obsTime <= request.Totime; i++ {
			obsVal := 10 + float64(i)
			obs = append(obs, &datastore.Observation{
				Time: obsTime,
				Value: obsVal,
				Metadata: &datastore.ObsMetadata{
					Field1: fmt.Sprintf("value1 (%d)", i),
					Field2: fmt.Sprintf("value2 (%d)", i),
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
