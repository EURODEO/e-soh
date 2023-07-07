package timescaledb

import (
	"datastore/datastore"
	"fmt"

	_ "github.com/lib/pq"
)

// GetObservations ... (see documentation in StorageBackend interface)
func (sbe *TimescaleDB) GetObservations(request *datastore.GetObsRequest) (
	*datastore.GetObsResponse, error) {

	// TODO

	// tsobs := []*datastore.TSObservations{}

	// // --- BEGIN TODO: retrieve observations from database -----------
	// // ... for now:
	// var nSteps int64 = 3
	// max := func(x, y int64) int64 {
	// 	if x > y {
	// 		return x
	// 	}
	// 	return y
	// }
	// timeStep := max(1, (request.Totime - request.Fromtime) / nSteps)
	// for _, tsid := range request.Tsids {
	// 	obs := []*datastore.Observation{}
	// 	obsTime := request.Fromtime
	// 	for i := 0; obsTime <= request.Totime; i++ {
	// 		obsVal := 10 + float64(i)
	// 		obs = append(obs, &datastore.Observation{
	// 			Time: obsTime,
	// 			Value: obsVal,
	// 			Metadata: &datastore.ObsMetadata{
	// 				Field1: fmt.Sprintf("value1 (%d)", i),
	// 				Field2: fmt.Sprintf("value2 (%d)", i),
	// 			},
	// 		})
	// 		obsTime += timeStep
	// 	}
	// 	tsobs = append(tsobs, &datastore.TSObservations{
	// 		Tsid: tsid,
	// 		Obs: obs,
	// 	})
	// }
	// // --- END TODO: retrieve observations from storage backend -----------

	// return &datastore.GetObsResponse{
	// 	Status: -1, // for now
	// 	Tsobs: tsobs,
	// }, nil

	return nil, fmt.Errorf("TimescaleDB/GetObservations() not implemented yet")
}
