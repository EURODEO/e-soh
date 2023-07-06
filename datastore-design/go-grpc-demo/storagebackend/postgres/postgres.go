package postgres

import (
	"database/sql"
	"datastore/common"
	"datastore/datastore"
	"fmt"

	_ "github.com/lib/pq"
)

// Postgres is an implementation of the StorageBackend interface that accesses
// data through a Postgres database.
type Postgres struct {
	Db *sql.DB
}

// Description ... (see documentation in StorageBackend interface)
func (sbe *Postgres) Description() string {
	return "Postgres database"
}

// AddTimeSeries ... (see documentation in StorageBackend interface)
func (sbe *Postgres) AddTimeSeries(request *datastore.AddTSRequest) error {
	// TODO
	return fmt.Errorf("Postgres/AddTimeSeries() not implemented yet")
}

// PutObservations ... (see documentation in StorageBackend interface)
func (sbe *Postgres) PutObservations(request *datastore.PutObsRequest) error {
	// TODO
	return fmt.Errorf("Postgres/PutObservations() not implemented yet")
}

// GetObservations ... (see documentation in StorageBackend interface)
func (sbe *Postgres) GetObservations(request *datastore.GetObsRequest) (
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

	return nil, fmt.Errorf("Postgres/GetObservations() not implemented yet")
}

// NewPostgres creates and returns a new Postgres instance.
func NewPostgres() (*Postgres, error) {
	sbe := new(Postgres)

	// get connection info
	host := common.Getenv("PSBHOST", "localhost")
	port := common.Getenv("PSBPORT", "5433")
	user := common.Getenv("PSBUSER", "postgres")
	password := common.Getenv("PSBPASSWORD", "")
	dbname := "data" // hard-coded for now

    // create and validate connection to Postgres server

	pgInfo := fmt.Sprintf(
	    "host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
        host, port, user, password, dbname)
	var err error
	sbe.Db, err = sql.Open("postgres", pgInfo)
	if err != nil {
		return nil, fmt.Errorf("sql.Open() failed: %v", err)
	}

	if err = sbe.Db.Ping(); err != nil  {
		return nil, fmt.Errorf("sbe.Db.Ping() failed: %v", err)
	}

	// connection ok

	return sbe, nil
}
