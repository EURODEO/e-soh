package storagebackend

import (
	datastore "datastore/datastore"
)

// StorageBackend is the interface for an observation storage backend.
type StorageBackend interface {

	// Description returns the description of the backend.
	Description() string

	// AddTimeSeries adds a time series to the storage.
	// Returns nil upon success, otherwise error.
	AddTimeSeries(*datastore.AddTSRequest) error

	// FindTimeSeries finds a set of time series in the storage.
	// Returns nil upon success, otherwise error.
	FindTimeSeries(*datastore.FindTSRequest) (*datastore.FindTSResponse, error)

	// PutObservations inserts observations in the storage.
	// Returns nil upon success, otherwise error.
	PutObservations(*datastore.PutObsRequest) error

	// GetObservations retrieves observations from the storage.
	// Returns nil upon success, otherwise error.
	GetObservations(*datastore.GetObsRequest) (*datastore.GetObsResponse, error)
}
