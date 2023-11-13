package storagebackend

import (
	datastore "datastore/datastore"
)

// StorageBackend is the interface for an observation storage backend.
type StorageBackend interface {

	// Description returns the description of the backend.
	Description() string

	// PutObservations inserts observations in the storage.
	// Returns nil upon success, otherwise error.
	PutObservations(*datastore.PutObsRequest) error

	// GetObservations retrieves observations from the storage.
	// Returns nil upon success, otherwise error.
	GetObservations(*datastore.GetObsRequest) (*datastore.GetObsResponse, error)

	// GetTSAttrCombos retrieves, for the non-default attributes in the input, the unique
	// combinations of attribute values currently represented in the storage.
	GetTSAttrCombos(*datastore.GetTSACRequest) (*datastore.GetTSACResponse, error)
}
