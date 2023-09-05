package dsimpl

import (
	"datastore/datastore"
	"datastore/storagebackend"
)

type ServiceInfo struct {
	Sbe storagebackend.StorageBackend

	// for forward compatibility (see note in datastore_grpc.pb.go (generated
	// by protoc))
	datastore.UnimplementedDatastoreServer
}
