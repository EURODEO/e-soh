#!/bin/bash

declare -a destination_paths=(
  "./api"
  "./datastore/data-loader"
  "./datastore/datastore"
  "./datastore/examples/clients/python"
  "./datastore/integration-test"
  "./ingest"
)

for path in "${destination_paths[@]}"
do
  cp -v -r "./protobuf" "$path"
done
