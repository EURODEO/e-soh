#!/usr/bin/env bash

echo "Copying protobuf files."
./ci/scripts/copy-protobuf.sh

echo "Building docker containers for the profile test."
docker compose --profile test build
