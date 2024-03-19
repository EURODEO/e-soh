#!/usr/bin/env bash

docker compose --env-file ./ci/config/env.list up -d --wait --wait-timeout 120

docker compose --env-file ./ci/config/env.list run --rm loader
