# list recipes
default:
    @just --list

set positional-arguments

# Run all docker services. After running the database needs cleanup, run just destroy
all: lint build unit services load integration performance client
# Build and run the default docker services
up: build services
# Build and run the unit, load and integration tests
test: build unit ingest_load integration

# Build and run the ingest loader and integration tests
ingest_test: build ingest_load integration

# ---------------------------------------------------------------------------- #
#                                  utility                                     #
# ---------------------------------------------------------------------------- #
# Copy the protofile to the Docker directories
copy-proto:
    #!/usr/bin/env bash
    set -euxo pipefail

    declare -a destination_paths=(
        "./api"
        "./datastore/data-loader"
        "./datastore/datastore"
        "./datastore/examples/clients/python"
        "./datastore/integration-test"
        "./datastore/load-test"
        "./ingest"
    )

    for path in "${destination_paths[@]}"
    do
        cp -v -r "./protobuf" "$path"
    done


# Check if the python version is 3.11 for reproducability
_check-python-version:
    #!/usr/bin/env bash
    set -euxo pipefail

    # Get Python version
    python_version=$(python --version 2>&1 | cut -d' ' -f2)

    # Extract major and minor version numbers
    major_version=$(echo "$python_version" | cut -d'.' -f1)
    minor_version=$(echo "$python_version" | cut -d'.' -f2)

    # Check if Python version is greater than or equal to 3.11
    if [[ "$major_version" -lt 3 || ( "$major_version" -eq 3 && "$minor_version" -lt 11 ) ]]; then
        echo "Error: Python version must be greater than or equal to 3.11"
        exit 1
    fi


# Run pip-compile for all the requirement files
pip-compile: _check-python-version
    #!/usr/bin/env bash
    set -euxo pipefail

    find . \
        -iname "*requirements.in" \
        -type f \
        -print \
        -execdir \
        pip-compile --upgrade --no-emit-index-url \
        {} ';'


# ---------------------------------------------------------------------------- #
#                                    test                                      #
# ---------------------------------------------------------------------------- #
# Run the pre-commit hook
lint: _check-python-version
    #!/usr/bin/env bash
    set -euxo pipefail

    if ! command -v pre-commit &> /dev/null; then
        python -m pip install pre-commit
    fi

    pre-commit run --config './.pre-commit-config.yaml' --all-files --color=always --show-diff-on-failure


# Run the integration tests
integration:
    docker compose --env-file ./ci/config/env.list run --rm integration


# Run the unit tests
unit:
    docker compose run --rm api-unit


# Run the performance tests
performance:
    docker compose --env-file ./ci/config/env.list run --rm performance


# Run the client test; after running client the database needs cleanup, run just destroy.
client:
    docker compose --env-file ./ci/config/env.list run --rm client


# ---------------------------------------------------------------------------- #
#                                    build                                     #
# ---------------------------------------------------------------------------- #
# Build the docker images
build: copy-proto
    docker compose --env-file ./ci/config/env.list --profile test build


# # ---------------------------------------------------------------------------- #
# #                                     local                                    #
# # ---------------------------------------------------------------------------- #
# Start the default docker compose containers
services:
    docker compose --env-file ./ci/config/env.list up -d --wait --wait-timeout 120


# Load the data in the database
load:
    docker compose --env-file ./ci/config/env.list run --rm loader

ingest_load:
    docker compose --env-file ./ci/config/env.list run --rm ingest_loader

# Stop all E-SOH containers
down:
    docker compose --profile test down


# Stop all E-SOH containers and remove their volumes
destroy:
    docker compose --profile test down --volumes
