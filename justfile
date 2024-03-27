# list recipes
default:
    @just --list

set positional-arguments

# After running just all, the database needs cleanup, run just down
all: lint unit build services load integration performance client
up: build services
test: unit load integration

# ---------------------------------------------------------------------------- #
#                                  utility                                     #
# ---------------------------------------------------------------------------- #
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
    )

    for path in "${destination_paths[@]}"
    do
        cp -v -r "./protobuf" "$path"
    done


check-python-version:
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


# run pip-compile for all the requirement files
pip-compile: check-python-version
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
lint: check-python-version
    #!/usr/bin/env bash
    set -euxo pipefail

    if ! command -v pre-commit &> /dev/null; then
        python -m pip install pre-commit
    fi

    pre-commit run --config './.pre-commit-config.yaml' --all-files --color=always --show-diff-on-failure


# run tests
integration: build
    docker compose --env-file ./ci/config/env.list run --rm integration


unit: build
    docker compose run --rm api-unit


performance: build
    docker compose --env-file ./ci/config/env.list run --rm performance


# After running client the database needs cleanup, run just down.
client:
    docker compose --env-file ./ci/config/env.list run --rm client

# ---------------------------------------------------------------------------- #
#                                    build                                     #
# ---------------------------------------------------------------------------- #
build: copy-proto
    docker compose --env-file ./ci/config/env.list --profile test build


# # ---------------------------------------------------------------------------- #
# #                                     local                                    #
# # ---------------------------------------------------------------------------- #
services:
    docker compose --env-file ./ci/config/env.list up -d --wait --wait-timeout 120


load:
    docker compose --env-file ./ci/config/env.list run --rm loader


down:
    docker compose --profile test down --volumes
