# list recipes
default:
    @just --list

set positional-arguments

# After running just all, the database needs cleanup, run just down
all: lint build services load test
up: build services load integration

# ---------------------------------------------------------------------------- #
#                                  utility                                     #
# ---------------------------------------------------------------------------- #
copy-proto:
    #!/usr/bin/env bash
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
test: integration performance client

lint: check-python-version
    #!/usr/bin/env bash
    if ! command -v pre-commit &> /dev/null; then
        python -m pip install pre-commit
    fi

    pre-commit run --config './.pre-commit-config.yaml' --all-files --color=always --show-diff-on-failure


# run tests
integration:
    docker compose --env-file ./ci/config/env.list run --rm integration


performance:
    #!/usr/bin/env bash
    cd datastore/load-test || exit 1
    pip install -r requirements.txt

    echo "Run load test (read only)."
    python --version
    python -m grpc_tools.protoc --proto_path=./protobuf datastore.proto --python_out=. --grpc_python_out=.
    locust -f locustfile_read.py --headless -u 5 -r 10 --run-time 60 --only-summary --csv store_read

    echo "Run load test (write + read)."
    python -m grpc_tools.protoc --proto_path=./protobuf datastore.proto --python_out=load-test --grpc_python_out=load-test
    python schedule_write.py > schedule_write.log 2>&1 &
    locust -f locustfile_read.py --headless -u 5 -r 10 --run-time 60 --only-summary --csv store_rw
    kill %1
    echo "Catting schedule_write output..."
    cat schedule_write.log
    echo "Done catting"
    cd ../.. || exit 1


# After running client the database needs cleanup, run just down.
client:
    docker compose --env-file ./ci/config/env.list run --rm client

# ---------------------------------------------------------------------------- #
#                                    build                                     #
# ---------------------------------------------------------------------------- #
build: copy-proto
    docker compose --profile test build


# # ---------------------------------------------------------------------------- #
# #                                     local                                    #
# # ---------------------------------------------------------------------------- #
services:
    docker compose --env-file ./ci/config/env.list up -d --wait --wait-timeout 120


load:
    docker compose --env-file ./ci/config/env.list run --rm loader


down:
    docker compose --profile test down --volumes
