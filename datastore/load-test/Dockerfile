FROM python:3.11-slim-bookworm

SHELL ["/bin/bash", "-eux", "-o", "pipefail", "-c"]

ENV DOCKER_PATH="/load-test"

RUN apt-get update \
    && apt-get -y upgrade \
    && apt-get install gcc libc6-dev -y --no-install-recommends \
    # Cleanup
    && rm -rf /usr/tmp  \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY "./protobuf/datastore.proto" "/protobuf/datastore.proto"
COPY "./requirements.txt" "${DOCKER_PATH}/requirements.txt"

# hadolint ignore=DL3013
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir --upgrade -r "${DOCKER_PATH}/requirements.txt"

# Compiling the protobuf file
RUN python -m grpc_tools.protoc  \
    --proto_path="protobuf" "protobuf/datastore.proto" \
    --python_out="${DOCKER_PATH}"  \
    --grpc_python_out="${DOCKER_PATH}"

COPY "./grpc_user.py" "${DOCKER_PATH}/grpc_user.py"
COPY "./locustfile_read.py" "${DOCKER_PATH}/locustfile_read.py"
COPY "./variables.py" "${DOCKER_PATH}/variables.py"
COPY "./schedule_write.py" "${DOCKER_PATH}/schedule_write.py"

WORKDIR "${DOCKER_PATH}"

CMD ["/bin/bash", "-c", "echo \"Run load test (read only).\"; \
locust -f locustfile_read.py --headless -u 5 -r 10 --run-time 60 --only-summary --csv \"${DOCKER_PATH}/output/store_read\"; \
echo \"Run load test (write + read).\"; \
python schedule_write.py > \"${DOCKER_PATH}/output/schedule_write.log\" 2>&1 & \
    locust -f locustfile_read.py --headless -u 5 -r 10 --run-time 60 --only-summary --csv \"${DOCKER_PATH}/output/store_rw\" && \
    kill %1 && \
    echo \"Catting schedule_write output...\" && \
    cat \"${DOCKER_PATH}/output/schedule_write.log\" && \
    echo \"Done catting\""]
