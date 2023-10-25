FROM python:3.11-slim-bookworm

SHELL ["/bin/bash", "-eux", "-o", "pipefail", "-c"]

ENV PROJECT_DATASTORE_PATH="datastore"
ENV PROJECT_PYTHON_PATH="api"
ENV DOCKER_PATH="/app"

RUN apt-get update \
    && apt-get -y upgrade \
    && apt-get install -y --no-install-recommends git \
    # Cleanup
    && rm -rf /usr/tmp  \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY "${PROJECT_DATASTORE_PATH}/protobuf/datastore.proto" "/protobuf/datastore.proto"
COPY "${PROJECT_PYTHON_PATH}/requirements.txt" "${DOCKER_PATH}/requirements.txt"

# hadolint ignore=DL3013
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir --upgrade -r "${DOCKER_PATH}/requirements.txt"

# Compiling the protobuf file
RUN python -m grpc_tools.protoc  \
    --proto_path="protobuf" "protobuf/datastore.proto" \
    --python_out="${DOCKER_PATH}"  \
    --grpc_python_out="${DOCKER_PATH}"

COPY "${PROJECT_PYTHON_PATH}/main.py" "${DOCKER_PATH}/main.py"

WORKDIR "${DOCKER_PATH}"
CMD ["gunicorn", "main:app", "--workers=4", "--worker-class=uvicorn.workers.UvicornWorker", "--bind=0.0.0.0:8000"]
