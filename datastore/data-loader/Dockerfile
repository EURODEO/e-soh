FROM python:3.11-slim-bookworm

SHELL ["/bin/bash", "-eux", "-o", "pipefail", "-c"]

ARG THROUGH_INGEST=false
ENV DOCKER_PATH="/clients/python"

COPY "test-data/KNMI/20221231.nc" "${DOCKER_PATH}/test-data/KNMI/20221231.nc"

RUN apt-get update \
    && apt-get -y upgrade \
    && apt-get -y install --no-install-recommends build-essential \
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

COPY "./parameters.py" "${DOCKER_PATH}/parameters.py"

# Copy both loaders and conditionally use the wanted one
COPY "./client_knmi_station_ingest.py" "${DOCKER_PATH}/client_knmi_station_ingest.py"
COPY "./client_knmi_station.py" "${DOCKER_PATH}/client_knmi_station.py"

RUN if [ "${THROUGH_INGEST}" = "true" ]; then \
      mv "${DOCKER_PATH}/client_knmi_station_ingest.py" "${DOCKER_PATH}/client_knmi_station.py"; \
    else \
      rm "${DOCKER_PATH}/client_knmi_station_ingest.py"; \
    fi

WORKDIR "${DOCKER_PATH}"
CMD ["python", "-u", "./client_knmi_station.py"]
