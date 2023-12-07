FROM kartoza/postgis:15

SHELL ["/bin/bash", "-eux", "-o", "pipefail", "-c"]

# update packages in container
RUN apt-get update \
    && apt-get -y upgrade \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY postgis_healthcheck.sh /
