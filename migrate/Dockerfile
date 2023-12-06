FROM migrate/migrate:4

SHELL ["/bin/sh", "-eux", "-o", "pipefail", "-c"]

# update packages in container
RUN apk update \
    && apk upgrade \
    && rm -rf /var/cache/apk/*
