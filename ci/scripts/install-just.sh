#!/usr/bin/env bash

if ! command -v curl &> /dev/null; then
    apt install curl
fi

curl -o /tmp/install.sh https://just.systems/install.sh \
    && chmod +x /tmp/install.sh \
    && /tmp/install.sh --to /usr/local/bin \
    && rm -rf /tmp/install.sh
