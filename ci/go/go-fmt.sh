#!/bin/bash

set -euo pipefail

output=$(gofmt -d -e -l -s .)

if [ -n "${output}" ]; then
  echo "${output}"
  exit 1
else
  echo "All files are in the proper format."
fi
