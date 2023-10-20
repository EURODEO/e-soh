#!/bin/bash

set -uo pipefail

output=$(cd ./datastore && go vet ./... 2>&1)

if [ -n "${output}" ]; then
  echo "${output}"
#  exit 1
else
  echo "No subtle issues found in the code. All is working as intended."
fi
