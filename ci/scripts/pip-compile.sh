#!/usr/bin/env bash

./ci/scripts/check-python-version.sh

echo "Compiling all Python requirements."
find . \
  -iname "*requirements.in" \
  -type f \
  -print \
  -execdir \
  pip-compile --upgrade --no-emit-index-url \
  {} ';'
