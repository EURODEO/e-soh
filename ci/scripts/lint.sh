#!/usr/bin/env bash

while getopts 'r' flag; do
  case "${flag}" in
  r) SHOW_REQUIREMENTS=true ;;
  *)
    echo "Default only integration test.
    -c [Run client test]
    -p [Run performance test]"
    exit 1
    ;;
  esac
done

./ci/scripts/check-python-version.sh

if ! command -v pre-commit &> /dev/null; then
    python -m pip install pre-commit
fi

if [[ $SHOW_REQUIREMENTS ]]; then
  echo "Show pre-commit requirements."
  python -m pip freeze --local
fi

pre-commit run --config './.pre-commit-config.yaml' --all-files --color=always --show-diff-on-failure
