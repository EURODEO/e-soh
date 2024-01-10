#!/usr/bin/env bash

CONNECTION_STRING=$1  # Postgres connection string
UPTIME_AMOUNT=${2:-1}  # Number of e.g. hours, minutes, seconds
UPTIME_TYPE=${3:-"minute"}  # E.g. hour, minute, second

# Return exit code based on the uptime of postgres
if [[ $(psql "${CONNECTION_STRING}" -XtAc \
 "SELECT COUNT(*) FROM (SELECT current_timestamp - pg_postmaster_start_time() AS uptime) AS t WHERE t.uptime > interval '${UPTIME_AMOUNT} ${UPTIME_TYPE}'") == 1 ]];
then
  exit 0
else
  exit 1
fi
