#!/bin/sh

exec migrate -path /migrations -database "postgres://${DB_USER}:${DB_PASS}@${DB_URL}:${DB_PORT:-5432}/data?sslmode=${ENABLE_SSL:-disable}" up
