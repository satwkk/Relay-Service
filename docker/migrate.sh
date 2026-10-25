#!/bin/sh
set -eu

: "${DATABASE_URL:?DATABASE_URL must be set}"

exec migrate -path=/migrations -database="$DATABASE_URL" "$@"
