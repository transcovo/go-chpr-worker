#!/bin/sh
# Run the tests + coverage for all files in the project.
# This is the command to use for TDD, it is meant to be faster than coverage.sh,
# which compiles files several times and is very slow.
# However the report is less pretty and it is not compatible with coveralls.

set -e

go get -t .

go test -v -race -cover -timeout=15000ms $(go list ./... | grep -v vendor)

EXIT_STATUS=$?

if [ $EXIT_STATUS -eq 0 ];then
   echo '\033[0;32m!!! SUCCESS !!!\033[0m'
else
   echo '\033[0;31m!!! FAILURE !!!\033[0m'
   exit $EXIT_STATUS
fi
