#!/bin/sh

source ./tools/lint.sh
EXIT_STATUS=$?

if [ $EXIT_STATUS -eq 0 ];then
   echo 'Linting is ok'
else
   exit $EXIT_STATUS
fi

source ./tools/coverage.sh
