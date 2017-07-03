#!/bin/sh
go get golang.org/x/tools/cmd/goimports
count=$(goimports -l $(find . -type f -name '*.go' -not -path "./vendor/*") | wc -l)
if [ $count -eq 0 ];then
	echo 'All files correctly formatted'
else
	echo 'Some files incorrectly formatted, please run goimports on them'
	goimports -l $(find . -type f -name '*.go' -not -path "./vendor/*")
	exit 1
fi

go get github.com/golang/lint/golint
golint -set_exit_status $(go list ./... | grep -v /vendor/)
