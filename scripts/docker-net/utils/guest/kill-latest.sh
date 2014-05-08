#!/bin/bash

CID=`docker.io ps -q | tail -1`
if [ "$CID" != "" ]; then
    echo "Stopping container $CID"
    docker.io kill "$CID"
else
    echo "No container found"
    exit 1
fi
