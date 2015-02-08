#!/bin/bash

HOST=`hostname`
CIP=`grep "$HOST-docker" docker-net/nodes/qp-docker-ips | sed "s/.*,//g"`
CID=`docker.io ps -q | tail -1`
if [ "$CID" != "" ] && [ "$CIP" != "" ]; then
    echo "Running: pipework vdocker0 $CID $CIP/24"
    docker-net/bin/pipework vdocker0 $CID "$CIP"/24
else
    echo "No container ID/IP found, skipping..."
    exit 1
fi
