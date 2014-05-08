#!/bin/bash

for CID in `docker.io ps -q`; do
    echo "Stopping container $CID"
    docker.io kill "$CID"
done
