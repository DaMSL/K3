#!/bin/bash

cat $1 | grep Failed | grep docker | sed 's/Failed to open \([^,]*\),.*/\1/' | sed 's/\/var\/lib\/docker\/aufs\/diff\///; s/\([0-9a-z]*\)\/.*/\1/' > docker_aufs_paths.txt
for i in `cat docker_aufs_paths.txt | sort | uniq`; do ln -s / /var/lib/docker/aufs/diff/$i; done

