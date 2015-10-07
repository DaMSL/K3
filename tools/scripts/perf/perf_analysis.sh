#!/bin/bash

if [ "$#" -lt 4 ]; then
    echo "Usage: $0 <k3 binary> <cluster perf binary path> <output name> <perf.data input ...>"
    exit 1
fi

BINARY_PATH=$1
SHADOW_PERF_PATH=$2
OUTPUT_DIR=`dirname $3`
OUTPUT_NAME=`basename $3`
PERF_INPUTS=${@:4}

BINARY=`basename $BINARY_PATH`

SHADOW_BINARY_DIR=/mnt/mesos/sandbox
DOCKER_BASE_DIR=/var/lib/docker/aufs/diff
PERF_DIR=/usr/bin
FLAMEGRAPH_DIR=./FlameGraph

test -d $OUTPUT_DIR || mkdir -p $OUTPUT_DIR
test -d $SHADOW_BINARY_DIR || mkdir -p $SHADOW_BINARY_DIR
test -f $SHADOW_BINARY_DIR/$BINARY || cp $BINARY_PATH $SHADOW_BINARY_DIR/$BINARY
test -d $DOCKER_BASE_DIR || mkdir -p $DOCKER_BASE_DIR

cp $PERF_DIR/perf $PERF_DIR/perf.bak
cp $SHADOW_PERF_PATH $PERF_DIR/perf

PERF_ID=0

for pfile in $PERF_INPUTS; do
    perf report -i $pfile > psyms-$PERF_ID.txt 2>&1

    cat psyms-$PERF_ID.txt | grep Failed | grep docker | \
        sed 's/Failed to open \([^,]*\),.*/\1/' | \
        sed 's/\/var\/lib\/docker\/aufs\/diff\///; s/\([0-9a-z]*\)\/.*/\1/' \
        > docker_aufs_paths-$PERF_ID.txt && \

    for i in `cat docker_aufs_paths-$PERF_ID.txt | sort | uniq`; do ln -s / $DOCKER_BASE_DIR/$i; done

    perf script -i $pfile > $OUTPUT_DIR/$OUTPUT_NAME-$PERF_ID.perf

    $FLAMEGRAPH_DIR/stackcollapse-perf.pl $OUTPUT_DIR/$OUTPUT_NAME-$PERF_ID.perf > $OUTPUT_DIR/$OUTPUT_NAME-$PERF_ID.folded
    $FLAMEGRAPH_DIR/flamegraph.pl $OUTPUT_DIR/$OUTPUT_NAME-$PERF_ID.folded > $OUTPUT_DIR/$OUTPUT_NAME-$PERF_ID.svg

    ((PERF_ID+=1))
done

mv $PERF_DIR/perf.bak $PERF_DIR/perf
