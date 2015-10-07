#!/bin/bash

if [ "$#" -ne 4 ]; then
	echo "Usage: $0 <perf.data input> <k3 binary> <cluster perf binary path> <output name>"
	exit 1
fi

PERF_INPUT=$1
BINARY_PATH=$2
SHADOW_PERF_PATH=$3
OUTPUT_DIR=`dirname $4`
OUTPUT_NAME=`basename $4`
BINARY=`basename $BINARY_PATH`

SHADOW_BINARY_DIR=/mnt/mesos/sandbox
DOCKER_BASE_DIR=/var/lib/docker/aufs/diff
PERF_DIR=/usr/bin
FLAMEGRAPH_DIR=./FlameGraph

test -d $OUTPUT_DIR || mkdir -p $OUTPUT_DIR
test -d $SHADOW_BINARY_DIR || mkdir -p $SHADOW_BINARY_DIR
test -f $SHADOW_BINARY_DIR/$BINARY || cp $BINARY_PATH $SHADOW_BINARY_DIR/$BINARY
test -d $DOCKER_BASE_DIR || mkdir -p $DOCKER_BASE_DIR

perf report -i $PERF_INPUT > perf_symbol_test.txt 2>&1

cat perf_symbol_test.txt | grep Failed | grep docker | \
	sed 's/Failed to open \([^,]*\),.*/\1/' | \
	sed 's/\/var\/lib\/docker\/aufs\/diff\///; s/\([0-9a-z]*\)\/.*/\1/' \
	> docker_aufs_paths.txt && \

for i in `cat docker_aufs_paths.txt | sort | uniq`; do ln -s / $DOCKER_BASE_DIR/$i; done

cp $PERF_DIR/perf $PERF_DIR/perf.bak
cp $SHADOW_PERF_PATH $PERF_DIR/perf
perf script -i $PERF_INPUT > $OUTPUT_DIR/$OUTPUT_NAME.perf
mv $PERF_DIR/perf.bak $PERF_DIR/perf

$FLAMEGRAPH_DIR/stackcollapse-perf.pl $OUTPUT_DIR/$OUTPUT_NAME.perf > $OUTPUT_DIR/$OUTPUT_NAME.folded
$FLAMEGRAPH_DIR/flamegraph.pl $OUTPUT_DIR/$OUTPUT_NAME.folded > $OUTPUT_DIR/$OUTPUT_NAME.svg
