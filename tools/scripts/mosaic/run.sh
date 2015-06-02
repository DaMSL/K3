#!/bin/sh

tools/scripts/mosaic/gen_yaml.py $@ > temp.yaml
if [ ! -d "out" ] ; then mkdir out; fi
rm out/*
__build/A -p temp.yaml -j out

