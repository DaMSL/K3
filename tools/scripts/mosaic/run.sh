#!/bin/sh

tools/scripts/mosaic/gen_yaml.py $@ > temp.yaml
rm out/*
__build/A -p temp.yaml -j out

