#!/bin/bash
for i in 34 35 36 38 39 40 41;
do
  ssh 192.168.1.$i pkill -f k3;
done
