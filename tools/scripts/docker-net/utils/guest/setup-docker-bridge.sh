#!/bin/bash

if [ "$#" -ne 2 ]; then
  echo "$0 <source device> <target mac address>"
fi

ip link add link $1 dev vdocker0 addr $2 type macvlan mode bridge
ifup vdocker0
route del -net 192.168.0.0 netmask 255.255.0.0 dev vdocker0
route add -net 192.168.1.0 netmask 255.255.255.0 dev vdocker0
