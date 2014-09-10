#! /bin/sh

service start redis-server
redis-sentinel /k3/K3-Driver/scripts/docker/discovery/sentinel.conf >& /var/log/redis-sentinel-output.log &

exec /bin/bash
