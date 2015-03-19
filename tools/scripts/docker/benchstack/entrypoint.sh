#!/bin/bash
cd /k3-benchstack && git pull
Xvfb :1 -screen 0 1024x768x16 &> xvfb.log  &
cd /k3-benchstack/launcher/web && python -m SimpleHTTPServer &> web.log &
export DISPLAY=:1.0
source /k3-benchstack/launcher/env.sh
exec "$@"
