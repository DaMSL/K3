#!/bin/bash
nice -n 19 k3 "$@" &> /app_data/stdouterr.log
