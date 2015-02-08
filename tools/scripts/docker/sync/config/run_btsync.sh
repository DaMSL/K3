#!/bin/sh
# Use the exec command to start the app you want to run in this container.
# Don't let the app daemonize itself.

# Read more here: https://github.com/phusion/baseimage-docker#adding-additional-daemons
exec /usr/bin/btsync --config /btsync/btsync.conf --nodaemon
