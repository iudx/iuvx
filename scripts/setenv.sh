#!/bin/bash

echo "Setting environment variables"
# Set common environment variable
export LB_IP=10.156.14.138
export LB_PORT=5000
export MQTT_PORT=1883
export ORIGIN_IP=10.156.14.138
export ORIGIN_ID=TestOrigin
export DIST_IP=10.156.14.138
export DIST_ID=TestDist
export ROOT_uname=username
export ROOT_passwd=password
# Note, this should be relative to the execution path
export STREAMS=./scripts/streams.json
