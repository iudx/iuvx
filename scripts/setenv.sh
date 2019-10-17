#!/bin/bash

echo "Setting environment variables"
# Set common environment variable
export HTTP_IP=0.0.0.0
export LB_IP=10.156.14.138
export HTTP_PORT=5000
export MONGO_INITDB_ROOT_USERNAME=root
export MONGO_INITDB_ROOT_PASSWORD=root
export MQTT_PORT=1883
export MQTT_UNAME=vid
export MQTT_PASSWD=vid@rbc
export MONGO_URL=localhost
export ORIGIN_IP=10.156.14.138
export ORIGIN_ID=TestOrigin
export DIST_IP=10.156.14.138
export DIST_ID=TestDist
export ROOT_uname=username
export ROOT_passwd=password
export FFMPEG_PATH=/usr/local/bin/ffmpeg
export NGINX_PATH=/opt/nginx/sbin/nginx
# Note, this should be relative to the execution path
export STREAMS=./scripts/streams.json
