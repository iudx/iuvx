#!/bin/bash

# To be used in docker only
./scripts/killall.sh

sed -i s/#allow_anonymous\ true/allow_anonymous\ true/ /etc/mosquitto/mosquitto.conf \
mosq
sudo /usr/local/nginx/sbin/nginx
python3 ./scripts/start_lb.py
python3 ./scripts/start_origin.py
