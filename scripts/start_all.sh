#!/bin/sh

# To be used in docker only
./scripts/killall.sh

echo -e "$MQTT_PASSWD\n$MQTT_PASSWD\n" | mosquitto_passwd /etc/mosquitto/passwd $MQTT_UNAME
sed -i s/#allow_anonymous\ true/allow_anonymous\ true/ /etc/mosquitto/mosquitto.conf \
/opt/nginx/sbin/nginx
redis-server --daemonize yes
mosquitto -d
python ./scripts/start_lb.py
python ./scripts/start_origin.py
