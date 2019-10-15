#!/bin/sh
killall python
killall celery
/opt/nginx/sbin/nginx -s stop

