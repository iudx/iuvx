#!/bin/sh
killall python
killall celery
tmux kill-server
/opt/nginx/sbin/nginx -s stop

