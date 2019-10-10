#!/bin/bash
./scripts/killall.sh
sudo /usr/local/nginx/sbin/nginx
python3 ./scripts/start_lb.py
python3 ./scripts/start_origin.py
