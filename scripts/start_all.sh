#!/bin/bash
./scripts/killall.sh
python3 ./scripts/start_lb.py
python3 ./scripts/start_origin.py
