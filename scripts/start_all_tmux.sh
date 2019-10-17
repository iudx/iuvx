./scripts/killall.sh

touch /etc/mosquitto/passwd
echo -e "$MQTT_PASSWD\n$MQTT_PASSWD\n" | mosquitto_passwd /etc/mosquitto/passwd $MQTT_UNAME
sed -i s/#allow_anonymous\ true/allow_anonymous\ true/ /etc/mosquitto/mosquitto.conf
/opt/nginx/sbin/nginx
redis-server --daemonize yes
mosquitto -d
# Project and root directories
PROJ_DIR=/vidiot/src

# $ Arguments required to allow resizing
tmux new-session -s "LB"  -d 
tmux new-window -t "LB":0 -n "LB"
tmux split-window -t "LB":0.0 -h 
tmux split-window -t "LB":0.1 -v
tmux split-window -t "LB":0.2 -v


tmux send-keys -t "LB":0.0 "cd ${PROJ_DIR} && python HTTPserver.py " C-j
tmux send-keys -t "LB":0.1 "cd ${PROJ_DIR} && su vid -c 'celery -A loadbalancercelery worker --loglevel=info' " C-j
tmux send-keys -t "LB":0.2 "cd ${PROJ_DIR} && python celeryLBmain.py " C-j


# $ Arguments required to allow resizing
tmux new-session -s "O" -n "O" -d 

tmux select-pane -t 0
tmux send-keys  "cd ${PROJ_DIR} && su vid -c 'celery -A OriginCelery worker --loglevel=info' " C-j

tmux split-window -h
tmux send-keys  "cd ${PROJ_DIR} && python originffmpegspawner.py " C-j


tmux split-window -t  -v
tmux send-keys  "cd ${PROJ_DIR} && python originffmpegkiller.py " C-j

tmux select-pane -t 0
tmux split-window -t -v
tmux send-keys "cd ${PROJ_DIR} && python originstatchecker.py " C-j
