
import os
import sys
import signal
from flask import Flask , request
import logging
import paho.mqtt.client as mqtt
import time
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)
app = Flask(__name__)
broker_address="localhost"
client = mqtt.Client("httpserver")
rtmp_link=""

def on_message(client, userdata, message):
	global rtmp_link
	msg=str(message.payload.decode("utf-8"))
	topic=str(message.topic.decode("utf-8"))
	rtmp_link=msg


@app.route('/reqstream')
def reqstream():
	global client, rtmp_link
	rtmp_link=""
	client.loop_start()
	stream_id  = request.args.get('id', None)
	user_ip=request.remote_addr
	client.publish("stream/request", user_ip+" "+stream_id)
	client.subscribe("lbsresponse/rtmp")
	time.sleep(10)
	client.loop_stop()
	if rtmp_link=="":
		return "No Stream Available"
	else:
		return rtmp_link


@app.route('/streams')
def stream():
	global client
	client.loop_start()
   	stream_ip  = request.args.get('ip', None)
	stream_id  = request.args.get('id', None)
	stream_op=request.args.get('opr', None)
	if stream_op=="add":
		client.publish("stream/add",str(stream_id)+" "+str(stream_ip))
		client.loop_stop()
		return "Stream added "+ stream_id +" "+ stream_ip
	elif stream_op=="delete":
		client.publish("stream/delete",str(stream_id)+" "+str(stream_ip))
		client.loop_stop()
		return "Stream deleted "+ stream_id +" "+ stream_ip
	else:
		client.loop_stop()
		return "Incorrect request"

@app.route('/origin')
def origin():
	global client
	client.loop_start()
	origin_ip  = request.args.get('ip', None)
	origin_id  = request.args.get('id', None)
	origin_op=request.args.get('opr', None)
	if origin_op=="add":
		client.publish("origin/add",str(origin_id)+" "+str(origin_ip))
		client.loop_stop()
		return "ORIGIN server added "+ origin_id +" "+ origin_ip
	elif origin_op=="delete":
		client.publish("origin/delete",str(origin_id)+" "+str(origin_ip))
		client.loop_stop()
		return "ORIGIN server deleted "+ origin_id +" "+ origin_ip
	else:
		client.loop_stop()
		return "Incorrect request"


@app.route('/dist')
def dist():
	global client
	client.loop_start()
	dist_ip  = request.args.get('ip', None)
	dist_id  = request.args.get('id', None)
	dist_op=request.args.get('opr', None)
	if dist_op=="add":
		client.publish("dist/add",str(dist_id)+" "+str(dist_ip))
		return "DIST server added "+ dist_id +" "+ dist_ip
		client.loop_stop()
	elif dist_op=="delete":
		client.publish("dist/delete",str(dist_id)+" "+str(dist_ip))
		return "DIST server deleted "+ dist_id +" "+ dist_ip
		client.loop_stop()
	else:
		client.loop_stop()
		return "Incorrect request"
		


if __name__=="__main__":
	client.connect(broker_address)
	client.on_message=on_message
	app.run(host="0.0.0.0",threaded=True,debug = True)






	
