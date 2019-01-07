
import os
import sys
import signal
from flask import Flask , request
import logging
import paho.mqtt.client as mqtt
from MQTTPubSub import MQTTPubSub
import time
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)
app = Flask(__name__)
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
	stream_id  = request.args.get('id', None)
	user_ip=request.remote_addr
	print "Request Stream.....User: "+ str(user_ip)+" Stream: "+ str(stream_id)
	client.publish("stream/request", user_ip+" "+stream_id)
	time.sleep(10)
	if rtmp_link=="":
		return "No Stream Available"
	else:
		return rtmp_link


@app.route('/streams')
def stream():
	global client
   	stream_ip  = request.args.get('ip', None)
	stream_id  = request.args.get('id', None)
	stream_op=request.args.get('opr', None)
	if stream_op=="add":
		print "Added Stream "+ str(stream_id)
		client.publish("stream/add",str(stream_id)+" "+str(stream_ip))
		return "Stream added "+ stream_id +" "+ stream_ip
	elif stream_op=="delete":
		print "Deleted Stream "+ str(stream_id)
		client.publish("stream/delete",str(stream_id)+" "+str(stream_ip))
		return "Stream deleted "+ stream_id +" "+ stream_ip
	else:
		return "Incorrect request"

@app.route('/origin')
def origin():
	global client
	origin_ip  = request.args.get('ip', None)
	origin_id  = request.args.get('id', None)
	origin_op=request.args.get('opr', None)
	if origin_op=="add":
		print "Added Origin Server " +str(origin_ip)
		client.publish("origin/add",str(origin_id)+" "+str(origin_ip))
		return "ORIGIN server added "+ origin_id +" "+ origin_ip
	elif origin_op=="delete":
		print "Deleted Origin Server " +str(origin_ip)
		client.publish("origin/delete",str(origin_id)+" "+str(origin_ip))
		return "ORIGIN server deleted "+ origin_id +" "+ origin_ip
	else:
		return "Incorrect request"


@app.route('/dist')
def dist():
	global client
	dist_ip  = request.args.get('ip', None)
	dist_id  = request.args.get('id', None)
	dist_op=request.args.get('opr', None)
	if dist_op=="add":
		print "Added Distribution "+str(dist_ip)
		client.publish("dist/add",str(dist_id)+" "+str(dist_ip))
		return "DIST server added "+ dist_id +" "+ dist_ip
	elif dist_op=="delete":
		print "Deleted Distribution "+str(dist_ip)
		client.publish("dist/delete",str(dist_id)+" "+str(dist_ip))
		return "DIST server deleted "+ dist_id +" "+ dist_ip
	else:
		return "Incorrect request"

mqttServerParams = {}
mqttServerParams["url"] = "127.0.0.1"
mqttServerParams["port"] = 1883
mqttServerParams["timeout"] = 60
mqttServerParams["topic"] = "lbsresponse/rtmp"
mqttServerParams["onMessage"] = on_message
client = MQTTPubSub(mqttServerParams)

if __name__=="__main__":
	client.run()
	app.run(host="0.0.0.0",threaded=True,debug = True)






	
