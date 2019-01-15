
import os
import sys
import signal
from flask import Flask , request
import logging
import paho.mqtt.client as mqtt
from MQTTPubSub import MQTTPubSub
import time
import json
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
	reqdict={"User_IP":user_ip,"Stream_ID":stream_id}
	client.publish("stream/request", json.dumps(reqdict))
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
		streamadddict={"Stream_ID":stream_id,"Stream_IP":stream_ip}
		client.publish("stream/add",json.dumps(streamadddict))
		return "Stream added "+ stream_id +" "+ stream_ip
	elif stream_op=="delete":
		print "Deleted Stream "+ str(stream_id)
		streamdeldict={"Stream_ID":stream_id,"Stream_IP":stream_ip}
		client.publish("stream/delete",json.dumps(streamdeldict))
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
		originadddict={"Origin_ID":origin_id,"Origin_IP":origin_ip}
		client.publish("origin/add",json.dumps(originadddict))
		return "ORIGIN server added "+ origin_id +" "+ origin_ip
	elif origin_op=="delete":
		print "Deleted Origin Server " +str(origin_ip)
		origindeldict={"Origin_ID":origin_id,"Origin_IP":origin_ip}
		client.publish("origin/delete",json.dumps(origindeldict))
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
		distadddict={"Dist_ID":dist_id,"Dist_IP":dist_ip}
		client.publish("dist/add",json.dumps(distadddict))
		return "DIST server added "+ dist_id +" "+ dist_ip
	elif dist_op=="delete":
		print "Deleted Distribution "+str(dist_ip)
		distdeldict={"Dist_ID":dist_id,"Dist_IP":dist_ip}
		client.publish("dist/delete",json.dumps(distdeldict))
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






	
