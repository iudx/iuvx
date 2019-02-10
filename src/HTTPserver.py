
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
	print msg
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
	print rtmp_link
	time.sleep(120)
	if not (rtmp_link):
		return "No Stream Available"
	else:
		return rtmp_link


@app.route('/archivestream')
def archivestream():
	global client
	stream_id  = request.args.get('id', None)
	start_date= request.args.get('start', None)
	end_date= request.args.get('end', None)
	start_time= request.args.get('sTime', None)
	end_time= request.args.get('eTime', None)
	opr=request.args.get('opr', None)
	job_id=request.args.get('job', None)
	user_ip=request.remote_addr
	print "Archive Stream.....User: "+ str(user_ip)+" Stream: "+ str(stream_id)
	if opr=="add":
		archivedict={"User_IP":user_ip,"Stream_ID":stream_id,"start_date":start_date,"start_time":start_time,"end_date":end_date,"end_time":end_time,"job_id":job_id}
		client.publish("archive/add", json.dumps(archivedict))
		return "Archived at location"
	elif opr=="delete":
		archivedict={"User_IP":user_ip,"Stream_ID":stream_id,"start_date":start_date,"start_time":start_time,"end_date":end_date,"end_time":end_time,"job_id":job_id}
		client.publish("archive/delete", json.dumps(archivedict))
		return "Archive Deleted"



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






	
