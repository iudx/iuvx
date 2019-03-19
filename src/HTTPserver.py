
import os
import sys
import signal
from flask import Flask , request
import logging
import paho.mqtt.client as mqtt
from MQTTPubSub import MQTTPubSub
import time
import json
import hashlib
from flask_httpauth import HTTPBasicAuth

auth = HTTPBasicAuth()

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)
app = Flask(__name__)
stream_link=""
user_info=""
verified=""
allusers=""
allorigins=""
alldists=""
allarchives=""
allstreams=""



def on_message(client, userdata, message):
	global allorigins, allusers, alldists, allarchives, allstreams, stream_link, user_info, verified
	msg=str(message.payload.decode("utf-8"))
	topic=str(message.topic.decode("utf-8"))
	if topic=="lbsresponse/rtmp":
		stream_link=msg
	if topic=="lbsresponse/user":
		user_info=msg
	if topic=="lbsresponse/verified":
		verified=msg
	if topic=="lbsresponse/allarchives":
		allarchives=msg
	if topic=="lbsresponse/allusers":
		allusers=msg
	if topic=="lbsresponse/allorigins":
		allorigins=msg
	if topic=="lbsresponse/alldists":
		alldists=msg
	if topic=="lbsresponse/allstreams":
		allstreams=msg








@auth.verify_password
def verify_password(username, password):
	global client, verified
	hashed_password = hashlib.sha512(password).hexdigest()
	reqdict={"User":username,"Password":hashed_password}
	client.publish("verify/user",json.dumps(reqdict))
	while(verified==""):
		continue
	retval=verified
	verified=""
	if retval=="true":
		return True
	else:
		return False
	




@app.route('/user',methods = ['POST','DELETE','GET'])
def userfunc():
	global allusers, client, user_info
	user_id=request.args.get('id',None)
	user_pass=request.args.get('pwd',None)
	if request.method=="POST":
		if user_id==None or user_pass==None:
			return "Invalid Credentials to create user..............."
		else:
			hashed_password = hashlib.sha512(user_pass).hexdigest()
			reqdict={"User":user_id,"Password":hashed_password}
			client.publish("user/add",json.dumps(reqdict))
		while(user_info==""):
			continue
		retval=user_info
		user_info=""
		return retval
	elif request.method=="DELETE":
		if user_id==None or user_pass==None:
			print "Invalid Credentials to delete user..............."
		else:
			hashed_password = hashlib.sha512(user_pass).hexdigest()
			reqdict={"User":user_id,"Password":hashed_password}
			client.publish("user/del",json.dumps(reqdict))
		while(user_info==""):
			continue
		retval=user_info
		user_info=""
		return retval
	elif request.method=="GET":
		client.publish('user/get',"All users")
		while(allusers==""):
			continue
		retval=allusers
		allusers=""
		return "Users present: "+str(retval)
	else:
		return "Incorrect Request"

	



@app.route('/request')
@auth.login_required
def reqstream():
	global client, stream_link
	stream_link=""
	stream_id  = request.args.get('id', None)
	proc= request.args.get('proc',None)
	user_ip=request.remote_addr
	print "Request Stream.....User: "+ str(user_ip)+" Stream: "+ str(stream_id)
	reqdict={"User_IP":user_ip,"Stream_ID":stream_id}
	client.publish("stream/request", json.dumps(reqdict))
	while(stream_link==""):
		continue
	retval=stream_link
	stream_link=""
	return retval+" .... Stream will be available in a while....."


@app.route('/archive',methods = ['POST','DELETE','GET'])
@auth.login_required
def archivestream():
	global allarchives, client
	stream_id  = request.args.get('id', None)
	start_date= request.args.get('start', None)
	end_date= request.args.get('end', None)
	start_time= request.args.get('sTime', None)
	end_time= request.args.get('eTime', None)
	job_id=request.args.get('job', None)
	user_ip=request.remote_addr
	print "Archive Stream.....User: "+ str(user_ip)+" Stream: "+ str(stream_id)
	if request.method=="POST":
		archivedict={"User_IP":user_ip,"Stream_ID":stream_id,"start_date":start_date,"start_time":start_time,"end_date":end_date,"end_time":end_time,"job_id":job_id}
		client.publish("archive/add", json.dumps(archivedict))
		return "Archived at location"
	elif request.method=="DELETE":
		archivedict={"User_IP":user_ip,"Stream_ID":stream_id,"start_date":start_date,"start_time":start_time,"end_date":end_date,"end_time":end_time,"job_id":job_id}
		client.publish("archive/delete", json.dumps(archivedict))
		return "Archive Deleted"
	elif request.method=="GET":
		client.publish("archive/get","All Archives")
		while(allarchives==""):
			continue
		retval=allarchives
		allarchives=""
		return "Archives present: "+str(retval)
	else:
		return "Incorrect Request"



@app.route('/streams',methods = ['POST','DELETE','GET'])
@auth.login_required
def stream():
	global allstreams, client
	stream_ip  = request.args.get('ip', None)
	stream_id  = request.args.get('id', None)
	if request.method=="POST":
		print "Added Stream "+ str(stream_id)
		streamadddict={"Stream_ID":stream_id,"Stream_IP":stream_ip}
		client.publish("stream/add",json.dumps(streamadddict))
		return "Stream added "+ stream_id +" "+ stream_ip
	elif request.method=="DELETE":
		print "Deleted Stream "+ str(stream_id)
		streamdeldict={"Stream_ID":stream_id,"Stream_IP":stream_ip}
		client.publish("stream/delete",json.dumps(streamdeldict))
		return "Stream deleted "+ stream_id +" "+ stream_ip
	elif request.method=="GET":
		client.publish("stream/get","All streams")
		while( allstreams==""):
			continue
		retval=allstreams
		allstreams=""
		return "Streams live: "+str(retval)
	else:
		return "Incorrect Request"


@app.route('/origin',methods = ['POST','DELETE','GET'])
@auth.login_required
def origin():
	global allorigins,client
	origin_ip  = request.args.get('ip', None)
	origin_id  = request.args.get('id', None)
	if request.method=="POST":
		print "Added Origin Server " +str(origin_ip)
		originadddict={"Origin_ID":origin_id,"Origin_IP":origin_ip}
		client.publish("origin/add",json.dumps(originadddict))
		return "ORIGIN server added "+ origin_id +" "+ origin_ip
	elif request.method=="DELETE":
		print "Deleted Origin Server " +str(origin_ip)
		origindeldict={"Origin_ID":origin_id,"Origin_IP":origin_ip}
		client.publish("origin/delete",json.dumps(origindeldict))
		return "ORIGIN server deleted "+ origin_id +" "+ origin_ip
	elif request.method=="GET":
		client.publish("origin/get","All origins")
		while( allorigins==""):
			continue
		retval=allorigins
		allorigins=""
		return "Origin Servers Present: "+str(retval)
	else:
		return "Incorrect Request"

@app.route('/dist',methods = ['POST','DELETE','GET'])
@auth.login_required
def dist():
	global alldists,client
	dist_ip  = request.args.get('ip', None)
	dist_id  = request.args.get('id', None)
	if request.method=="POST":
		print "Added Distribution "+str(dist_ip)
		distadddict={"Dist_ID":dist_id,"Dist_IP":dist_ip}
		client.publish("dist/add",json.dumps(distadddict))
		return "DIST server added "+ dist_id +" "+ dist_ip
	elif request.method=="DELETE":
		print "Deleted Distribution "+str(dist_ip)
		distdeldict={"Dist_ID":dist_id,"Dist_IP":dist_ip}
		client.publish("dist/delete",json.dumps(distdeldict))
		return "DIST server deleted "+ dist_id +" "+ dist_ip
	elif request.method=="GET":
		client.publish("dist/get","All dists")
		while( alldists==""):
			continue
		retval=alldists
		alldists=""
		return "Distribution Servers present: "+str(retval)
	else:
		return "Incorrect request"

mqttServerParams = {}
mqttServerParams["url"] = "127.0.0.1"
mqttServerParams["port"] = 1883
mqttServerParams["timeout"] = 60
mqttServerParams["topic"] = [("lbsresponse/allusers",0),("lbsresponse/allorigins",0),("lbsresponse/alldists",0),("lbsresponse/allstreams",0),("lbsresponse/allarchives",0),("lbsresponse/verified",0),("lbsresponse/rtmp",0),("lbsresponse/user",0)]
mqttServerParams["onMessage"] = on_message
client = MQTTPubSub(mqttServerParams)

if __name__=="__main__":
	client.run()
	app.run(host="0.0.0.0",threaded=True,debug = True)






	
