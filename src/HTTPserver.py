
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
from flask import Response
from flask_cors import CORS

auth = HTTPBasicAuth()

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)
app = Flask(__name__)
cors = CORS(app)
stream_link=""

#userparams
addusers=""
delusers=""
verified=""
allusers=""


#originparams
allorigins=""
addorigin=""
delorigin=""


#distparams
alldists=""
adddists=""
deldists=""


#streamparams:
addstreams=""
allstreams=""
delstreams=""


#archiveparams
allarchives=""
addarchives=""
delarchives=""


def on_message(client, userdata, message):
	global addarchives, delarchives, addstreams, adddists, delstreams, deldists, delorigin, addorigin, allorigins, allusers, alldists, allarchives, allstreams, stream_link, addusers, delusers, verified
	msg=str(message.payload.decode("utf-8"))
	print(msg)
	topic=str(message.topic.decode("utf-8"))
	if topic=="lbsresponse/rtmp":
		stream_link=msg
	if topic=="lbsresponse/user/add":
		addusers=json.loads(msg)
	if topic=="lbsresponse/user/del":
		delusers=json.loads(msg)
	if topic=="lbsresponse/verified":
		verified=json.loads(msg)
	if topic=="lbsresponse/archive/all":
		allarchives=msg
	if topic=="lbsresponse/user/all":
		allusers=msg
	if topic=="lbsresponse/origin/all":
		allorigins=msg
	if topic=="lbsresponse/dist/all":
		alldists=msg
	if topic=="lbsresponse/stream/all":
		allstreams=msg
	if topic=="lbsresponse/origin/add":
		addorigin=json.loads(msg)
	if topic=="lbsresponse/origin/del":
		delorigin=json.loads(msg)
	if topic=="lbsresponse/stream/add":
		addstreams=json.loads(msg)
	if topic=="lbsresponse/stream/del":
		delstreams=json.loads(msg)
	if topic=="lbsresponse/dist/add":
		adddists=json.loads(msg)
	if topic=="lbsresponse/dist/del":
		deldists=json.loads(msg)
	if topic=="lbsresponse/archive/add":
		addarchives=json.loads(msg)
	if topic=="lbsresponse/archive/del":
		delarchives=json.loads(msg)








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
	if retval:
		return True
	else:
		return False
	




@app.route('/user',methods = ['POST','DELETE','GET'])
def userfunc():
		global addusers, delusers, allusers, client
		if request.method=="POST":
			data= request.get_json(force=True)
			user_name  = data["username"]
			user_pass = data["password"]
			hashed_password = hashlib.sha512(user_pass).hexdigest()
			reqdict={"User":user_name,"Password":hashed_password}
			client.publish("user/add",json.dumps(reqdict))
			while(addusers==""):
				continue
			retval=addusers
			addusers=""
			if retval:
				return Response(json.dumps({"success":"User added "+ user_name }),status=200,mimetype="application/json")
			else:
				return Response(json.dumps({"error":"User already present"}),status=409,mimetype="application/json")
		elif request.method=="DELETE":
			data= request.get_json(force=True)
			user_name  = data["username"]
			user_pass = data["password"]
			hashed_password = hashlib.sha512(user_pass).hexdigest()
			reqdict={"User":user_name,"Password":hashed_password}
			client.publish("user/del",json.dumps(reqdict))
			while (delusers==""):
					continue
			retval=delusers
			delusers=""
			if retval:
				return Response(json.dumps({"success":"User deleted "+ user_name}),status=200,mimetype="application/json")
			else:
				return Response(json.dumps({"error":"User does not exist"}),status=404,mimetype="application/json")
		elif request.method=="GET":
			client.publish('user/get',"All users")
			while(allusers==""):
				continue
			retval=allusers
			allusers=""
			if retval:
				return Response(json.dumps({"success":" Users Present: "+str(retval)}),status=200, mimetype='application/json')
			else:
				return Response(json.dumps({"error":" Operation Failed"}),status=408, mimetype='application/json')
		else:
			return Response(json.dumps({"error":"Request not supported"}), status=405, mimetype='application/json')



		



@app.route('/request',methods=['POST'])
#@auth.login_required
def reqstream():
        print("Working")
	if(verify_password(request.headers["username"], request.headers["password"])):
			global client, stream_link
			stream_link=""
			data= request.get_json(force=True)
			stream_id=data["id"]
			user_ip=request.remote_addr
			reqdict={"User_IP":user_ip,"Stream_ID":stream_id}
			client.publish("stream/request", json.dumps(reqdict))
			while(stream_link==""):
				continue
			retval=stream_link
			stream_link=""
			if retval=="false":
				return Response(json.dumps({"error":" Stream not available"}),status=409, mimetype='application/json')
			else:
				return Response(json.dumps({"success":retval+" .... Stream will be available in a while....."}),status=200, mimetype="application/json")
	else:
		return Response(json.dumps({"error":"Invalid Credentials"}), status=403, mimetype='application/json')


@app.route('/archive',methods = ['POST','DELETE','GET'])
#@auth.login_required
def archivestream():
	if(verify_password(request.headers["username"], request.headers["password"])):
		global addarchives, delarchives, allarchives, client
		
		user_ip=request.remote_addr
		if request.method=="POST":
			data= request.get_json(force=True)
			stream_id  = data["id"]
			try:
				start_date= data["start"]
			except:
				start_date=None
			try:
				end_date=data["end"]
			except:
				end_date=None
			try:
				start_time= data["sTime"]
			except:
				start_time=None
			try:
				end_time= data["eTime"]
			except:
				end_time=None
			job_id=data["job"]
			archivedict={"User_IP":user_ip,"Stream_ID":stream_id,"start_date":start_date,"start_time":start_time,"end_date":end_date,"end_time":end_time,"job_id":job_id}
			client.publish("archive/add", json.dumps(archivedict))
			while( addarchives==""):
					continue
			retval=addarchives
			addarchives=""
			if retval:
				return Response(json.dumps({"success":"Archive added "+ stream_id }),status=200,mimetype="application/json")
			else:
				return Response(json.dumps({"error":"Archive already present"}),status=409,mimetype="application/json")
		elif request.method=="DELETE":
			data= request.get_json(force=True)
			stream_id  = data["id"]
			try:
				start_date= data["start"]
			except:
				start_date=None
			try:
				end_date=data["end"]
			except:
				end_date=None
			try:
				start_time= data["sTime"]
			except:
				start_time=None
			try:
				end_time= data["eTime"]
			except:
				end_time=None
			job_id=data["job"]
			archivedict={"User_IP":user_ip,"Stream_ID":stream_id,"start_date":start_date,"start_time":start_time,"end_date":end_date,"end_time":end_time,"job_id":job_id}
			client.publish("archive/delete", json.dumps(archivedict))
			while(delarchives==""):
				continue
			retval=delarchives
			delarchives=""
			if retval:
				return Response(json.dumps({"success":"Archive deleted "+ job_id+" "+stream_id }),status=200,mimetype="application/json")
			else:
				return Response(json.dumps({"error":"Archive doesn't exist"}),status=409,mimetype="application/json")
		elif request.method=="GET":
			client.publish("archive/get","All Archives")
			while(allarchives==""):
				continue
			retval=allarchives
			allarchives=""
			if retval:
				return Response(json.dumps({"success":" Archive Streams Present: "+str(retval)}),status=200, mimetype='application/json')
			else:
				return Response(json.dumps({"error":" Operation Failed"}),status=408, mimetype='application/json')
		else:
			return Response(json.dumps({"error":"Request not supported"}), status=405, mimetype='application/json')
	else:
		return Response(json.dumps({"error":"Invalid Credentials"}), status=403, mimetype='application/json')




@app.route('/streams',methods = ['POST','DELETE','GET'])
#@auth.login_required
def stream():
	if(verify_password(request.headers["username"], request.headers["password"])):
		global addstreams, delstreams, allstreams, client
		if request.method=="POST":
			data= request.get_json(force=True)
			stream_ip  = data["ip"]
			stream_id  = data["id"]
			print "Added Stream "+ str(stream_id)
			streamadddict={"Stream_ID":stream_id,"Stream_IP":stream_ip}
			client.publish("stream/add",json.dumps(streamadddict))
			while( addstreams==""):
					continue
			retval=addstreams
			addstreams=""
			if retval:
				return Response(json.dumps({"success":"Stream added "+ stream_id +" "+ stream_ip}),status=200,mimetype="application/json")
			else:
				return Response(json.dumps({"error":"Stream already present"}),status=409,mimetype="application/json")
		elif request.method=="DELETE":
			data= request.get_json(force=True)
			stream_ip  = data["ip"]
			stream_id  = data["id"]
			print "Deleted Stream "+ str(stream_id)
			streamdeldict={"Stream_ID":stream_id,"Stream_IP":stream_ip}
			client.publish("stream/delete",json.dumps(streamdeldict))
			while (delstreams==""):
					continue
			retval=delstreams
			delstreams=""
			if retval:
				return Response(json.dumps({"success":"Stream deleted "+ stream_id +" "+ stream_ip}),status=200,mimetype="application/json")
			else:
				return Response(json.dumps({"error":"Stream does not exist"}),status=404,mimetype="application/json")
		elif request.method=="GET":
			client.publish("stream/get","All streams")
			while( allstreams==""):
				continue
			retval=allstreams
			allstreams=""
			if retval:
				return Response(json.dumps({"success":" Streams Present: "+str(retval)}),status=200, mimetype='application/json')
			else:
				return Response(json.dumps({"error":" Operation Failed"}),status=408, mimetype='application/json')
		else:
			return Response(json.dumps({"error":"Request not supported"}), status=405, mimetype='application/json')

	else:
		return Response(json.dumps({"error":"Invalid Credentials"}), status=403, mimetype='application/json')




@app.route('/origin',methods = ['POST','DELETE','GET'])
#@auth.login_required
def origin():
	if(verify_password(request.headers["username"], request.headers["password"])):
		global delorigin, addorigin, allorigins,client
		if request.method=="POST":
			data= request.get_json(force=True)
			origin_ip  = data["ip"]
			origin_id  = data["id"]
			print "Added Origin Server " +str(origin_ip)
			originadddict={"Origin_ID":origin_id,"Origin_IP":origin_ip}
			client.publish("origin/add",json.dumps(originadddict))
			while( addorigin==""):
				continue
			retval=addorigin
			addorigin=""
			if retval:
				return Response(json.dumps({"success":"ORIGIN server added "+ origin_id +" "+ origin_ip}),status=200,mimetype="application/json")
			else:
				return Response(json.dumps({"error":"Origin already present"}),status=409,mimetype="application/json")
		elif request.method=="DELETE":
			data= request.get_json(force=True)
			origin_ip  = data["ip"]
			origin_id  = data["id"]
			print origin_ip,origin_id
			print "Deleted Origin Server " +str(origin_ip)
			origindeldict={"Origin_ID":origin_id,"Origin_IP":origin_ip}
			client.publish("origin/delete",json.dumps(origindeldict))
			while (delorigin==""):
				continue
			retval=delorigin
			delorigin=""
			if retval:
				return Response(json.dumps({"success":"ORIGIN server deleted "+ origin_id +" "+ origin_ip}),status=200,mimetype="application/json")
			else:
				return Response(json.dumps({"error":"ORIGIN server does not exist"}),status=404,mimetype="application/json")

		elif request.method=="GET":
			client.publish("origin/get","All origins")
			while( allorigins==""):
				continue
			retval=allorigins
			allorigins=""
			if retval:
				return Response(json.dumps({"success":"Origin Servers Present: "+str(retval)}),status=200, mimetype='application/json')
			else:
				return Response(json.dumps({"error":" Operation Failed"}),status=408, mimetype='application/json')
		else:
			return Response(json.dumps({"error":"Request not supported"}), status=405, mimetype='application/json')
	else:
		return Response(json.dumps({"error":"Invalid Credentials"}), status=403, mimetype='application/json')

@app.route('/dist',methods = ['POST','DELETE','GET'])
#@auth.login_required
def dist():
	if(verify_password(request.headers["username"], request.headers["password"])):
		global adddists, deldists, alldists,client
		if request.method=="POST":
			data= request.get_json(force=True)
			dist_ip  = data["ip"]
			dist_id  = data["id"]
			print "Added Distribution "+str(dist_ip)
			distadddict={"Dist_ID":dist_id,"Dist_IP":dist_ip}
			client.publish("dist/add",json.dumps(distadddict))
			while( adddists==""):
				continue
			retval=adddists
			adddists=""
			if retval:
				return Response(json.dumps({"success":"Distribution server added "+ dist_id +" "+ dist_ip}),status=200,mimetype="application/json")
			else:
				return Response(json.dumps({"error":"Distribution already present"}),status=409,mimetype="application/json")
		elif request.method=="DELETE":
			data= request.get_json(force=True)
			dist_ip  = data["ip"]
			dist_id  = data["id"]
			print "Deleted Distribution "+str(dist_ip)
			distdeldict={"Dist_ID":dist_id,"Dist_IP":dist_ip}
			client.publish("dist/delete",json.dumps(distdeldict))
			while (deldists==""):
				continue
			retval=deldists
			deldists=""
			if retval:
				return Response(json.dumps({"success":"Distribution server deleted "+ dist_id +" "+ dist_ip}),status=200,mimetype="application/json")
			else:
				return Response(json.dumps({"error":"Distribution server does not exist"}),status=404,mimetype="application/json")
		elif request.method=="GET":
			client.publish("dist/get","All dists")
			while( alldists==""):
				continue
			retval=alldists
			alldists=""
			if retval:
				return Response(json.dumps({"success":"Distribution Servers Present: "+str(retval)}),status=200, mimetype='application/json')
			else:
				return Response(json.dumps({"error":" Operation Failed"}),status=408, mimetype='application/json')
		else:
			return Response(json.dumps({"error":"Request not supported"}), status=405, mimetype='application/json')
	else:
		return Response(json.dumps({"error":"Invalid Credentials"}), status=403, mimetype='application/json')

mqttServerParams = {}
mqttServerParams["url"] = "127.0.0.1"
mqttServerParams["port"] = 1883
mqttServerParams["timeout"] = 60
mqttServerParams["topic"] = [("lbsresponse/archive/add",0),("lbsresponse/archive/del",0),("lbsresponse/user/add",0),("lbsresponse/user/del",0),("lbsresponse/dist/add",0),("lbsresponse/dist/del",0),("lbsresponse/stream/add",0),("lbsresponse/stream/del",0),("lbsresponse/origin/del",0),("lbsresponse/origin/add",0),("lbsresponse/user/all",0),("lbsresponse/origin/all",0),("lbsresponse/dist/all",0),("lbsresponse/stream/all",0),("lbsresponse/archive/all",0),("lbsresponse/verified",0),("lbsresponse/rtmp",0),("lbsresponse/user",0)]
mqttServerParams["onMessage"] = on_message
client = MQTTPubSub(mqttServerParams)

if __name__=="__main__":
	client.run()
	app.run(host="0.0.0.0",threaded=True,debug = True)






	
