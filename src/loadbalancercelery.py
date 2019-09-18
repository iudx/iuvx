# Needs to update the database with all the details of everythign that's going on. Plus handle all the other important requests.



from __future__ import absolute_import
from celery import Celery
#celery
from celery.utils.log import get_task_logger


import json
from MQTTPubSub import MQTTPubSub
import pymongo
import time
import datetime
import os
import sys
# LBS Params


app=Celery('loadbalancercelery',backend="redis://",broker="redis://")

logger = get_task_logger(__name__)
# originstreams={}
# diststreams={}




mongoclient=pymongo.MongoClient('mongodb://localhost:27017/')
mydb=mongoclient["ALL_Streams"]
col1=mydb["Origin_Servers"]
col2=mydb["Distribution_Servers"]
col3=mydb["Streams"]
col4=mydb["Ffmpeg_Procs"]
col5=mydb["Users"]
col6=mydb["Archives"]

#Update Origin, Dists and Streams

def search_archives():
	global col6
	archivejobid=[]
	archivestream=[]
	if col6.count()!=0:
		for i in col6.find():
			archivejobid.append(i["Job_ID"])
			archivestream.append(i["Stream_ID"])
		return archivestream,archivejobid
	else:
		return [],[]

def search_origin():
	global col1
	origin_ips=[]
	origin_ids=[]
	if col1.count()!=0:
		for i in col1.find():
			origin_ips.append(i["Origin_IP"])
			origin_ids.append(i["Origin_ID"])
		return origin_ips,origin_ids
	else:
		return [],[]

def search_dist():
	global col2
	dist_ips=[]
	dist_ids=[]
	if col2.count()!=0:
		for i in col2.find():
			dist_ips.append(i["Dist_IP"])
			dist_ids.append(i["Dist_ID"])
		return dist_ips,dist_ids
	else:
		return [],[]

def search_stream():
	global col3
	stream_ips=[]
	stream_ids=[]
	if col3.count()!=0:
		for i in col3.find():
			stream_ips.append(i["Stream_IP"])
			stream_ids.append(i["Stream_ID"])
		return stream_ips,stream_ids
	else:
		return [],[]


def search_users():
	global col5
	usernames=[]
	if col5.count()!=0:
		for i in col5.find():
			usernames.append(i["User"])
		return usernames
	else:
		return []

#On_message paho.mqtt



#All Celery functions


# @app.task 
# def ExistingServers():
#     
#     if col1.count()!=0:
#         for i in col1.find():
#             norigin[i["Origin_IP"]]=0
#     logger.info(norigin)
#     if col2.count()!=0:
#         for i in col2.find():
#             ndist[i["Dist_IP"]]=0
@app.task
def GetUsers():
	usernames=search_users()
	return {"topic":"lbsresponse/user/all","ddict":usernames}

@app.task
def GetOrigins():
	msg={}
	origin_ips, origin_ids=search_origin()
	for i in range(len(origin_ips)):
		msg[origin_ips[i]]=origin_ids[i]
	return {"topic":"lbsresponse/origin/all","ddict":msg}

@app.task
def GetDists():
	msg={}
	dist_ips, dist_ids=search_dist()
	for i in range(len(dist_ips)):
		msg[dist_ips[i]]=dist_ids[i]
	return {"topic":"lbsresponse/dist/all","ddict":msg}


@app.task
def GetStreams():
	msg={}
	stream_ips, stream_ids=search_stream()
	for i in range(len(stream_ips)):
		msg[stream_ips[i]]=stream_ids[i]
	return {"topic":"lbsresponse/stream/all","ddict":msg}

@app.task
def GetArchives():
	msg={}
	streamarch, jobarch=search_archives()
	for i in range(len(streamarch)):
		msg[streamarch[i]]=jobarch[i]
	return {"topic":"lbsresponse/archive/all","ddict":msg}



@app.task
def ReqAllStreams(req_all_streams):
	ip=req_all_streams[1]
	msg={}
	for i in col1.find():
		msg={"Origin_IP":i["Origin_IP"],"Stream_List":[]}
		for j in col3.find():
			if j["Origin_IP"]==i["Origin_IP"]:
				msg["Stream_List"].append({"Stream_ID":j["Stream_ID"],"Stream_IP":j["Stream_IP"]})
		logger.info( msg)
	for j in col2.find():
		msg={"Dist_IP":ip,"Stream_List":[]}
		for i in col3.find():
			if i["Dist_IP"]==j["Dist_IP"]:
				msg["Stream_List"].append({"Stream_ID":i["Stream_ID"],"Stream_IP":i["Stream_IP"]})
		logger.info( msg)
	return {"topic":"lb/request/allstreams","ddict":msg}


@app.task
def InsertOrigin(insert_origin):
	logger.info("Here origin")
	origin_ips,origin_ids=search_origin()
	msg=json.loads(insert_origin[1])
   # client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(msg))
	if msg["Origin_ID"] not in origin_ids and msg["Origin_IP"] not in origin_ips:
			col1.insert_one({"Origin_IP":msg["Origin_IP"],"Origin_ID":msg["Origin_ID"],"NClients":0})
			logger.info( "Origin Added----> ID:"+str(msg["Origin_ID"])+" IP:"+str(msg["Origin_IP"]))
			return {"topic":"lbsresponse/origin/add","ddict":True}
	else:
			logger.info( "Origin already present----> ID:"+str(msg["Origin_ID"])+" IP:"+str(msg["Origin_IP"]))
			return {"topic":"lbsresponse/origin/add","ddict":False}
	# norigin[msg["Origin_IP"]]=0
	# logger.info(norigin)


@app.task
def DeleteOrigin( delete_origin):
	origin_ips,origin_ids=search_origin()
	msg=json.loads(delete_origin[1])
	#client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(msg))
	if msg["Origin_ID"] not in origin_ids and msg["Origin_IP"] not in origin_ips:
		logger.info( "Origin not present----> ID:"+str(msg["Origin_ID"])+" IP:"+str(msg["Origin_IP"]))
		return {"topic":"lbsresponse/origin/del","ddict":False}
	else:
		
		col4.delete_many({"TO_IP":msg["Origin_IP"]})
		col4.delete_many({"FROM_IP":msg["Origin_IP"]})
		col1.delete_one({"Origin_IP":msg["Origin_IP"],"Origin_ID":msg["Origin_ID"]})
		col3.delete_many({"Origin_IP":msg["Origin_IP"]})
		logger.info( "Origin Deleted----> ID:"+str(msg["Origin_ID"])+" IP:"+str(msg["Origin_IP"]))
		return [{"topic":"lbsresponse/origin/del","ddict":True},{"topic":"origin/ffmpeg/killall","ddict":msg}]
	# del norigin[msg["Origin_IP"]]
	# logger.info(norigin)

@app.task
def OriginFfmpegStream( origin_ffmpeg_stream):
	msg=json.loads(origin_ffmpeg_stream[1])
	#client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(msg))
	logger.info( str(msg["Stream_ID"])+" stream has been started to origin "+ str(msg["TO_IP"]))
	col4.insert_one(msg)
	col3.update_one({"Stream_ID":msg["Stream_ID"]},{"$set":{"Origin_IP":msg["TO_IP"]}},upsert=True)
	time.sleep(0.1)
	return 0
	# col1.update_one({"Origin_IP":msg["TO_IP"]},{"$set":{"Stream_ID":msg["Stream_ID"]}},upsert=True)
	

@app.task
def InsertDist( insert_dist):
	dist_ips,dist_ids=search_dist()
	msg=json.loads(insert_dist[1])
	#client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(msg))
	if msg["Dist_ID"] not in dist_ids and msg["Dist_IP"] not in dist_ips:
		col2.insert_one({"Dist_IP":msg["Dist_IP"],"Dist_ID":msg["Dist_ID"],"NClients":0})
		logger.info( "Distribution Added----> ID:"+str(msg["Dist_ID"])+" IP:"+str(msg["Dist_IP"]))
		return {"topic":"lbsresponse/dist/add","ddict":True}
	else:
		logger.info( "Distribution already present----> ID:"+str(msg["Dist_ID"])+" IP:"+str(msg["Dist_IP"]))
		return {"topic":"lbsresponse/dist/add","ddict":False}
	# ndist[msg["Dist_IP"]]=0
	# logger.info(ndist)


@app.task
def DeleteDist(delete_dist):
	dist_ips,dist_ids=search_dist()
	msg=json.loads(delete_dist[1])
	killist=[]
	#client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(msg))
	if msg["Dist_ID"] not in dist_ids and msg["Dist_IP"] not in dist_ips:
		logger.info( "Distribution not present----> ID:"+str(msg["Dist_ID"])+" IP:"+str(msg["Dist_IP"]))
		return {"topic":"lbsresponse/dist/del","ddict":False}
	else:
		for i in col4.find():
			if i["TO_IP"]==msg["Dist_IP"]:
				killist.append({"Origin_IP":i["FROM_IP"],"CMD":i["CMD"]})
		col4.delete_many({"TO_IP":msg["Dist_IP"]})
		col2.delete_one({"Dist_IP":msg["Dist_IP"],"Dist_ID":msg["Dist_ID"]})
		col3.update_many({"Dist_IP":msg["Dist_IP"]},{"$set":{"Dist_IP":""}},upsert=True)
		logger.info( "Distribution Deleted----> ID:"+str(msg["Dist_ID"])+" IP:"+str(msg["Dist_IP"]))
		return [{"topic":"lbsresponse/dist/del","ddict":True},{"topic":"origin/ffmpeg/kill","ddict":killist}]
	#     del ndist[msg["Dist_IP"]]
	# logger.info(ndist)


@app.task
def InsertStream(insert_stream):
	if col1.count()==0:
		logger.info( "No Origin Server Present")
		return 0
	else:
		sorigin=col1.find_one(sort=[("NClients", 1)])["Origin_IP"]

		stream_ips,stream_ids=search_stream()
		msg=json.loads(insert_stream[1])
		# client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(msg))
		if msg["Stream_ID"] not in stream_ids:
			col3.insert_one({"Stream_IP":msg["Stream_IP"],"Stream_ID":msg["Stream_ID"],"Origin_IP":"","Dist_IP":""})
			logger.info( str(sorigin)+" "+str(msg["Stream_ID"])+" "+str(msg["Stream_IP"]))
			ddict={"Origin_IP":sorigin,"Stream_ID":msg["Stream_ID"],"Stream_IP":msg["Stream_IP"]}
			logger.info( "Sending Dict")
			logger.info( "Stream Added----> ID:"+str(msg["Stream_ID"])+" IP:"+str(msg["Stream_IP"]))
			return [{"topic":"lbsresponse/stream/add","ddict":True},{"topic":"origin/ffmpeg/stream/spawn","ddict":ddict},{"topic":"origin/ffmpeg/stream/stat/spawn","ddict":ddict}]
		else:
			logger.info( "Stream already present----> ID:"+str(msg["Stream_ID"])+" IP:"+str(msg["Stream_IP"]))
			return {"topic":"lbsresponse/stream/add","ddict":False}

@app.task
def DeleteStream( delete_stream):
	killist=[]
	stream_ips,stream_ids=search_stream()
	msg=json.loads(delete_stream[1])
	logger.info( msg)
	#client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(msg))
	if msg["Stream_ID"] not in stream_ids and msg["Stream_IP"] not in stream_ips:
		logger.info( "Stream not present----> ID:"+str(msg["Stream_ID"])+" IP:"+str(msg["Stream_IP"]))
		return {"topic":"lbsresponse/stream/del","ddict":False}
	else:
		norigin=[]
		for j in col1.find():
				norigin.append(j["Origin_IP"])
		for i in col4.find():
			if str(i["Stream_ID"])==msg["Stream_ID"]:
				if i["TO_IP"] in norigin:
					killist.append({"Origin_IP":i["TO_IP"],"CMD":i["CMD"],"Stream_ID":i["Stream_ID"],"Dist_IP":"","RTSP_CMD":i["RTSP_CMD"]})
				elif i["FROM_IP"] in norigin:
					killist.append({"Origin_IP":i["TO_IP"],"CMD":i["CMD"],"Stream_ID":i["Stream_ID"],"Dist_IP":i["TO_IP"],"RTSP_CMD":i["RTSP_CMD"]})
		col3.delete_one({"Stream_IP":msg["Stream_IP"],"Stream_ID":msg["Stream_ID"]})
		col4.delete_many({"Stream_ID":msg["Stream_ID"]})
		# col1.update_one({"Stream_ID":msg["Stream_ID"]},{"$set":{"Stream_ID":""}},upsert=True)
		logger.info( "Stream Deleted----> ID:"+str(msg["Stream_ID"])+" IP:"+str(msg["Stream_IP"]))
		return [{"topic":"lbsresponse/stream/del","ddict":True},{"topic":"origin/ffmpeg/kill","ddict":killist}]

@app.task
def RequestStream( reqstream):
	logger.info("Here")
	msg=json.loads(reqstream[1])
	stream_id=msg["Stream_ID"]
	logger.info( stream_id)
	alreadypushedflag=0
   # client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(msg))
   	if col3.count()!=0:
   		stream_ips, stream_ids= search_stream()
   		if stream_id not in stream_ids:
   			return[{"topic":"lbsresponse/rtmp","ddict":False}]
		if col2.count()!=0:
			ndist={}
			for j in col2.find():
				ndist[j["Dist_IP"]]=j["NClients"]
			for i in col4.find():
				if (str(i["Stream_ID"])==stream_id) and (str(i["TO_IP"]) in ndist.keys()):
					logger.info( "Should come here if already pushed to a dist")
					return {"topic":"lbsresponse/rtmp","ddict":"RTMP: "+str(i["CMD"].split()[-2])+" RTSP: "+str(i["RTSP_CMD"].split()[-2])+" HLS: http://"+str(i["TO_IP"]+":8080/hls/"+str(i["Stream_ID"]+".m3u8"))}
					alreadypushedflag=0
					break
				else:
					alreadypushedflag=1
			if alreadypushedflag==1:
				sdist=col2.find_one(sort=[("NClients", 1)])["Dist_IP"]
				sorigin=col1.find_one(sort=[("NClients", 1)])["Origin_IP"]
				logger.info( str(sorigin)+" "+str(sdist)+" "+str(stream_id))
				stream_ip=col3.find_one({"Stream_ID":stream_id})["Stream_IP"]
				distdict={"Origin_IP":sorigin,"Dist_IP":sdist,"Stream_ID":stream_id,"Stream_IP":stream_ip}
				return [{"topic":"lbsresponse/rtmp","ddict":"RTMP: "+"rtmp://"+str(sdist)+":1935/dynamic/"+str(stream_id)+" RTSP: rtsp://"+str(sdist)+":80/dynamic/"+str(stream_id)+" HLS: http://"+str(i["TO_IP"]+":8080/hls/"+str(i["Stream_ID"]+".m3u8"))},{"topic":"origin/ffmpeg/dist/spawn","ddict":distdict},{"topic":"dist/ffmpeg/stream/stat/spawn","ddict":distdict}]
		else:
			for i in col4.find():
				if i["Stream_ID"]==stream_id:
					return {"topic":"lbsresponse/rtmp","ddict":"RTMP: "+str(i["CMD"].split()[-2])+" RTSP: "+str(i["RTSP_CMD"].split()[-2])+" HLS: http://"+str(i["TO_IP"]+":8080/hls/"+str(i["Stream_ID"]+".m3u8"))}
	else:
		return[{"topic":"lbsresponse/rtmp","ddict":False}]

@app.task()
def OriginFfmpegDist( origin_ffmpeg_dist):
	msg=json.loads(origin_ffmpeg_dist[1])
	logger.info( msg)
	#client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(msg))
	logger.info( msg["Stream_ID"]+" stream push has been started from origin "+ msg["FROM_IP"]+" to distribution "+msg["TO_IP"])
	col4.insert_one({"CMD":msg["CMD"],"TO_IP":msg["TO_IP"],"FROM_IP":msg["FROM_IP"],"Stream_ID":msg["Stream_ID"],"RTSP_CMD": msg["RTSP_CMD"]})
	col3.update_one({"Stream_ID":msg["Stream_ID"],"Origin_IP":msg["FROM_IP"]},{"$set":{"Dist_IP":msg["TO_IP"]}},upsert=True)
	logger.info( str(msg["CMD"].split()[-2]))
	time.sleep(0.1)
	return {"topic":"lbsresponse/rtmp","ddict":str(msg["CMD"].split()[-2])}


@app.task
def OriginFfmpegRespawn( origin_ffmpeg_respawn):
	msg=json.loads(origin_ffmpeg_respawn[1])
	logger.info( str(msg)+" should come here only when missing becomes active")
	return {"topic":"origin/ffmpeg/respawn","ddict":msg}

@app.task
def ArchiveAdd( archive_stream_add):
	msg=json.loads(archive_stream_add[1])
	logger.info( msg)
	msg["Stream_IP"] = col3.find_one({"Stream_ID":msg["Stream_ID"]})["Stream_IP"]
	msg["Origin_IP"]= col3.find_one({"Stream_ID":msg["Stream_ID"]})["Origin_IP"]

	archivedstreams, archivedjobs=search_archives()
	if msg["job_id"] not in archivedjobs:
		col6.insert_one({"Job_ID": msg["job_id"],"Stream_ID": msg["Stream_ID"]})
		return [{"topic":"lbsresponse/archive/add","ddict":True},{"topic":"origin/ffmpeg/archive/add","ddict":msg}]
		logger.info( str(msg)+" archiving this......")
	else:
		return {"topic":"lbsresponse/archive/add","ddict":False}

@app.task
def ArchiveDel( archive_stream_del):
	msg=json.loads(archive_stream_del[1])
	logger.info( msg)
	msg["Stream_IP"] = col3.find_one({"Stream_ID":msg["Stream_ID"]})["Stream_IP"]
	msg["Origin_IP"]= col3.find_one({"Stream_ID":msg["Stream_ID"]})["Origin_IP"]
	archivedstreams, archivedjobs=search_archives()
	if msg["job_id"] not in archivedjobs:
		return {"topic":"lbsresponse/archive/del","ddict":False}
	else:
		logger.info( str(msg)+"  deleting this archive......")
		col6.delete_one({"Job_ID": msg["job_id"]})
		return [{"topic":"lbsresponse/archive/del","ddict":True},{"topic":"origin/ffmpeg/archive/delete","ddict":msg}]

@app.task
def OriginFFmpegDistRespawn( origin_ffmpeg_dist_respawn):
	msg=json.loads(origin_ffmpeg_dist_respawn[1])
	logger.info( str(msg)+" should come here only when missing becomes active")
	msg["Origin_IP"]=col3.find_one({"Stream_IP": msg["Stream_IP"],"Stream_ID": msg["Stream_ID"], "Dist_IP": msg["Dist_IP"]})["Origin_IP"]
	return {"topic":"origin/ffmpeg/dist/respawn","ddict":msg}

@app.task
def OriginStat(msg):
	msg=json.loads(msg)
	for i in col1.find():
			if msg["Origin_IP"]==i["Origin_IP"]:
				col1.update({"Origin_IP":msg["Origin_IP"]},{"$set":{"NClients":int(msg["NClients"])}},upsert=True)


@app.task
def DistStat(msg):
	msg=json.loads(msg)
	for i in col2.find():
			if msg["Dist_IP"]==i["Dist_IP"]:
				col2.update({"Dist_IP":msg["Dist_IP"]},{"$set":{"NClients":int(msg["NClients"])}},upsert=True)


@app.task
def AddUser(user_add):
	msg=json.loads(user_add[1])
	if col5.count()==0:
		col5.insert_one({"User":msg["User"],"Password":msg["Password"]})
		return {"topic":"lbsresponse/user/add","ddict":True}
	else:
		usernames=search_users()
		if msg["User"] not in usernames:
			col5.insert_one({"User":msg["User"],"Password":msg["Password"]})
			return {"topic":"lbsresponse/user/add","ddict":True}
		else:
			return {"topic":"lbsresponse/user/add","ddict":False}


@app.task
def DelUser(user_del):
	msg=json.loads(user_del[1])
	if col5.count()==0:
		return{"topic":"lbsresponse/user/del","ddict":False}
	else:
		usernames=search_users()
		if msg["User"] not in usernames:
			return{"topic":"lbsresponse/user/del","ddict":False}
		else:
			count=0;
			for i in col5.find():
				if i["User"]==msg["User"]:
					if i["Password"]==msg["Password"]:
						col5.delete_one({"User":msg["User"]})
						return {"topic":"lbsresponse/user/del","ddict":True}
					else:
						return {"topic":"lbsresponse/user/del","ddict":False}
				   

@app.task
def VerifyUser(verify_user):
	print("Verifying ")
	print(verify_user)
	msg=json.loads(verify_user[1])
	if col5.count!=0:
		for i in col5.find():
			if i["User"]==msg["User"]:
				if i["Password"]==msg["Password"]:
					return {"topic":"lbsresponse/verified","ddict":True}
	return {"topic":"lbsresponse/verified","ddict":False}



		

	   







