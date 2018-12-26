# Needs to update the database with all the details of everythign that's going on. Plus handle all the other important requests.
import paho.mqtt.client as mqtt
import pymongo
import os
import sys
import signal
from flask import Flask , request
import time
import pymongo
import datetime
sorigin=""
sdist=""
def search_origin():
	origin_ips=[]
    origin_ids=[]
    for i in col1.find():
    		origin_ips.append(i["Origin_IP"])
    		origin_ids.append(i["Origin_ID"])
    return origin_ips,origin_ids

def search_dist():
	dist_ips=[]
    dist_ids=[]
    for i in col2.find():
    		dist_ips.append(i["Dist_IP"])
    		dist_ids.append(i["Dist_ID"])
    return dist_ips,dist_ids

def search_stream():
	stream_ips=[]
    stream_ids=[]
    for i in col3.find():
    		stream_ips.append(i["Stream_IP"])
    		stream_ids.append(i["Stream_ID"])
    return stream_ips,stream_ids


def on_message(client, userdata, message):
    global sorigin,sdist
    msg=str(message.payload.decode("utf-8")).split()
    topic=str(message.topic.decode("utf-8"))
    client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Topic: "+str(str(topic)+" Message: "+str(message.payload.decode("utf-8"))
    if topic=="origin/add":
    	origin_ips,origin_ids=search_origin()
    	if msg[0] not in origin_ids and msg[1] not in origin_ips:
    			col1.insert_one({"Origin_IP":msg[1],"Origin_ID":msg[0],"Stream_ID":""})
    			print "Origin Added----> ID:"+str(msg[0])+" IP:"+str(msg[1])
    	else:
    		print "Origin already present----> ID:"+str(msg[0])+" IP:"+str(msg[1])
    elif topic=="lbs/origin":
        sorigin=msg[0]
    elif topic=="lbs/dist":
        sdist=msg[0]
    elif topic=="db/origin/ffmpeg/stream/spawn":
    	cmd=" ".join(msg[:9])
    	PID=msg[9]
    	TO_IP=msg[10]
    	FROM_IP=msg[11]
    	Stream_ID=msg[12]
    	col4.insert_one({"CMD":cmd,"PID":PID,"TO_IP":TO_IP,"FROM_IP":FROM_IP,"Stream_ID":,Stream_ID})
    	col3.update_one({"Stream_IP":msg[11]},{"$set",{"Origin_IP":msg[12]}},upsert=True)
    	col1.update_one({"Origin_IP":msg[12]},{"$set",{"STREAM_ID":msg[11]}},upsert=True)
    elif topic=="origin/delete"
    	origin_ips,origin_ids=search_origin()
    	if msg[0] not in origin_ids and msg[1] not in origin_ips:
    		print "Origin not present----> ID:"+str(msg[0])+" IP:"+str(msg[1])
    	else:
    		client.publish("origin/ffmpeg/killall",str(msg[1])+" "+str(i["Stream_IP"])+" "+str(i["Stream_ID"]))
    		col4.delete_many({"TO_IP":msg[1]})
    		col4.delete_many({"FROM_IP":msg[1]})
    		col1.delete_one({"Origin_IP":msg[1],"Origin_ID":msg[0]})
    		col3.update_one({"Origin_IP":msg[1]},{"$set",{"Origin_IP":""}},upsert=True)
    		print "Origin Deleted----> ID:"+str(msg[0])+" IP:"+str(msg[1])
    elif topic=="dist/add":
    	dist_ips,dist_ids=search_dist()
    	if msg[0] not in dist_ids and msg[1] not in dist_ips:
    		col2.insert_one({"Dist_IP":msg[1],"Dist_ID":msg[0],"Origin_ID":""})
    		print "Distribution Added----> ID:"+str(msg[0])+" IP:"+str(msg[1])
    	else:
    		print "Distribution already present----> ID:"+str(msg[0])+" IP:"+str(msg[1])
    elif topic=="dist/delete":
    	dist_ips,dist_ids=search_dist()
    	if msg[0] not in dist_ids and msg[1] not in dist_ips:
    		print "Distribution not present----> ID:"+str(msg[0])+" IP:"+str(msg[1])
    	else:
    		for i in col4.find():
    			if i["TO_IP"]==msg[1]:
    				client.publish("origin/ffmpeg/kill",str(i["PID"]))
    		col4.delete_many({"TO_IP":msg[1]})
    		col2.delete_one({"Dist_IP":msg[1],"Dist_ID":msg[0]})
    		print "Distribution Deleted----> ID:"+str(msg[0])+" IP:"+str(msg[1])
    elif topic=="stream/add":
    	stream_ips,stream_ids=search_stream()
    	if msg[0] not in stream_ids and msg[1] not in stream_ips:
    		client.publish("origin/ffmpeg/stream/spawn",str(sorigin)+" "+str(msg[0])+" "+str(msg[1]))
    		col3.insert_one({"Stream_IP":msg[1],"Stream_ID":msg[0],"Origin_IP":""})
    		print "Stream Added----> ID:"+str(msg[0])+" IP:"+str(msg[1])
    	else:
    		print "Stream already present----> ID:"+str(msg[0])+" IP:"+str(msg[1])
    elif topic=="stream/delete":
    	stream_ips,stream_ids=search_stream()
    	if msg[0] not in stream_ids and msg[1] not in stream_ips:
    		print "Stream not present----> ID:"+str(msg[0])+" IP:"+str(msg[1])
    	else:
    		for i in col4.find():
    			if i["FROM_IP"]==msg[1]:
    				client.publish("origin/ffmpeg/kill",str(i["PID"]))
    		col3.delete_one({"Stream_IP":msg[1],"Stream_ID":msg[0]})
    		col4.delete_many({"TO_IP":msg[1]})
    		col1.update_one({"Stream_ID":msg[0]},{"$set",{"Stream_ID":""}},upsert=True)
    		print "Stream Deleted----> ID:"+str(msg[0])+" IP:"+str(msg[1])
    elif topic=="stream/request":
    	stream_id=msg[1]
        client.publish("origin/ffmpeg/dist/spawn",str(sorigin)+" "+str(sdist)+" "+str(stream_id))
    elif topic=="db/origin/ffmepg/dist/spawn":
        cmd=" ".join(msg[:9])
        PID=msg[9]
        TO_IP=msg[10]
        FROM_IP=msg[11]
        Stream_ID=msg[12]
        col4.insert_one({"CMD":cmd,"PID":PID,"TO_IP":TO_IP,"FROM_IP":FROM_IP,"Stream_ID":,Stream_ID})




mongoclient=pymongo.MongoClient('mongodb://localhost:27017/')
mydb=mongoclient["ALL_Streams"]
col1=mydb["Origin_Servers"]
col2=mydb["Distribution_Servers"]
col3=mydb["Streams"]
col4=mydb["Ffmpeg_Procs"]
broker_address="localhost"
client = mqtt.Client("P2") 
client.connect(broker_address)
client.on_message=on_message #connect to broker
client.loop_start()
client.subscribe([("origin/add",0),("origin/delete",0),("dist/add",0),("dist/delete",0),("stream/add",0),("stream/delete",0),("stream/request",0),("db/origin/ffmpeg/dist/spawn",0)("db/origin/ffmpeg/stream/spawn",0),("lbs/origin",0),("lbs/dist",0)])
client.loop_forever()
