# Needs to update the database with all the details of everythign that's going on. Plus handle all the other important requests.
import paho.mqtt.client as mqtt
import pymongo
import os
import sys
import signal
import time
import pymongo
import datetime

# LBS Params
sorigin=""
sdist=""
norigin={}
ndist={}

#All Flags
insert_origin=[0,""]
delete_origin=[0,""]
origin_ffmpeg_stream=[0,""]
origin_ffmpeg_dist=[0,""]
insert_dist=[0,""]
delete_dist=[0,""]
insert_stream=[0,""]
delete_stream=[0,""]
reqstream=[0,""]

#Update Origin, Dists and Streams
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

#On_message paho.mqtt
def on_message(client, userdata, message):
    global norigin,ndist,insert_dist,insert_origin,insert_stream,delete_stream,delete_origin,delete_dist,reqstream,origin_ffmpeg_dist,origin_ffmpeg_stream
    msg=str(message.payload.decode("utf-8"))
    topic=str(message.topic.decode("utf-8"))
    if topic=="origin/add":
    	insert_origin[0]=1
        insert_origin[1]=msg
    elif topic=="db/origin/ffmpeg/stream/spawn":
    	origin_ffmpeg_stream[0]=1
        origin_ffmpeg_stream[1]=msg
    elif topic=="origin/delete":
    	delete_origin[0]=1
        delete_origin[1]=msg
    elif topic=="dist/add":
    	insert_dist[0]=1
        insert_dist[1]=msg
    elif topic=="dist/delete":
    	delete_dist[0]=1
        delete_dist[1]=msg
    elif topic=="stream/add":
        insert_stream[0]=1
        insert_stream[1]=msg 	
    elif topic=="stream/delete":
        delete_stream[0]=1
        delete_stream[1]=msg
    elif topic=="stream/request":
    	reqstream[0]=1
        reqstream[1]=msg
    elif topic=="db/origin/ffmepg/dist/spawn":
        origin_ffmpeg_dist[0]=1
        origin_ffmpeg_dist[1]=msg
    elif topic=="origin/stat":
        ip=msg.split()[0]
        clients=msg.split()[1]
        if ip in norigin.keys():
            norigin[ip]=clients
    elif topic=="dist/stat":
        ip=msg.split()[0]
        clients=msg.split()[1]
        if ip in ndist.keys():
            ndist[ip]=clients
     

if __name__=="__main__":
    mongoclient=pymongo.MongoClient('mongodb://localhost:27017/')
    mydb=mongoclient["ALL_Streams"]
    col1=mydb["Origin_Servers"]
    col2=mydb["Distribution_Servers"]
    col3=mydb["Streams"]
    col4=mydb["Ffmpeg_Procs"]
    broker_address="localhost"
    client = mqtt.Client("loadbalancer") 
    client.connect(broker_address)
    client.on_message=on_message #connect to broker
    client.loop_start()
    client.subscribe([("origin/stat",0),("dist/stat",0),("origin/add",0),("origin/delete",0),("dist/add",0),("dist/delete",0),("stream/add",0),("stream/delete",0),("stream/request",0),("db/origin/ffmpeg/dist/spawn",0),("db/origin/ffmpeg/stream/spawn",0)])
    if col1.count()!=0:
        for i in col1.find():
            norigin[i["Origin_IP"]]=0
    if col2.count()!=0:
        for i in col2.find():
            norigin[i["Dist_IP"]]=0
    while(True):
        #Always setting Load Balancer Params
        if len(ndist)!=0:
            sdist=str(min(ndist.items(), key=lambda x: x[1])[0])
        if len(norigin)!=0:
            sorigin=str(min(norigin.items(), key=lambda x: x[1])[0])
        if insert_origin[0]: 
            print "Inserting Origin"
            origin_ips,origin_ids=search_origin()
            msg=insert_origin[1].split()
            client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(" ".join(msg)))
            if msg[0] not in origin_ids and msg[1] not in origin_ips:
                    col1.insert_one({"Origin_IP":msg[1],"Origin_ID":msg[0],"Stream_ID":""})
                    print "Origin Added----> ID:"+str(msg[0])+" IP:"+str(msg[1])
            else:
                    print "Origin already present----> ID:"+str(msg[0])+" IP:"+str(msg[1])
            norigin[msg[1]]=0
            insert_origin=[0,""]
        elif delete_origin[0]:
            origin_ips,origin_ids=search_origin()
            msg=delete_origin[1].split()
            client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(" ".join(msg)))
            if msg[0] not in origin_ids and msg[1] not in origin_ips:
                print "Origin not present----> ID:"+str(msg[0])+" IP:"+str(msg[1])
            else:
                client.publish("origin/ffmpeg/killall",str(msg[1]))
                col4.delete_many({"TO_IP":msg[1]})
                col4.delete_many({"FROM_IP":msg[1]})
                col1.delete_one({"Origin_IP":msg[1],"Origin_ID":msg[0]})
                col3.delete_many("Origin_IP":msg[1])
                print "Origin Deleted----> ID:"+str(msg[0])+" IP:"+str(msg[1])
            del norigin[msg[1]]
            delete_origin=[0,""]
        elif origin_ffmpeg_stream[0]:
            msg=origin_ffmpeg_stream[1].split()
            client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(" ".join(msg)))
            cmd=" ".join(msg[:9])
            PID=msg[9]
            TO_IP=msg[10]
            FROM_IP=msg[12]
            Stream_ID=msg[11]
            col4.insert_one({"CMD":cmd,"PID":PID,"TO_IP":TO_IP,"FROM_IP":FROM_IP,"Stream_ID":Stream_ID})
            col3.update_one({"Stream_IP":msg[12]},{"$set":{"Origin_IP":msg[10]}},upsert=True)
            col1.update_one({"Origin_IP":msg[10]},{"$set":{"Stream_ID":msg[11]}},upsert=True)
            origin_ffmpeg_stream=[0,""]
        elif insert_dist[0]:
            dist_ips,dist_ids=search_dist()
            msg=insert_dist[1].split()
            client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(" ".join(msg)))
            if msg[0] not in dist_ids and msg[1] not in dist_ips:
                col2.insert_one({"Dist_IP":msg[1],"Dist_ID":msg[0],"Origin_ID":""})
                print "Distribution Added----> ID:"+str(msg[0])+" IP:"+str(msg[1])
            else:
                print "Distribution already present----> ID:"+str(msg[0])+" IP:"+str(msg[1])
            ndist[msg[1]]=0
            insert_dist=[0,""]
        elif delete_dist[0]:
            dist_ips,dist_ids=search_dist()
            msg=delete_dist[1].split()
            client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(" ".join(msg)))
            if msg[0] not in dist_ids and msg[1] not in dist_ips:
                print "Distribution not present----> ID:"+str(msg[0])+" IP:"+str(msg[1])
            else:
                for i in col4.find():
                    if i["TO_IP"]==msg[1]:
                        client.publish("origin/ffmpeg/kill",str(i["PID"]))
                col4.delete_many({"TO_IP":msg[1]})
                col2.delete_one({"Dist_IP":msg[1],"Dist_ID":msg[0]})
                print "Distribution Deleted----> ID:"+str(msg[0])+" IP:"+str(msg[1])
            del ndist[msg[1]]
            delete_dist=[0,""]
        elif insert_stream[0]:
            stream_ips,stream_ids=search_stream()
            msg=insert_stream[1].split()
            client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(" ".join(msg)))
            if msg[0] not in stream_ids and msg[1] not in stream_ips:
                print str(sorigin)+" "+str(msg[0])+" "+str(msg[1])
                client.publish("origin/ffmpeg/stream/spawn",str(sorigin)+" "+str(msg[0])+" "+str(msg[1]))
                col3.insert_one({"Stream_IP":msg[1],"Stream_ID":msg[0],"Origin_IP":""})
                print "Stream Added----> ID:"+str(msg[0])+" IP:"+str(msg[1])
            else:
                print "Stream already present----> ID:"+str(msg[0])+" IP:"+str(msg[1])
            insert_stream=[0,""]
        elif delete_stream[0]:
            print "Came here"
            stream_ips,stream_ids=search_stream()
            msg=delete_stream[1].split()
            client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(" ".join(msg)))
            if msg[0] not in stream_ids and msg[1] not in stream_ips:
                print "Stream not present----> ID:"+str(msg[0])+" IP:"+str(msg[1])
            else:
                for i in col4.find():
                    if i["FROM_IP"]==msg[1]:
                        client.publish("origin/ffmpeg/kill",str(i["TO_IP"])+" "+str(i["PID"]))
                col3.delete_one({"Stream_IP":msg[1],"Stream_ID":msg[0]})
                col4.delete_many({"FROM_IP":msg[1]})
                col1.update_one({"Stream_ID":msg[0]},{"$set":{"Stream_ID":""}},upsert=True)
                print "Stream Deleted----> ID:"+str(msg[0])+" IP:"+str(msg[1])
            delete_stream=[0,""]
        elif reqstream[0]:
            msg=reqstream[1].split()
            stream_id=msg[1]
            client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(" ".join(msg)))
            if len(ndist)!=0:
                client.publish("origin/ffmpeg/dist/spawn",str(sorigin)+" "+str(sdist)+" "+str(stream_id))
            else:
                for i in col4.find():
                    if i["Stream_ID"]==stream_id:
                        client.publish("lbsresponse/rtmp",str(i["CMD"].split()[-1]))

        elif origin_ffmpeg_dist[0]:
            msg=origin_ffmpeg_dist[1].split()
            client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(" ".join(msg)))
            cmd=" ".join(msg[:9])
            PID=msg[9]
            TO_IP=msg[10]
            FROM_IP=msg[11]
            Stream_ID=msg[12]
            col4.insert_one({"CMD":cmd,"PID":PID,"TO_IP":TO_IP,"FROM_IP":FROM_IP,"Stream_ID":Stream_ID})
            client.publish("lbsresponse/rtmp",str(cmd.split()[-1]))






