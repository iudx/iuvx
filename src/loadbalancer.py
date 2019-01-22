# Needs to update the database with all the details of everythign that's going on. Plus handle all the other important requests.
import paho.mqtt.client as mqtt
import pymongo
import os
import sys
import signal
import time
import pymongo
import datetime
from MQTTPubSub import MQTTPubSub

import json

# LBS Params
sorigin=""
sdist=""
norigin={}
ndist={}
# originstreams={}
# diststreams={}

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
origin_ffmpeg_respawn=[0,""]
archive_stream_add=[0,""]
archive_stream_del=[0,""]
req_all_streams=[0,""]
origin_ffmpeg_dist_respawn=[0,""]

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
    global origin_ffmpeg_dist_respawn, archive_stream_add,archive_stream_del,req_all_streams,norigin,origin_ffmpeg_respawn,ndist,insert_dist,insert_origin,insert_stream,delete_stream,delete_origin,delete_dist,reqstream,origin_ffmpeg_dist,origin_ffmpeg_stream
    msg=str(message.payload.decode("utf-8"))
    topic=str(message.topic.decode("utf-8"))
    print topic
    if topic=="origin/add":
        print topic+" "+msg
    	insert_origin[0]=1
        insert_origin[1]=msg
    elif topic=="db/origin/ffmpeg/stream/spawn":
    	origin_ffmpeg_stream[0]=1
        origin_ffmpeg_stream[1]=msg
        print topic+" "+msg
    elif topic=="origin/delete":
    	delete_origin[0]=1
        delete_origin[1]=msg
        print topic+" "+msg
    elif topic=="dist/add":
    	insert_dist[0]=1
        insert_dist[1]=msg
        print topic+" "+msg
    elif topic=="dist/delete":
    	delete_dist[0]=1
        delete_dist[1]=msg
        print topic+" "+msg
    elif topic=="stream/add":
        insert_stream[0]=1
        insert_stream[1]=msg
        print topic+" "+msg 	
    elif topic=="stream/delete":
        delete_stream[0]=1
        delete_stream[1]=msg
        print topic+" "+msg
    elif topic=="stream/request":
    	reqstream[0]=1
        reqstream[1]=msg
        print topic+" "+msg
    elif topic=="db/origin/ffmpeg/dist/spawn":
        origin_ffmpeg_dist[0]=1
        origin_ffmpeg_dist[1]=msg
        print topic+" "+msg
    elif topic=="origin/stat":
        ip=msg.split()[0]
        clients=msg.split()[1]
        if ip in norigin.keys():
            norigin[ip]=clients
            # originstreams[ip]=streams
        # print topic+" "+msg
    elif topic=="dist/stat":
        ip=msg.split()[0]
        clients=msg.split()[1]
        if ip in ndist.keys():
            ndist[ip]=clients
    elif topic=="db/origin/ffmpeg/respawn":
        origin_ffmpeg_respawn[0]=1
        origin_ffmpeg_respawn[1]=msg
    elif topic=="db/dist/ffmpeg/respawn":
        origin_ffmpeg_dist_respawn[0]=1
        origin_ffmpeg_dist_respawn[1]=msg
    elif topic=="archive/add":
        archive_stream_add[0]=1
        archive_stream_add[1]=msg
    elif topic=="archive/delete":
        archive_stream_del[0]=1
        archive_stream_del[1]=msg
    elif topic=="request/allstreams":
        req_all_streams[0]=1
        req_all_streams[1]=msg
        # print topic+" "+msg
            # diststreams[ip]=streams
     
#MQTT Params

mqttServerParams = {}
mqttServerParams["url"] = "127.0.0.1"
mqttServerParams["port"] = 1883
mqttServerParams["timeout"] = 60
mqttServerParams["topic"] = [("db/dist/ffmpeg/respawn",0),("archive/delete",0),("request/allstreams",0),("archive/add",0),("db/origin/ffmpeg/respawn",0),("origin/stat",0),("dist/stat",0),("origin/add",0),("origin/delete",0),("dist/add",0),("dist/delete",0),("stream/add",0),("stream/delete",0),("stream/request",0),("db/origin/ffmpeg/dist/spawn",0),("db/origin/ffmpeg/stream/spawn",0)]
mqttServerParams["onMessage"] = on_message
client = MQTTPubSub(mqttServerParams)





if __name__=="__main__":
    mongoclient=pymongo.MongoClient('mongodb://localhost:27017/')
    mydb=mongoclient["ALL_Streams"]
    col1=mydb["Origin_Servers"]
    col2=mydb["Distribution_Servers"]
    col3=mydb["Streams"]
    col4=mydb["Ffmpeg_Procs"]
    client.run()
    if col1.count()!=0:
        for i in col1.find():
            norigin[i["Origin_IP"]]=0
    if col2.count()!=0:
        for i in col2.find():
            ndist[i["Dist_IP"]]=0
    while(True):
        msg={}
        #Always setting Load Balancer Params
        if req_all_streams[0]:
            ip=req_all_streams[1]
            if ip in norigin.keys():
                msg={"Origin_IP":ip,"Stream_List":[]}
                for i in col3.find():
                    if i["Origin_IP"]==ip:
                        msg["Stream_List"].append({"Stream_ID":i["Stream_ID"],"Stream_IP":i["Stream_IP"]})
                print msg
            elif ip in ndist.keys():
                msg={"Dist_IP":ip,"Stream_List":[]}
                for i in col3.find():
                    if i["Dist_IP"]==ip:
                        msg["Stream_List"].append({"Stream_ID":i["Stream_ID"],"Stream_IP":i["Stream_IP"]})
                print msg
            client.publish("lb/request/allstreams",json.dumps(msg))
            req_all_streams[0]=0
        elif insert_origin[0]:
            origin_ips,origin_ids=search_origin()
            msg=json.loads(insert_origin[1])
            client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(msg))
            if msg["Origin_ID"] not in origin_ids and msg["Origin_IP"] not in origin_ips:
                    col1.insert_one({"Origin_IP":msg["Origin_IP"],"Origin_ID":msg["Origin_ID"]})
                    print "Origin Added----> ID:"+str(msg["Origin_ID"])+" IP:"+str(msg["Origin_IP"])
            else:
                    print "Origin already present----> ID:"+str(msg["Origin_ID"])+" IP:"+str(msg["Origin_IP"])
            norigin[msg["Origin_IP"]]=0
            insert_origin=[0,""]
        elif delete_origin[0]:
            origin_ips,origin_ids=search_origin()
            msg=json.loads(delete_origin[1])
            client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(msg))
            if msg["Origin_ID"] not in origin_ids and msg["Origin_IP"] not in origin_ips:
                print "Origin not present----> ID:"+str(msg["Origin_ID"])+" IP:"+str(msg["Origin_IP"])
            else:
                client.publish("origin/ffmpeg/killall",json.dumps(msg))
                col4.delete_many({"TO_IP":msg["Origin_IP"]})
                col4.delete_many({"FROM_IP":msg["Origin_IP"]})
                col1.delete_one({"Origin_IP":msg["Origin_IP"],"Origin_ID":msg["Origin_ID"]})
                col3.delete_many({"Origin_IP":msg["Origin_IP"]})
                print "Origin Deleted----> ID:"+str(msg["Origin_ID"])+" IP:"+str(msg["Origin_IP"])
            del norigin[msg["Origin_IP"]]
            delete_origin=[0,""]
        elif origin_ffmpeg_stream[0]:
            msg=json.loads(origin_ffmpeg_stream[1])
            client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(msg))
            print str(msg["Stream_ID"])+" stream has been started to origin "+ str(msg["TO_IP"])
            col4.insert_one(msg)
            col3.update_one({"Stream_IP":msg["FROM_IP"]},{"$set":{"Origin_IP":msg["TO_IP"]}},upsert=True)
            # col1.update_one({"Origin_IP":msg["TO_IP"]},{"$set":{"Stream_ID":msg["Stream_ID"]}},upsert=True)
            origin_ffmpeg_stream=[0,""]
        elif insert_dist[0]:
            dist_ips,dist_ids=search_dist()
            msg=json.loads(insert_dist[1])
            client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(msg))
            if msg["Dist_ID"] not in dist_ids and msg["Dist_IP"] not in dist_ips:
                col2.insert_one({"Dist_IP":msg["Dist_IP"],"Dist_ID":msg["Dist_ID"]})
                print "Distribution Added----> ID:"+str(msg["Dist_ID"])+" IP:"+str(msg["Dist_IP"])
            else:
                print "Distribution already present----> ID:"+str(msg["Dist_ID"])+" IP:"+str(msg["Dist_IP"])
            ndist[msg["Dist_IP"]]=0
            insert_dist=[0,""]
        elif delete_dist[0]:
            dist_ips,dist_ids=search_dist()
            msg=json.loads(delete_dist[1])
            client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(msg))
            if msg["Dist_ID"] not in dist_ids and msg["Dist_IP"] not in dist_ips:
                print "Distribution not present----> ID:"+str(msg["Dist_ID"])+" IP:"+str(msg["Dist_IP"])
            else:
                for i in col4.find():
                    if i["TO_IP"]==msg["Dist_IP"]:
                        killdict={"Origin_IP":i["FROM_IP"],"CMD":i["CMD"]}
                        client.publish("origin/ffmpeg/kill",json.dumps(killdict))
                col4.delete_many({"TO_IP":msg["Dist_IP"]})
                col2.delete_one({"Dist_IP":msg["Dist_IP"],"Dist_ID":msg["Dist_ID"]})
                print "Distribution Deleted----> ID:"+str(msg["Dist_ID"])+" IP:"+str(msg["Dist_IP"])
            del ndist[msg["Dist_IP"]]
            delete_dist=[0,""]
        elif insert_stream[0]:
            if len(norigin)==0:
                print "No Origin Server Present"
                insert_stream=[0,""]
            else:
                sorigin=str(min(norigin.items(), key=lambda x: x[1])[0])
                stream_ips,stream_ids=search_stream()
                msg=json.loads(insert_stream[1])
                client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(msg))
                if msg["Stream_ID"] not in stream_ids:
                    print str(sorigin)+" "+str(msg["Stream_ID"])+" "+str(msg["Stream_IP"])
                    ddict={"Origin_IP":sorigin,"Stream_ID":msg["Stream_ID"],"Stream_IP":msg["Stream_IP"]}
                    print "Sending Dict"
                    client.publish("origin/ffmpeg/stream/spawn",json.dumps(ddict)) # Insert dict here
                    time.sleep(30)
                    client.publish("origin/ffmpeg/stream/stat/spawn",json.dumps(ddict)) # Insert dict here
                    col3.insert_one({"Stream_IP":msg["Stream_IP"],"Stream_ID":msg["Stream_ID"],"Origin_IP":"","Dist_IP":""})
                    print "Stream Added----> ID:"+str(msg["Stream_ID"])+" IP:"+str(msg["Stream_IP"])
                else:
                    print "Stream already present----> ID:"+str(msg["Stream_ID"])+" IP:"+str(msg["Stream_IP"])
                insert_stream=[0,""]
        elif delete_stream[0]:
            stream_ips,stream_ids=search_stream()
            msg=json.loads(delete_stream[1])
            print msg
            client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(msg))
            if msg["Stream_ID"] not in stream_ids and msg["Stream_IP"] not in stream_ips:
                print "Stream not present----> ID:"+str(msg["Stream_ID"])+" IP:"+str(msg["Stream_IP"])
            else:
                for i in col4.find():
                    if str(i["FROM_IP"])==msg["Stream_IP"]:
                        killdict={"Origin_IP":i["TO_IP"],"CMD":i["CMD"],"Stream_ID":i["Stream_ID"]}
                        client.publish("origin/ffmpeg/kill",json.dumps(killdict))
                    if str(i["Stream_ID"])==msg["Stream_ID"]:
                        killdict={"Origin_IP":i["FROM_IP"],"CMD":i["CMD"],"Stream_ID":i["Stream_ID"]}
                        client.publish("origin/ffmpeg/kill",json.dumps(killdict))
                col3.delete_one({"Stream_IP":msg["Stream_IP"],"Stream_ID":msg["Stream_ID"]})
                col4.delete_many({"Stream_ID":msg["Stream_ID"]})
                # col1.update_one({"Stream_ID":msg["Stream_ID"]},{"$set":{"Stream_ID":""}},upsert=True)
                print "Stream Deleted----> ID:"+str(msg["Stream_ID"])+" IP:"+str(msg["Stream_IP"])
            delete_stream=[0,""]
        elif reqstream[0]:
            msg=json.loads(reqstream[1])
            stream_id=msg["Stream_ID"]
            print stream_id
            alreadypushedflag=0
            client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(msg))
            if len(ndist)!=0:
                for i in col4.find():
                    if (str(i["Stream_ID"])==stream_id) and (str(i["TO_IP"]) in ndist.keys()):
                        print "Should come here if already pushed to a dist"
                        client.publish("lbsresponse/rtmp",str(i["CMD"].split()[-2]))
                        alreadypushedflag=0
                        break
                    else:
                        alreadypushedflag=1
                if alreadypushedflag==1:
                    sdist=str(min(ndist.items(), key=lambda x: x[1])[0])
                    print str(sorigin)+" "+str(sdist)+" "+str(stream_id)
                    stream_ip=col3.find_one({"Stream_ID":stream_id})["Stream_IP"]
                    distdict={"Origin_IP":sorigin,"Dist_IP":sdist,"Stream_ID":stream_id,"Stream_IP":stream_ip}
                    client.publish("origin/ffmpeg/dist/spawn",json.dumps(distdict))
                    time.sleep(60)
                    client.publish("dist/ffmpeg/stream/stat/spawn",json.dumps(distdict))
            else:
                for i in col4.find():
                    if i["Stream_ID"]==stream_id:
                        client.publish("lbsresponse/rtmp",str(i["CMD"].split()[-2]))
	    reqstream=[0,""]
        elif origin_ffmpeg_dist[0]:
            msg=json.loads(origin_ffmpeg_dist[1])
            print msg
            client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(msg))
            # cmd=msg["CMD"]
            # # PID=msg[11]s
            # TO_IP=msg["TO_IP"]
            # FROM_IP=msg["FROM_IP"]
            # Stream_ID=msg["Stream_ID"]
            print msg["Stream_ID"]+" stream push has been started from origin "+ msg["FROM_IP"]+" to distribution "+msg["TO_IP"]
            col4.insert_one({"CMD":msg["CMD"],"TO_IP":msg["TO_IP"],"FROM_IP":msg["FROM_IP"],"Stream_ID":msg["Stream_ID"]})
            col3.update_one({"Stream_ID":msg["Stream_ID"],"Origin_IP":msg["FROM_IP"]},{"$set":{"Dist_IP":msg["TO_IP"]}},upsert=True)
            client.publish("lbsresponse/rtmp",str(msg["CMD"].split()[-2]))
            origin_ffmpeg_dist=[0,""]
        elif origin_ffmpeg_respawn[0]:
            msg=json.loads(origin_ffmpeg_respawn[1])
            print str(msg)+" should come here only when missing becomes active"
            # streams = msg["Stream_list"]
            # origin_IP = msg["Origin_IP"]
            # i = 0
            # for stream in streams:
            #     stream_ip = col3.find_one({"Stream_ID":stream["Stream_ID"]})["Stream_IP"]
            #     streams[i] = {"Origin_IP":origin_IP,"Stream_ID":stream["Stream_ID"],"Stream_IP":stream_ip}
            #     i += 1
            client.publish("origin/ffmpeg/respawn",json.dumps(msg))
            origin_ffmpeg_respawn=[0,""]
        elif archive_stream_add[0]:
            msg=json.loads(archive_stream_add[1])
            print msg
            msg["Stream_IP"] = col3.find_one({"Stream_ID":msg["Stream_ID"]})["Stream_IP"]
            msg["Origin_IP"]= col3.find_one({"Stream_ID":msg["Stream_ID"]})["Origin_IP"]
            print str(msg)+" archiving this......"
            client.publish("origin/ffmpeg/archive/add",json.dumps(msg))
            archive_stream_add=[0,""]
        elif archive_stream_del[0]:
            msg=json.loads(archive_stream_del[1])
            print msg
            msg["Stream_IP"] = col3.find_one({"Stream_ID":msg["Stream_ID"]})["Stream_IP"]
            msg["Origin_IP"]= col3.find_one({"Stream_ID":msg["Stream_ID"]})["Origin_IP"]
            print str(msg)+" archiving deleting this......"
            client.publish("origin/ffmpeg/archive/delete",json.dumps(msg))
            archive_stream_del=[0,""]
        elif origin_ffmpeg_dist_respawn[0]:
            msg=json.loads(origin_ffmpeg_dist_respawn[1])
            print str(msg)+" should come here only when missing becomes active"
            msg["Origin_IP"]=col3.find_one({"Stream_IP": msg["Stream_IP"],"Stream_ID": msg["Stream_ID"], "Dist_IP": msg["Dist_IP"]})["Origin_IP"]
            client.publish("origin/ffmpeg/dist/respawn",json.dumps(msg))
            origin_ffmpeg_dist_respawn=[0,""]
        else:
            continue
        

       







