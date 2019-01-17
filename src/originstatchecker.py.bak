import requests
import xmltodict as xd
import paho.mqtt.client as mqtt
import socket
# import pymongo
import time
import threading
from MQTTPubSub import MQTTPubSub
import time
import collections
import json


streamList = []

s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect(("8.8.8.8", 80))
origin_IP = str(s.getsockname()[0])
s.close()


def on_message(client, userdata, message):
    global streamList
    msg = str(message.payload.decode("utf-8"))
    topic = str(message.topic.decode("utf-8"))
    print(msg)
    ddict = json.loads(msg)
    print("Added Stream \t" + str(ddict))
    print (topic)
    if ddict["Origin_IP"] == origin_IP:
        print ("Entered topic tree")
        if topic == "origin/ffmpeg/stream/stat/spawn":
            print ("Came here"+ddict["Stream_ID"])
            streamList.append(ddict["Stream_ID"])
        if topic == "origin/ffmpeg/kill":
            print ("Came here"+ddict["Stream_ID"])
            streamList.remove(ddict["Stream_ID"])
            print (streamList)
        if topic == "origin/ffmpeg/killall":
            streamList = []


# MQTT Params
mqttServerParams = {}
mqttServerParams["url"] = "10.156.14.141"
mqttServerParams["port"] = 1883
mqttServerParams["timeout"] = 60
mqttServerParams["topic"] = [("origin/ffmpeg/stream/stat/spawn", 0),
                             ("origin/ffmpeg/kill", 0), ("origin/ffmpeg/killall", 0)]
mqttServerParams["onMessage"] = on_message
client = MQTTPubSub(mqttServerParams)


'''
	{"Stream_IP":scas,"Stream_ID":ssdm,"OriginIp":ascasc}
'''


def checkStat():
    global streamList
    global client
    global origin_IP
    currStreams = []
    print("Running Stat Checker")
    missing = []
    streamPresentFlag = False
    while(True):
        try:
            req = requests.get("http://localhost:8080/stat")
            stat = xd.parse(req.content)
            currStreamList = []
            try:
                stat = stat["rtmp"]["server"]["application"][1]["live"]["stream"]
                streamPresentFlag = True
            except Exception as e:
                streamPresentFlag = False
                print(e)
            print("Registered streams \t" + str(streamList))
            if(streamPresentFlag):
                if isinstance(stat, collections.OrderedDict):
                    print("Single Stream")
                    currStreams.append(stat["name"])
                elif isinstance(stat, list):
                    print("Multi Stream")
                    for stream in stat:
                        currStreams.append(stream["name"])
                    print("Done")

            #missing = list(set(streamList).symmetric_difference(set(currStreams)))
            for st in streamList:
                if st not in currStreams:
                    missing.append(st)

            print("Missing " + str(missing))

            if(missing):
                print ("Should not publish till this is entered")
                print (missing)
                # spawnList = []
                spawnDict = {"Origin_IP": "", "Stream_list": []}
                streams = {}
                for st in missing:
                    spawnDict["Stream_list"].append({"Stream_ID": st})
                spawnDict["Origin_IP"] = origin_IP
                print (spawnDict)
                client.publish("db/origin/ffmpeg/respawn",
                               json.dumps(spawnDict))
                missing = []
                time.sleep(30)

            currStreams = []


#		if  isinstance(stat["rtmp"]["server"]["application"][1]["live"]["stream"],collections.OrderedDict):
#			# col.insert_one(stat["rtmp"]["server"]["application"][1]["live"]["stream"],{"Status":1})
#			streams.append(stat["rtmp"]["server"]["application"][1]["live"]["stream"],{"Status":1})
#		else:
#			for i in range(len(stat["rtmp"]["server"]["application"][1]["live"]["stream"])):
#				#col.insert_one(stat["rtmp"]["server"]["application"][1]["live"]["stream"][i],{"Status":1})
#				streams.append(stat["rtmp"]["server"]["application"][1]["live"]["stream"][i],{"Status":1})

        except Exception as e:
            print(e)
            print ("No Streams YETTTTT!!")

        time.sleep(0.01)


if __name__ == "__main__":
    # mongoclient=pymongo.MongoClient('mongodb://localhost:27017/')
    # mydb=mongoclient["Origin_Streams"]
    # col1=mydb["Streams"]

    client.run()
    statThread = threading.Thread(target=checkStat)
    statThread.start()
    print("Running statter")

    while(True):
        time.sleep(0.01)
        req = requests.get("http://localhost:8080/stat")
        stat = xd.parse(req.content)
        client.publish("origin/stat", str(origin_IP)+" " +
                       str(stat["rtmp"]["server"]["application"][1]["live"]["nclients"]))
