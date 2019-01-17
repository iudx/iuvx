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
import threading
import Queue

streamList = []



class Statter():
    """Statter Class to check status of NGINX based FFMPEG streams"""
    def __init__(self, statPageURL, mqttServer, mqttTopics):
        """ Internal Defs"""
        self.statPageURL = statPageURL
        self.registeredStreams = {} 
        self.missingsQ = Queue.Queue()
        """ Origin Server IP Address, currently LAN IP """
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80)) 
        self.origin_IP = str(s.getsockname()[0])
        s.close()
        ''' MQTT Backend '''
        self.mqttServerParams = {}
        self.mqttServerParams["url"] = mqttServer
        self.mqttServerParams["port"] = 1883
        self.mqttServerParams["timeout"] = 60
        self.mqttServerParams["topic"] = mqttTopics 
        self.mqttServerParams["onMessage"] = self.on_message
        self.mqttc = MQTTPubSub(self.mqttServerParams)


    def addNewStream(self, stream_id, stream_ip):
        self.registeredStreams[stream_id] = {"Stream_IP":stream_ip,
                                              "Status":1} 

    def deleteStream(self, streamId):
        self.registeredStreams.pop(streamId)

    def on_message(self, client, userdata, message):
        msg = str(message.payload.decode("utf-8"))
        topic = str(message.topic.decode("utf-8"))
        msgDict = json.loads(msg)
        try:
            if msgDict["Origin_IP"] == self.origin_IP:
                if topic == "origin/ffmpeg/stream/stat/spawn":
                    self.addNewStream(msgDict["Stream_ID"],msgDict["Stream_IP"])
                if topic == "origin/ffmpeg/kill":
                    self.deleteStream(msgDict["Stream_ID"])
                if topic == "origin/ffmpeg/killall":
                    self.registeredStreams = []
        except Exception as e:
            print(e)

    def stat(self):
        while(True):
            req = requests.get(self.statPageURL)
            stats = xd.parse(req.content)
            statList = [] 
            try:
                stats = stats["rtmp"]["server"]["application"][1]["live"]["stream"]
            except Exception as e:
                print(e)

            if isinstance(stats, collections.OrderedDict):
                statList.append(stats)
            elif isinstance(stats, list):
                statList = stats

            try:
                ''' Clear status '''
                for stream in self.registeredStreams:
                    self.registeredStreams[stream]["Status"] = 0
                ''' Update status '''
                for stat in statList:
                    if stat["name"] in self.registeredStreams:
                        self.registeredStreams[stat["name"]]["Status"] = 1
            except Exception as e:
                print(e)

            time.sleep(0.5)


    def checkStat(self):
        while(True):
            for stream in self.registeredStreams:
                if self.registeredStreams[stream]["Status"] == 0:
                    self.missingsQ.put(stream)
            time.sleep(10)


    def pub(self):
        while(True):
            if(self.missingsQ.empty() != True):
                stream = self.missingsQ.get()
                print("Missing")
                print(self.registeredStreams[stream])
                streamId = stream 
                streamIp = self.registeredStreams[stream]["Stream_IP"] 
                streamDict = {"Stream_IP":streamIp,"Stream_ID":streamId, "Origin_IP":self.origin_IP}
                print("Publishing")
                print(streamDict)
                self.mqttc.publish("db/origin/ffmpeg/respawn",json.dumps(streamDict))
            time.sleep(1)


    def streamMissingCallback(self, streamID):
        pass


    def start(self):
        self.mqttc.run()
        ''' Start the stat thread '''
        self.statThread = threading.Thread(target=self.stat)
        self.statThread.daemon = True
        self.statThread.start()
        ''' Start checker '''
        self.checkThread = threading.Thread(target=self.checkStat)
        self.checkThread.daemon = True
        self.checkThread.start()
        ''' Missing Publisher '''
        self.pubThread = threading.Thread(target=self.pub)
        self.pubThread.daemon = True
        self.pubThread.start()

        while(True):
            try:
                for stream in self.registeredStreams:
                    obj = self.registeredStreams[stream]
                    print("Stream_ID  " + str(stream) + "\t Stream_Status   " + str(obj["Status"]))
            except Exception as e:
                print(e)
            time.sleep(2)


if __name__ == "__main__":
    # mongoclient=pymongo.MongoClient('mongodb://localhost:27017/')
    # mydb=mongoclient["Origin_Streams"]
    # col1=mydb["Streams"]
    statPageURL ="http://localhost:8080/stat"
    mqttServer = "10.156.14.141"
    mqttTopics = [("origin/ffmpeg/stream/stat/spawn", 0),
                  ("origin/ffmpeg/kill", 0), 
                  ("origin/ffmpeg/killall", 0)]
    statter = Statter(statPageURL, mqttServer, mqttTopics)


    statter.start()
