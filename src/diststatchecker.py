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
from influxdb import InfluxDBClient


class Statter():
    """Statter Class to check status of NGINX based FFMPEG streams"""

    def __init__(self, statPageURL, mqttServer, mqttTopics):
        """ Internal Defs"""
        self.statPageURL = statPageURL
        self.registeredStreams = {}
        self.missingsQ = Queue.Queue()
        self.startFlag = False
        self.waitPeriod = 30
        self.dictLock = threading.Lock()
        """ Distribution  Server IP Address, currently LAN IP """
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        self.dist_IP = str(s.getsockname()[0])
        s.close()
        ''' MQTT Backend '''
        self.mqttServerParams = {}
        self.mqttServerParams["url"] = mqttServer
        self.mqttServerParams["port"] = 1883
        self.mqttServerParams["timeout"] = 60
        self.mqttServerParams["topic"] = mqttTopics
        self.mqttServerParams["onMessage"] = self.on_message
        self.mqttc = MQTTPubSub(self.mqttServerParams)
        self.numClients=0
        ''' InfluxDB '''
        #tsDBUrl = "127.0.0.1"
        #tsDBPort = 8086
       # tsDBUname = "root"
        #tsDBPwd = "root"
        #appName = "Statter"
        #self.influxClient = InfluxDBClient(
           # tsDBUrl, tsDBPort, tsDBUname, tsDBPwd, appName)

    def addNewStream(self, stream_id, stream_ip):
        with self.dictLock:
            print("Adding Stream \t" + str(stream_id))
            self.registeredStreams[stream_id] = {"Stream_IP": stream_ip,
                "Status": 1,"Revived":0, "Timer":None, "InBW":0}

    def deleteStream(self, streamId):
        with self.dictLock:
            print ("Removing Stream \t"+str(streamId))
            del self.registeredStreams[streamId]

    def on_message(self, client, userdata, message):
        msg = str(message.payload.decode("utf-8"))
        topic = str(message.topic.decode("utf-8"))
        msgDict = json.loads(msg)
        try:
            if isinstance(msgDict,list):
                for i in msgDict:
                    if i["Dist_IP"] == self.dist_IP:
                        if topic == "origin/ffmpeg/kill":
                            self.deleteStream(i["Stream_ID"])
            else:
                if msgDict["Dist_IP"] == self.dist_IP:
                    if topic == "dist/ffmpeg/stream/stat/spawn":
                        self.addNewStream(
                            msgDict["Stream_ID"], msgDict["Stream_IP"])
                    if topic == "origin/ffmpeg/killall":
                        with self.dictLock:
                            self.registeredStreams = []
                    if topic == "lb/request/allstreams":
                        print("Initialized Streams")
                        if (msgDict["Stream_List"]):
                            streamList = msgDict["Stream_List"]
                            for stream in streamList:
                                self.addNewStream(
                                    stream["Stream_ID"], stream["Stream_IP"])
                        self.startFlag = True
        except Exception as e:
            print(e)

    def stat(self):
        while(True):
            req = requests.get(self.statPageURL)
            stats = xd.parse(req.content)
            statList = []
            try:
                self.numClients= stats["rtmp"]["server"]["application"][1]["live"]["nclients"]
                stats = stats["rtmp"]["server"]["application"][1]["live"]["stream"]
            except Exception as e:
                print(e)

            if isinstance(stats, collections.OrderedDict):
                statList.append(stats)
            elif isinstance(stats, list):
                statList = stats

            try:
                with self.dictLock:
                    ''' Clear status '''
                    for stream in self.registeredStreams:
                        self.registeredStreams[stream]["Status"] = 0
                    ''' Update status '''
                    for stat in statList:
                        if stat["name"] in self.registeredStreams:
                            self.registeredStreams[stat["name"]]["InBW"] = int(stat["bw_in"])
                            self.registeredStreams[stat["name"]]["Status"] = 1
            except Exception as e:
                print(e)
            time.sleep(0.5)

    def resetRevived(self, streamId):
        with self.dictLock:
            try:
                self.registeredStreams[streamId]["Revived"] = 0
            except Exception as e:
                print e

    def checkStat(self):
        while(True):
            with self.dictLock:
                for stream in self.registeredStreams:
                    if (self.registeredStreams[stream]["Status"] == 0) and (self.registeredStreams[stream]["Revived"] == 0):
                        self.registeredStreams[stream]["Revived"] = 1
                        self.missingsQ.put(stream)
                        self.registeredStreams[stream]["Timer"] = threading.Timer(self.waitPeriod,
                                                                          self.resetRevived, args=[stream])
                        self.registeredStreams[stream]["Timer"].start()
            time.sleep(1)

    def pub(self):
        while(True):
            if(self.missingsQ.empty() != True):
                stream = self.missingsQ.get()
                print("Missing")
                print(self.registeredStreams[stream])
                streamId = stream
                streamIp = self.registeredStreams[stream]["Stream_IP"]
                streamDict = {"Stream_IP": streamIp,
                              "Stream_ID": streamId, "Dist_IP": self.dist_IP}
                print("Publishing")
                print(streamDict)
                self.mqttc.publish("db/origin/ffmpeg/respawn",
                                   json.dumps(streamDict))
            time.sleep(1)

    def logger(self):
        ''' Replace with publisher here '''
        while(True):
            self.mqttc.publish("dist/stat",json.dumps({"Dist_IP":self.dist_IP,"NClients":str(self.numClients)}))
            epochTime = int(time.time()) * 1000000000
            self.logDataFlag = False
            with self.dictLock:
                for stream in self.registeredStreams:
                    series = []
                    ''' Status '''
                    pointValues = {
                        "time": epochTime,
                        "measurement": "Status",
                        'fields': {
                            'value': self.registeredStreams[stream]["Status"],
                        },
                        'tags': {
                            "streamId": stream
                        },
                    }
                    series.append(pointValues)
                    ''' BW '''
                    pointValues = {
                        "time": epochTime,
                        "measurement": "Bandwidth",
                        'fields': {
                            'value': self.registeredStreams[stream]["InBW"],
                        },
                        'tags': {
                            "streamId": stream
                        },
                    }
                    series.append(pointValues)
                    ''' Append BitRate here '''
                    #self.influxClient.write_points(series, time_precision='n')
            time.sleep(30)

    def start(self):
        self.mqttc.run()
        time.sleep(0.5)
        self.mqttc.publish("request/allstreams", self.dist_IP)
        while(self.startFlag == False):
            time.sleep(0.5)
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
        ''' Logger '''
        self.loggerThread = threading.Thread(target=self.logger)
        self.loggerThread.daemon = True
        self.loggerThread.start()

        while(True):
            try:
		with self.dictLock:
			if (self.registeredStreams):
			    for stream in self.registeredStreams:
				obj = self.registeredStreams[stream]
            except Exception as e:
                print(e)
            time.sleep(2)


if __name__ == "__main__":
    # mongoclient=pymongo.MongoClient('mongodb://localhost:27017/')
    # mydb=mongoclient["Origin_Streams"]
    # col1=mydb["Streams"]
    statPageURL = "http://localhost:8080/stat"
    mqttServer = "10.156.14.138"
    mqttTopics = [("dist/ffmpeg/stream/stat/spawn", 0),
                  ("origin/ffmpeg/kill", 0),
                  ("lb/request/allstreams", 0),
                  ("origin/ffmpeg/killall", 0)]
    statter = Statter(statPageURL, mqttServer, mqttTopics)

    statter.start()

