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
import os
import sys


class Statter():
    """Statter Class to check status of NGINX based FFMPEG streams"""

    def __init__(self, tsDBParams, statPageURL, mqtt_ip, mqtt_port, mqttTopics,
                 mqtt_uname, mqtt_passwd):
        """ Internal Defs"""
        self.statPageURL = statPageURL
        ''' Registered Streams '''
        self.rS = {}
        self.missingsQ = Queue.Queue()
        self.startFlag = False
        self.waitPeriod = 30
        self.dictLock = threading.Lock()
        """ Origin Server IP Address, currently LAN IP """
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        self.ORIGIN_IP = str(s.getsockname()[0])
        self.ORIGIN_ID = os.environ["ORIGIN_ID"]
        if(self.ORIGIN_ID is None):
            sys.exit(0)
        s.close()

        ''' MQTT Backend '''
        self.mqttServerParams = {}
        self.mqttServerParams["url"] = mqtt_ip
        self.mqttServerParams["port"] = int(mqtt_port)
        self.mqttServerParams["timeout"] = 60
        self.mqttServerParams["topic"] = mqttTopics
        self.mqttServerParams["username"] = mqtt_uname
        self.mqttServerParams["password"] = mqtt_passwd
        self.mqttServerParams["onMessage"] = self.on_message
        self.mqttc = MQTTPubSub(self.mqttServerParams)
        self.mqttc.run()
        self.numClients = 0

        ''' InfluxDB '''
        self.influxClient = InfluxDBClient(
            tsDBParams["url"], tsDBParams["port"],
            tsDBParams["uname"], tsDBParams["pwd"], tsDBParams["appName"])
        self.influxClient.create_database('statter')
        print("Initalization done")

    #def addNewStream(self, stream_id, stream_ip):
    def addNewStream(self, stream_id, stream_ip, dbStatus):
        with self.dictLock:
            print("Adding Stream \t" + str(stream_id))
            self.rS[stream_id] = {"stream_ip": stream_ip,
                                  "status": dbStatus,
                                  "revived": 0,
                                  "Timer": None,
                                  "cmdTimerDuration": 15,
                                  "cmdCounter": 0,
                                  "InBW": 0}

    def deleteStream(self, streamId):
        with self.dictLock:
            print("Removing Stream \t" + str(streamId))
            del self.rS[streamId]

    def on_message(self, client, userdata, message):
        msg = message.payload
        topic = message.topic
        msgDict = json.loads(msg)
        print(msgDict)
        try:
            if isinstance(msgDict, list):
                for stream in msgDict:
                    if stream["origin_id"] == self.ORIGIN_ID:
                        if topic == "origin/ffmpeg/kill":
                            self.deleteStream(stream["stream_id"])
            else:
                if msgDict["origin_id"] == self.ORIGIN_ID:
                    if topic == "origin/ffmpeg/stream/stat/spawn":
                        self.addNewStream(msgDict["stream_id"],
                                          msgDict["stream_ip"], "onboarding")
                        #self.addNewStream(msgDict["stream_id"], msgDict["stream_ip"])
                    if topic == "origin/ffmpeg/killall":
                        with self.dictLock:
                            self.rS = {}
                    if topic == "lb/request/origin/streams":
                        print("Initialized Streams")
                        if (msgDict["stream_list"]):
                            streamList = msgDict["stream_list"]
                            for stream in streamList:
                                self.addNewStream(stream["stream_id"], stream["stream_ip"], stream["status"])
                        self.startFlag = True
        except Exception as e:
            print("Couldn't decode response", e)

    def stat(self):
        while(True):
            print("Statting")
            req = requests.get(self.statPageURL)
            stats = xd.parse(req.content)
            statList = []
            try:
                # TODO: Hardcoded application 1 here
                self.numClients = (stats["rtmp"]["server"]["application"][1]
                                        ["live"]["nclients"])
                stats = (stats["rtmp"]["server"]["application"][1]
                              ["live"]["stream"])
            except Exception as e:
                print("Couldn't decode response", e)

            if isinstance(stats, collections.OrderedDict):
                statList.append(stats)
            elif isinstance(stats, list):
                statList = stats

            try:
                with self.dictLock:
                    presentStreams = []
                    allStreams = self.rS.keys()
                    #print("Allstreams ",allStreams)
                    #for stream in self.rS:
                    #    self.rS[stream]["status"] = "down" 
        
                    ''' Update status '''
                    if isinstance(statList, list):
                        for stat in statList:
                            try: 
                                """ Check only publishing streams """
                                if stat["name"] in self.rS and "publishing" in stat.keys():
                                    s_name = stat["name"]
                                    presentStreams.append(stat["name"])
                                    self.rS[stat["name"]]["InBW"] = int(stat["bw_in"])
                                    #Ab: Found an active stream which in dB shows "down" or "onboarding"
                                    #    Send message to dB to update the status
                                    if (self.rS[stat["name"]]["status"] == "down" or 
                                             self.rS[stat["name"]]["status"] == "onboarding"):
                                        print("Found down/onboarding stream that is active")
                                        #Update the status in dB
                                        self.rS[stat["name"]]["status"] = "active"
                                        #print(self.rS[stat["name"]]["status"], s_name)
                                        msg = {"stream_id": stat["name"], "status": "active"}
                                        #print(json.dumps(msg))
                                        self.mqttc.publish("stream/stat", json.dumps(msg))
                                        if (self.rS[s_name]["Timer"] != None):
                                            self.rS[s_name]["Timer"].cancel()
                                            self.rS[s_name]["Timer"] = None
                                            self.rS[s_name]["cmdCounter"] = 0
                            except Exception as e:
                                print("No name in stat, error message ",e)
                    #print("Present Streams ", set(presentStreams))
                    for missing in list(set(allStreams) - set(presentStreams)):
                        print("Missing ", missing)
                        #print(self.rS[missing]['status'])
                        if (self.rS[missing]['status'] == "active" or self.rS[missing]['status'] == "onboarding"):
                           print("Active becoming inactive")
                           self.rS[missing]['status']  = "down"
                           msg = {"stream_id": missing, "status": "down"}
                           self.mqttc.publish("stream/stat", json.dumps(msg))

                        if (self.rS[missing]["status"] == "down"):
                           #print(self.rS[missing]['status'])
                           #If Timer object is not there, then initiate the timer and start
                           if (self.rS[missing]["Timer"] == None):
                               #print("Starting Timer", self.rS[missing]['status'])
                               self.rS[missing]["cmdCounter"] = 0
                               #print(self.rS[missing]["cmdTimerDuration"])
                               self.rS[missing]["Timer"] = threading.Timer(self.rS[missing]["cmdTimerDuration"],
                                                                         self.cmdTimerCallback,
                                                                         args=[missing])
                               self.rS[missing]["Timer"].start()
                            

            except Exception as e:
                print("Couldn't read stat ", e)
            time.sleep(10)

    def cmdTimerCallback(self, streamid):
        #Timed Out, Re-issue spawning command
        print("Timed out, Trying spawning stream again: ", self.rS[streamid]['stream_ip'])
        out = json.dumps({"origin_ip": self.ORIGIN_IP,
                          "origin_id": self.ORIGIN_ID,
                          "stream_id": streamid,
                          "stream_ip": self.rS[streamid]["stream_ip"]})
        self.mqttc.publish("origin/ffmpeg/stream/spawn", out)
    
        #Start Timer again
        self.rS[streamid]["cmdCounter"] = self.rS[streamid]["cmdCounter"] + 1
        self.rS[streamid]["Timer"] = threading.Timer(self.rS[streamid]["cmdTimerDuration"],
                                                   self.cmdTimerCallback,
                                                   args=[streamid])
        self.rS[streamid]["Timer"].start()
        return

    def resetrevived(self, streamId):
        with self.dictLock:
            try:
                self.rS[streamId]["revived"] = 0
            except Exception as e:
                print("Couldn't  revive ", e)

    def checkStat(self):
        while(True):
            with self.dictLock:
                for stream in self.rS:
                    if (self.rS[stream]["status"] == 0 and
                            self.rS[stream]["revived"] == 0):
                        self.rS[stream]["revived"] = 1
                        self.missingsQ.put(stream)
                        self.rS[stream]["Timer"] = threading.Timer(self.waitPeriod,
                                                                   self.resetrevived,
                                                                   args=[stream])
                        self.rS[stream]["Timer"].start()
            time.sleep(1)

    def pub(self):
        while(True):
            if(self.missingsQ.empty() is not True):
                stream = self.missingsQ.get()
                print("Missing")
                print(self.rS[stream])
                streamId = stream
                streamIp = self.rS[stream]["stream_ip"]
                streamDict = {"stream_ip": streamIp,
                              "stream_id": streamId, "origin_id": self.ORIGIN_ID}
                print("Publishing")
                print(streamDict)
                self.mqttc.publish("db/origin/ffmpeg/respawn",
                                   json.dumps(streamDict))
                msg = {"stream_id": streamId, "status": "down"}
                self.mqttc.publish("stream/stat", json.dumps(msg))
            time.sleep(1)

    def logger(self):
        ''' Replace with publisher here '''
        while(True):
            ''' TODO: Send status on a per stream basis here '''
            msg = json.dumps({"origin_id": self.ORIGIN_ID,
                              "num_clients": str(self.numClients)})
            print(msg)
            self.mqttc.publish("origin/stat", msg)
            epochTime = int(time.time()) * 1000000000
            self.logDataFlag = False
            with self.dictLock:
                for stream in self.rS:
                    series = []
                    ''' status '''
                    pointValues = {
                        "time": epochTime,
                        "measurement": "status",
                        'fields': {
                            'value': self.rS[stream]["status"],
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
                            'value': self.rS[stream]["InBW"],
                        },
                        'tags': {
                            "streamId": stream
                        },
                    }
                    series.append(pointValues)
                    ''' Append BitRate here '''
                    self.influxClient.write_points(series, time_precision='n')
            time.sleep(30)

    def start(self):
        time.sleep(10)
        self.mqttc.publish("request/origin/streams", json.dumps({"origin_id": self.ORIGIN_ID}))
        print("Requesting all streams belonging to ", self.ORIGIN_ID)
        while(self.startFlag is False):
            time.sleep(0.5)
        ''' Start the stat thread '''
        self.statThread = threading.Thread(target=self.stat)
        self.statThread.daemon = True
        self.statThread.start()
        ''' Start checker '''
        #self.checkThread = threading.Thread(target=self.checkStat)
        #self.checkThread.daemon = True
        #self.checkThread.start()
        ''' Missing Publisher '''
        #self.pubThread = threading.Thread(target=self.pub)
        #self.pubThread.daemon = True
        #self.pubThread.start()
        ''' Logger '''
        self.loggerThread = threading.Thread(target=self.logger)
        self.loggerThread.daemon = True
        self.loggerThread.start()

        while(True):
            try:
                with self.dictLock:
                    if (self.rS):
                        for stream in self.rS:
                            ''' For printing '''
                            obj = self.rS[stream]
            except Exception as e:
                print("Error in loop ", e)
            time.sleep(2)


if __name__ == "__main__":
    statPageURL = "http://localhost:8080/stat"
    mqtt_ip = os.environ["LB_IP"]
    mqtt_port = os.environ["MQTT_PORT"]
    mqtt_uname = os.environ["MQTT_UNAME"]
    mqtt_passwd = os.environ["MQTT_PASSWD"]
    mqttTopics = [("origin/ffmpeg/stream/stat/spawn", 1),
                  ("origin/ffmpeg/kill", 1),
                  ("lb/request/origin/streams", 1),
                  ("origin/ffmpeg/killall", 1)]
    ''' TODO: Parameterize tsdb params '''
    tsDBParams = {"url": "127.0.0.1", "port": 8086,
                  "uname": "root", "pwd": "root", "appName": "statter"}
    statter = Statter(tsDBParams, statPageURL, mqtt_ip, mqtt_port,
                      mqttTopics, mqtt_uname, mqtt_passwd)

    statter.start()

