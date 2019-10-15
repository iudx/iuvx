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

    def __init__(self, tsDBParams, statPageURL, mqtt_ip, mqtt_port, mqttTopics):
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
        self.origin_IP = str(s.getsockname()[0])
        self.origin_ID = os.environ["ORIGIN_ID"]
        if(self.origin_ID is None):
            sys.exit(0)

        s.close()
        ''' MQTT Backend '''
        self.mqttServerParams = {}
        self.mqttServerParams["url"] = mqtt_ip
        self.mqttServerParams["port"] = mqtt_port
        self.mqttServerParams["timeout"] = 60
        self.mqttServerParams["topic"] = mqttTopics
        self.mqttServerParams["onMessage"] = self.on_message
        self.mqttc = MQTTPubSub(self.mqttServerParams)
        self.numClients = 0
        ''' InfluxDB '''
        self.influxClient = InfluxDBClient(
            tsDBParams["url"], tsDBParams["port"],
            tsDBParams["uname"], tsDBParams["pwd"], tsDBParams["appName"])

    def addNewStream(self, stream_id, stream_ip):
        with self.dictLock:
            print("Adding Stream \t" + str(stream_id))
            self.rS[stream_id] = {"stream_ip": stream_ip,
                                  "status": 1,
                                  "revived": 0,
                                  "Timer": None,
                                  "InBW": 0}

    def deleteStream(self, streamId):
        with self.dictLock:
            print("Removing Stream \t" + str(streamId))
            del self.rS[streamId]

    def on_message(self, client, userdata, message):
        msg = str(message.payload.decode("utf-8"))
        topic = str(message.topic.decode("utf-8"))
        msgDict = json.loads(msg)
        print(msgDict)
        try:
            if isinstance(msgDict, list):
                for stream in msgDict:
                    if stream["origin_ip"] == self.origin_IP:
                        if topic == "origin/ffmpeg/kill":
                            self.deleteStream(stream["stream_id"])
            else:
                if msgDict["origin_ip"] == self.origin_IP:
                    if topic == "origin/ffmpeg/stream/stat/spawn":
                        self.addNewStream(msgDict["stream_id"],
                                          msgDict["stream_ip"])
                    if topic == "origin/ffmpeg/killall":
                        with self.dictLock:
                            self.rS = {}
                    if topic == "lb/request/origin/streams":
                        print("Initialized Streams")
                        if (msgDict["stream_list"]):
                            streamList = msgDict["stream_list"]
                            for stream in streamList:
                                self.addNewStream(stream["stream_id"],
                                                  stream["stream_ip"])
                        self.startFlag = True
        except Exception as e:
            print(e)

    def stat(self):
        while(True):
            req = requests.get(self.statPageURL)
            stats = xd.parse(req.content)
            statList = []
            try:
                self.numClients = (stats["rtmp"]["server"]["application"][1]
                                        ["live"]["nclients"])
                stats = (stats["rtmp"]["server"]["application"][1]
                              ["live"]["stream"])
            except Exception as e:
                print(e)

            if isinstance(stats, collections.OrderedDict):
                statList.append(stats)
            elif isinstance(stats, list):
                statList = stats

            try:
                with self.dictLock:
                    ''' Clear status '''
                    for stream in self.rS:
                        self.rS[stream]["status"] = 0
                    ''' Update status '''
                    for stat in statList:
                        if stat["name"] in self.rS:
                            self.rS[stat["name"]]["InBW"] = int(stat["bw_in"])
                            self.rS[stat["name"]]["status"] = 1
            except Exception as e:
                print(e)
            time.sleep(0.5)

    def resetrevived(self, streamId):
        with self.dictLock:
            try:
                self.rS[streamId]["revived"] = 0
            except Exception as e:
                print(e)

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
                              "stream_id": streamId, "origin_id": self.origin_ID}
                print("Publishing")
                print(streamDict)
                self.mqttc.publish("db/origin/ffmpeg/respawn",
                                   json.dumps(streamDict))
            time.sleep(1)

    def logger(self):
        ''' Replace with publisher here '''
        while(True):
            ''' TODO: Send status on a per stream basis here '''
            self.mqttc.publish("origin/stat",
                               json.dumps({"origin_id": self.origin_ID,
                                           "num_clients": str(self.numClients)}))
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
        self.mqttc.run()
        time.sleep(0.5)
        self.mqttc.publish("request/origin/streams", json.dumps({"origin_ip": self.origin_IP}))
        while(self.startFlag is False):
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
                    if (self.rS):
                        for stream in self.rS:
                            ''' For printing '''
                            obj = self.rS[stream]
            except Exception as e:
                print(e)
            time.sleep(2)


if __name__ == "__main__":
    statPageURL = "http://localhost:8080/stat"
    mqtt_ip = os.environ["LB_IP"]
    mqtt_port = os.environ["MQTT_PORT"]
    mqttTopics = [("origin/ffmpeg/stream/stat/spawn", 0),
                  ("origin/ffmpeg/kill", 0),
                  ("lb/request/origin/streams", 0),
                  ("origin/ffmpeg/killall", 0)]
    ''' TODO: Parameterize tsdb params '''
    tsDBParams = {"url": "127.0.0.1", "port": 8086,
                  "uname": "root", "pwd": "root", "appName": "statter"}
    statter = Statter(tsDBParams, statPageURL, mqtt_ip, mqtt_port, mqttTopics)

    statter.start()

