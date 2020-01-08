import requests
import xmltodict as xd
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
        self.registered_streams = {}
        self.missingsQ = Queue.Queue()
        self.startFlag = False
        self.waitPeriod = 30
        self.dictLock = threading.Lock()
        self.ORIGIN_IP = os.environ["ORIGIN_IP"]
        self.ORIGIN_ID = os.environ["ORIGIN_ID"]
        if(self.ORIGIN_ID is None):
            sys.exit(0)

        ''' MQTT Backend '''
        self.mqttServerParams = {}
        self.mqttServerParams["url"] = mqtt_ip
        self.mqttServerParams["port"] = int(mqtt_port)
        self.mqttServerParams["timeout"] = 60
        self.mqttServerParams["topic"] = mqttTopics
        self.mqttServerParams["username"] = mqtt_uname
        self.mqttServerParams["password"] = mqtt_passwd
        self.mqttServerParams["onMessage"] = self.on_message
        self.mqttServerParams["onConnect"] = self.on_connect
        self.mqttc = MQTTPubSub(self.mqttServerParams)
        self.numClients = 0

        ''' InfluxDB '''
        self.influxClient = InfluxDBClient(
            tsDBParams["url"], tsDBParams["port"],
            tsDBParams["uname"], tsDBParams["pwd"], tsDBParams["appName"])
        self.influxClient.create_database('statter')
        print("Initalization done")

    def addNewStream(self, stream_id, stream_ip, dbStatus):
        """ Add a stream to rS Registered Stream dict """
        with self.dictLock:
            print("Adding Stream \t" + str(stream_id))
            self.registered_streams[stream_id] = {"stream_ip": stream_ip,
                                  "status": dbStatus,
                                  "revived": 0,
                                  "Timer": None,
                                  "cmdTimerDuration": 15,
                                  "cmdCounter": 0,
                                  "InBW": 0}

    def deleteStream(self, streamId):
        with self.dictLock:
            print("Removing Stream \t" + str(streamId))
            del self.registered_streams[streamId]

    def on_connect(self, client, userdata, flags, rc):
        self.mqttc.publish("request/origin/streams", json.dumps({"origin_id": self.ORIGIN_ID}))
        print("Requesting all streams belonging to ", self.ORIGIN_ID)


    def on_message(self, client, userdata, registerd_streams):
        msg = registerd_streams.payload
        topic = registerd_streams.topic
        msgDict = json.loads(msg)
        if msgDict["origin_id"] != self.ORIGIN_ID:
            return
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
                            self.registered_streams = {}
                    if topic == "lb/request/origin/streams":
                        print("Initialized Streams")
                        if (msgDict["stream_list"]):
                            streamList = msgDict["stream_list"]
                            for stream in streamList:
                                self.addNewStream(stream["stream_id"], stream["stream_ip"], stream["status"])
                        self.startFlag = True
        except Exception as e:
            print("Couldn't decode response", e)

    def stat_loop(self):
        time.sleep(20)
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
                    allStreams = self.registered_streams.keys()
                    ''' Update status '''
                    if isinstance(statList, list):
                        for stat in statList:
                            try: 
                                """ Check only publishing streams """
                                if stat["name"] in self.registered_streams and "publishing" in stat.keys():
                                    s_name = stat["name"]
                                    presentStreams.append(stat["name"])
                                    self.registered_streams[stat["name"]]["InBW"] = int(stat["bw_in"])
                                    if (self.registered_streams[stat["name"]]["status"] == "down" or 
                                        self.registered_streams[stat["name"]]["status"] == "onboarding"):
                                        print("Found down/onboarding stream that is active")
                                        #Update the status in dB
                                        self.registered_streams[stat["name"]]["status"] = "active"
                                        print(self.registered_streams[stat["name"]]["status"], s_name)
                                        msg = {"stream_id": stat["name"], "status": "active"}
                                        print(json.dumps(msg))
                                        self.mqttc.publish("stream/stat", json.dumps(msg))
                                        if (self.registered_streams[s_name]["Timer"] != None):
                                            self.registered_streams[s_name]["Timer"].cancel()
                                            self.registered_streams[s_name]["Timer"] = None
                                            self.registered_streams[s_name]["cmdCounter"] = 0
                            except Exception as e:
                                print("No name in stat, error message ",e)
                    #print("Present Streams ", set(presentStreams))
                    for missing in list(set(allStreams) - set(presentStreams)):
                        print("Missing ", missing)
                        #print(self.registered_streams[missing]['status'])
                        if (self.registered_streams[missing]['status'] == "active" or self.registered_streams[missing]['status'] == "onboarding"):
                           print("Active becoming inactive")
                           self.registered_streams[missing]['status']  = "down"
                           msg = {"stream_id": missing, "status": "down"}
                           self.mqttc.publish("stream/stat", json.dumps(msg))

                        if (self.registered_streams[missing]["status"] == "down"):
                           #print(self.registered_streams[missing]['status'])
                           #If Timer object is not there, then initiate the timer and start
                           if (self.registered_streams[missing]["Timer"] == None):
                               #print("Starting Timer", self.registered_streams[missing]['status'])
                               self.registered_streams[missing]["cmdCounter"] = 0
                               #print(self.registered_streams[missing]["cmdTimerDuration"])
                               self.registered_streams[missing]["Timer"] = threading.Timer(self.registered_streams[missing]["cmdTimerDuration"],
                                                                         self.cmdTimerCallback,
                                                                         args=[missing])
                               self.registered_streams[missing]["Timer"].start()
                            

            except Exception as e:
                print("Couldn't read stat ", e)
            time.sleep(10)

    def cmdTimerCallback(self, streamid):
        #Timed Out, Re-issue spawning command
        print("Timed out, Trying spawning stream again: ", self.registered_streams[streamid]['stream_ip'])
        out = json.dumps({"origin_ip": self.ORIGIN_IP,
                          "origin_id": self.ORIGIN_ID,
                          "stream_id": streamid,
                          "stream_ip": self.registered_streams[streamid]["stream_ip"]})
        self.mqttc.publish("origin/ffmpeg/stream/spawn", out)
    
        #Start Timer again
        self.registered_streams[streamid]["cmdCounter"] = self.registered_streams[streamid]["cmdCounter"] + 1
        self.registered_streams[streamid]["Timer"] = threading.Timer(self.registered_streams[streamid]["cmdTimerDuration"],
                                                   self.cmdTimerCallback,
                                                   args=[streamid])
        self.registered_streams[streamid]["Timer"].start()
        return

    def resetrevived(self, streamId):
        with self.dictLock:
            try:
                self.registered_streams[streamId]["revived"] = 0
            except Exception as e:
                print("Couldn't  revive ", e)


    def logger(self):
        ''' Replace with publisher here '''
        time.sleep(20)
        while(True):
            ''' TODO: Send status on a per stream basis here '''
            msg = json.dumps({"origin_id": self.ORIGIN_ID,
                              "num_clients": str(self.numClients)})
            print(msg)
            self.mqttc.publish("origin/stat", msg)
            epochTime = int(time.time()) * 1000000000
            self.logDataFlag = False
            with self.dictLock:
                for stream in self.registered_streams:
                    series = []
                    ''' status '''
                    pointValues = {
                        "time": epochTime,
                        "measurement": "status",
                        'fields': {
                            'value': self.registered_streams[stream]["status"],
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
                            'value': self.registered_streams[stream]["InBW"],
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

        ''' Start the stat thread '''
        self.statThread = threading.Thread(target=self.stat_loop)
        self.statThread.start()
        ''' Logger '''
        self.loggerThread = threading.Thread(target=self.logger)
        self.loggerThread.start()

        self.mqttc.run()

def main():
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
    statter.mqttc.run()

if __name__ == "__main__":
    main()
