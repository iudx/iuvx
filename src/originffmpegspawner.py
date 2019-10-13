import os
import sys
import time
import threading
from MQTTPubSub import MQTTPubSub
import OriginCelery as oc
import json


class Origin():
    def __init__(self, origin_id, mqtt_ip, mqtt_port):
        ''' Init the router '''
        self.origin_id = origin_id
        self.action = "idle"
        self.msg = ""
        self.mqParams = {}
        self.mqParams["url"] = mqtt_ip
        self.mqParams["port"] = int(mqtt_port)
        self.mqParams["timeout"] = 60
        self.mqParams["topic"] = [("origin/ffmpeg/dist/respawn", 0),
                                  ("origin/ffmpeg/stream/spawn", 0),
                                  ("origin/ffmpeg/dist/spawn", 0),
                                  ("origin/ffmpeg/respawn", 0)]
        self.mqParams["onMessage"] = self.on_message
        self.client = MQTTPubSub(self.mqParams)

    def router(self):
        while(True):
            if self.action == "origin/ffmpeg/stream/spawn":
                res = oc.OriginFfmpegSpawn.delay(self.msg)
                threading.Thread(target=self.monitorTaskResult,
                                 args=(res,)).start()

            if self.action == "origin/ffmpeg/dist/spawn":
                res = oc.OriginFfmpegDistSpawn.delay(self.msg)
                threading.Thread(target=self.monitorTaskResult,
                                 args=(res,)).start()

            if self.action == "origin/ffmpeg/dist/respawn":
                res = oc.OriginFfmpegDistRespawn.delay(self.msg)
                threading.Thread(target=self.monitorTaskResult,
                                 args=(res,)).start()

            else:
                self.action = "idle"
                self.msg = ""
                continue
            self.action = "idle"
            self.msg = ""
            time.sleep(0.001)

    def on_message(self, client, userdata, message):
        ''' MQTT Callback function '''
        self.msg = message.payload.decode("utf-8")
        self.msg = json.loads(self.msg)
        if self.msg["origin_id"] == self.origin_id:
            self.action = message.topic.decode("utf-8")

    def monitorTaskResult(self, res):
        ''' Celery task monitor '''
        while(True):
            if res.ready():
                ret = res.get()
                if not ret:
                    pass
                elif isinstance(ret, dict):
                    self.client.publish(ret["topic"],
                                        json.dumps(ret["msg"]))
                    time.sleep(0.1)
                elif isinstance(ret, list):
                    for retDict in ret:
                        self.client.publish(retDict["topic"], retDict["msg"])
                        time.sleep(30)
                break


class DefunctCleaner(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self, target=sys.exit)

    def run(self):
        while(True):
            try:
                os.waitpid(-1, os.WNOHANG)
            except Exception as exc:
                continue


def main():
    mqtt_ip = os.environ["LB_IP"]
    mqtt_port = os.environ["MQTT_PORT"]
    if mqtt_ip is None or mqtt_port is None:
        print("Error! LB_IP and LB_PORT not set")
        sys.exit(0)
    origin_id = os.environ["ORIGIN_ID"]
    origin = Origin(origin_id, mqtt_ip, mqtt_port)
    t1 = DefunctCleaner()
    t1.setDaemon(True)
    t1.start()
    origin.router()


if __name__ == "__main__":
    main()
