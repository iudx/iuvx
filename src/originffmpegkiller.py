import os
import sys
import time
import json
from MQTTPubSub import MQTTPubSub
import OriginCelery as oc
import threading


class OriginKiller():
    def __init__(self, origin_id, mqtt_ip, mqtt_port, mqtt_uname, mqtt_passwd):
        ''' Init the router '''
        self.origin_id = origin_id
        self.action = "idle"
        self.msg = ""
        self.mqParams = {}
        self.mqParams["url"] = mqtt_ip
        self.mqParams["port"] = int(mqtt_port)
        self.mqParams["timeout"] = 60
        self.mqParams["username"] = mqtt_uname
        self.mqParams["password"] = mqtt_passwd
        self.mqParams["topic"] = [("origin/ffmpeg/kill", 0),
                                  ("origin/ffmpeg/killall", 0)]
        self.mqParams["onMessage"] = self.on_message
        self.client = MQTTPubSub(self.mqParams)

    def router(self):
        while(True):
            if self.action == "origin/ffmpeg/kill":
                res = oc.OriginFfmpegKill.delay(self.msg)
                threading.Thread(target=self.monitorTaskResult,
                                 args=(res,)).start()

            if self.action == "origin/ffmpeg/killall":
                res = oc.OriginFfmpegKillAll.delay(self.msg)
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


def main():
    mqtt_ip = os.environ["LB_IP"]
    mqtt_port = os.environ["MQTT_PORT"]
    mqtt_uname = os.environ["MQTT_UNAME"]
    mqtt_passwd = os.environ["MQTT_PASSWD"]
    if mqtt_ip is None or mqtt_port is None:
        print("Error! LB_IP and LB_PORT not set")
        sys.exit(0)
    origin_id = os.environ["ORIGIN_ID"]
    originKiller = OriginKiller(origin_id, mqtt_ip, mqtt_port,
                                mqtt_uname, mqtt_passwd)
    originKiller.router()


if __name__ == "__main__":
    main()
