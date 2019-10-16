import os
import sys
import time
import threading
from MQTTPubSub import MQTTPubSub
import OriginCelery as oc
import json


class Origin():
    def __init__(self, origin_id, mqtt_ip, mqtt_port, mqtt_uname, mqtt_passwd):
        ''' Init the router '''
        self.origin_id = origin_id
        self.action = "idle"
        self.msg = ""
        self.mqParams = {}
        self.mqParams["url"] = mqtt_ip
        self.mqParams["username"] = mqtt_uname
        self.mqParams["password"] = mqtt_passwd
        self.mqParams["port"] = mqtt_port
        self.mqParams["timeout"] = 60
        self.mqParams["topic"] = [("origin/ffmpeg/dist/respawn", 1),
                                  ("origin/ffmpeg/stream/spawn", 1),
                                  ("origin/ffmpeg/dist/spawn", 1),
                                  ("origin/ffmpeg/respawn", 1)]
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

            self.action = "idle"
            self.msg = ""
            time.sleep(0.01)

    def on_message(self, client, userdata, message):
        ''' MQTT Callback function '''
        self.msg = message.payload
        msg_dict = json.loads(self.msg)
        if msg_dict["origin_id"] == self.origin_id:
            self.action = message.topic
            print(self.action, self.msg)

    def monitorTaskResult(self, res):
        ''' Celery task monitor '''
        while(True):
            if res.ready():
                ret = res.get()
                if not ret:
                    pass
                elif isinstance(ret, dict):
                    self.client.publish(ret["topic"], ret["msg"])
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
    mqtt_uname = os.environ["MQTT_UNAME"]
    mqtt_passwd = os.environ["MQTT_PASSWD"]
    if mqtt_ip is None or mqtt_port is None:
        print("Error! LB_IP and LB_PORT not set")
        sys.exit(0)
    origin_id = os.environ["ORIGIN_ID"]
    origin = Origin(origin_id, mqtt_ip, mqtt_port, mqtt_uname, mqtt_passwd)
    t1 = DefunctCleaner()
    t1.setDaemon(True)
    t1.start()
    origin.client.run()
    origin.router()


if __name__ == "__main__":
    main()
