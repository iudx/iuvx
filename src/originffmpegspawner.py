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
        self.ORIGIN_ID = origin_id
        self.action = "idle"
        self.msg = ""
        self.mqParams = {}
        self.mqParams["url"] = mqtt_ip
        self.mqParams["username"] = mqtt_uname
        self.mqParams["password"] = mqtt_passwd
        self.mqParams["port"] = int(mqtt_port)
        self.mqParams["timeout"] = 60
        self.mqParams["topic"] = [("origin/ffmpeg/dist/respawn", 1),
                                  ("origin/ffmpeg/stream/spawn", 1),
                                  ("origin/ffmpeg/dist/spawn", 1),
                                  ("origin/ffmpeg/respawn", 1)]
        self.mqParams["onMessage"] = self.on_message
        self.client = MQTTPubSub(self.mqParams)

    def on_message(self, client, userdata, message):

        msg = message.payload.decode("UTF-8")
        action = message.topic
        msg_dict = json.loads(msg)
        print(msg_dict)

        if msg_dict["origin_id"] != self.ORIGIN_ID:
            print("Oh no returning")
            return

        if action == "origin/ffmpeg/stream/spawn":
            res = oc.OriginFfmpegSpawn.delay(msg)
            threading.Thread(target=self.monitorTaskResult,
                             args=(res,)).start()

        if action == "origin/ffmpeg/dist/spawn":
            res = oc.OriginFfmpegDistSpawn.delay(msg)
            threading.Thread(target=self.monitorTaskResult,
                             args=(res,)).start()

        if action == "origin/ffmpeg/dist/respawn":
            res = oc.OriginFfmpegDistRespawn.delay(msg)
            threading.Thread(target=self.monitorTaskResult,
                             args=(res,)).start()

        action = "idle"
        msg = ""

    def defunct_cleaner(self):
        refresh_every = 10 # seconds
        while(True):
            time.sleep(refresh_every)
            oc.CleanProcessTable()


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
    clean_thread = threading.Thread(target=origin.defunct_cleaner)
    clean_thread.start()
    origin.client.run()


if __name__ == "__main__":
    main()
