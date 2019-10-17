import loadbalancercelery as lbc
import os
import sys
import time
from MQTTPubSub import MQTTPubSub
import threading

'''
    TODO:
        1. Check msg origin_id to perform action here
'''


class LB():
    ''' Load Balancer Router Class '''

    def __init__(self, mqtt_ip, mqtt_port, mqtt_uname, mqtt_passwd):
        ''' Init the router '''
        self.action = "idle"
        self.msg = ""
        # MQTT Params
        self.mqParams = {}
        self.mqParams["url"] = mqtt_ip
        self.mqParams["port"] = int(mqtt_port)
        self.mqParams["timeout"] = 60
        self.mqParams["username"] = mqtt_uname
        self.mqParams["password"] = mqtt_passwd
        self.mqParams["topic"] = [("origin/get", 1), ("dist/get", 1),
                                  ("archive/get", 1), ("stream/get", 1),
                                  ("user/get", 1), ("verify/user", 1),
                                  ("user/add", 1), ("user/del", 1),
                                  ("db/dist/ffmpeg/respawn", 1),
                                  ("archive/delete", 1), ("archive/add", 1),
                                  ("request/allstreams", 1),
                                  ("db/origin/ffmpeg/respawn", 1),
                                  ("origin/stat", 1), ("dist/stat", 1),
                                  ("origin/add", 1), ("origin/delete", 1),
                                  ("dist/add", 1), ("dist/delete", 1),
                                  ("stream/add", 1), ("stream/delete", 1),
                                  ("stream/request", 1),
                                  ("db/origin/ffmpeg/dist/spawn", 1),
                                  ("db/origin/ffmpeg/stream/spawn", 1)]
        self.mqParams["onMessage"] = self.on_message
        self.client = MQTTPubSub(self.mqParams)

    def on_message(self, client, userdata, message):
        ''' MQTT Callback function '''
        self.msg = message.payload
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
                    self.client.publish(ret["topic"],
                                        ret["msg"])
                elif isinstance(ret, list):
                    for retDict in ret:
                        self.client.publish(retDict["topic"],
                                            retDict["msg"])
                break

    def router(self):
        ''' Router '''
        while(True):

            if self.action == "request/dist/streams":
                ''' TODO: Use this '''
                res = lbc.ReqAllDistStreams.delay(self.msg)
                threading.Thread(target=self.monitorTaskResult,
                                 args=(res,)).start()

            if self.action == "origin/stat":
                lbc.OriginStat.delay(self.msg)

            if self.action == "db/origin/ffmpeg/stream/spawn":
                res = lbc.UpdateOriginStream.delay(self.msg)
                threading.Thread(target=self.monitorTaskResult,
                                 args=(res,)).start()

            if self.action == "dist/stat":
                ''' TODO: Why no ret '''
                lbc.DistStat.delay(self.msg)

            if self.action == "db/origin/ffmpeg/dist/spawn":
                res = lbc.OriginFfmpegDistPush.delay(self.msg)
                threading.Thread(target=self.monitorTaskResult,
                                 args=(res,)).start()

            if self.action == "db/origin/ffmpeg/respawn":
                res = lbc.OriginFfmpegRespawn.delay(self.msg)
                threading.Thread(target=self.monitorTaskResult,
                                 args=(res,)).start()

            if self.action == "db/dist/ffmpeg/respawn":
                ''' TODO: Message should come from diststatchecker '''
                res = lbc.OriginFFmpegDistRespawn.delay(self.msg)
                threading.Thread(target=self.monitorTaskResult,
                                 args=(res,)).start()

            if self.action == "request/origin/streams":
                res = lbc.ReqAllOriginStreams.delay(self.msg)
                threading.Thread(target=self.monitorTaskResult,
                                 args=(res,)).start()

            self.action = "idle"
            self.msg = ""
            time.sleep(0.001)


def main():
    mqtt_ip = os.environ["LB_IP"]
    mqtt_port = os.environ["MQTT_PORT"]
    mqtt_uname = os.environ["MQTT_UNAME"]
    mqtt_passwd = os.environ["MQTT_PASSWD"]
    if mqtt_ip is None or mqtt_port is None:
        print("Error! LB_IP and LB_PORT not set")
        sys.exit(0)
    lb = LB(mqtt_ip, mqtt_port, mqtt_uname, mqtt_passwd)
    lb.client.run()
    lb.router()


if __name__ == "__main__":
    main()
