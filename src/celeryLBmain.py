import loadbalancercelery as lbc
import os
import sys
import time
from MQTTPubSub import MQTTPubSub
import json
import threading


class LB():
    ''' Load Balancer Router Class '''
    def __init__(self, mqtt_ip, mqtt_port):
        ''' Init the router '''
        self.action = "idle"
        self.msg = ""
        # MQTT Params
        self.mqParams = {}
        self.mqParams["url"] = mqtt_ip
        self.mqParams["port"] = mqtt_port
        self.mqParams["timeout"] = 60
        self.mqParams["topic"] = [("origin/get", 0), ("dist/get", 0),
                                  ("archive/get", 0), ("stream/get", 0),
                                  ("user/get", 0), ("verify/user", 0),
                                  ("user/add", 0), ("user/del", 0),
                                  ("db/dist/ffmpeg/respawn", 0),
                                  ("archive/delete", 0), ("archive/add", 0),
                                  ("request/allstreams", 0),
                                  ("db/origin/ffmpeg/respawn", 0),
                                  ("origin/stat", 0), ("dist/stat", 0),
                                  ("origin/add", 0), ("origin/delete", 0),
                                  ("dist/add", 0), ("dist/delete", 0),
                                  ("stream/add", 0), ("stream/delete", 0),
                                  ("stream/request", 0),
                                  ("db/origin/ffmpeg/dist/spawn", 0),
                                  ("db/origin/ffmpeg/stream/spawn", 0)]
        self.mqParams["onMessage"] = self.on_message
        self.client = MQTTPubSub(self.mqParams)

    def on_message(self, client, userdata, message):
        ''' MQTT Callback function '''
        self.msg = str(message.payload.decode("utf-8"))
        self.action = str(message.topic.decode("utf-8"))

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
                        if ((retDict["topic"] is
                                "origin/ffmpeg/stream/stat/spawn") or
                            (retDict["topic"] is
                                "dist/ffmpeg/stream/stat/spawn")):
                            time.sleep(30)
                        self.client.publish(retDict["topic"],
                                            json.dumps(retDict["msg"]))
                        time.sleep(0.1)
                break

    def router(self):
        ''' Router '''
        while(True):

            if self.action == "request/origin/streams":
                res = lbc.ReqAllOriginStreams.delay(self.msg)
                threading.Thread(target=self.monitorTaskResult,
                                 args=(res,)).start()

            if self.action == "request/dist/streams":
                res = lbc.ReqAllDistStreams.delay(self.msg)
                threading.Thread(target=self.monitorTaskResult,
                                 args=(res,)).start()

            if self.action == "origin/add":
                res = lbc.InsertOrigin.delay(self.msg)
                threading.Thread(target=self.monitorTaskResult,
                                 args=(res,)).start()

            if self.action == "origin/delete":
                res = lbc.DeleteOrigin.delay(self.msg)
                threading.Thread(target=self.monitorTaskResult,
                                 args=(res,)).start()

            if self.action == "origin/stat":
                ''' TODO: Why no ret '''
                lbc.OriginStat.delay(self.msg)

            if self.action == "origin/get":
                res = lbc.GetOrigins.delay()
                threading.Thread(target=self.monitorTaskResult,
                                 args=(res,)).start()

            if self.action == "db/origin/ffmpeg/stream/spawn":
                res = lbc.UpdateOriginStream.delay(self.msg)
                threading.Thread(target=self.monitorTaskResult,
                                 args=(res,)).start()

            if self.action == "dist/add":
                res = lbc.InsertDist.delay(self.msg)
                threading.Thread(target=self.monitorTaskResult,
                                 args=(res,)).start()

            if self.action == "dist/delete":
                res = lbc.DeleteDist.delay(self.msg)
                threading.Thread(target=self.monitorTaskResult,
                                 args=(res,)).start()

            if self.action == "dist/get":
                res = lbc.GetDists.delay()
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

            if self.action == "stream/add":
                res = lbc.InsertStream.delay(self.msg)
                threading.Thread(target=self.monitorTaskResult,
                                 args=(res,)).start()

            if self.action == "stream/delete":
                res = lbc.DeleteStream.delay(self.msg)
                threading.Thread(target=self.monitorTaskResult,
                                 args=(res,)).start()

            if self.action == "stream/request":
                res = lbc.RequestStream.delay(self.msg)
                threading.Thread(target=self.monitorTaskResult,
                                 args=(res,)).start()

            if self.action == "stream/get":
                res = lbc.GetStreams.delay()
                threading.Thread(target=self.monitorTaskResult,
                                 args=(res,)).start()

            if self.action == "archive/add":
                res = lbc.ArchiveAdd.delay(self.msg)
                threading.Thread(target=self.monitorTaskResult,
                                 args=(res,)).start()

            if self.action == "archive/delete":
                res = lbc.ArchiveDel.delay(self.msg)
                threading.Thread(target=self.monitorTaskResult,
                                 args=(res,)).start()

            if self.action == "archve/get":
                res = lbc.GetArchives.delay()
                threading.Thread(target=self.monitorTaskResult,
                                 args=(res,)).start()

            if self.action == "user/get":
                res = lbc.GetUsers.delay(self.msg)
                threading.Thread(target=self.monitorTaskResult,
                                 args=(res,)).start()

            if self.action == "user/add":
                res = lbc.AddUser.delay(self.msg)
                threading.Thread(target=self.monitorTaskResult,
                                 args=(res,)).start()

            if self.action == "user/del":
                res = lbc.DelUser.delay(self.msg)
                threading.Thread(target=self.monitorTaskResult,
                                 args=(res,)).start()

            if self.action == "verify/user":
                res = lbc.VerifyUser.delay(self.msg)
                threading.Thread(target=self.monitorTaskResult,
                                 args=(res,)).start()

            else:
                self.action = "idle"
                self.msg = ""
                continue
            time.sleep(0.001)


def main():
    mqtt_ip = os.environ["LB_IP"]
    mqtt_port = os.environ["LB_PORT"]
    if mqtt_ip is None or mqtt_port is None:
        print("Error! LB_IP and LB_PORT not set")
        sys.exit(0)
    lb = LB(mqtt_ip, mqtt_port)
    lb.client.run()
    lb.router()


if __name__ == "__main__":
    main()
