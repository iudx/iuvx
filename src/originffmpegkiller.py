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
        self.mqParams["topic"] = [("origin/ffmpeg/kill", 1),
                                  ("origin/ffmpeg/killall", 1)]
        self.mqParams["onMessage"] = self.on_message
        self.client = MQTTPubSub(self.mqParams)
        self.client.run()

    def on_message(self, client, userdata, message):
        ''' MQTT Callback function '''
        self.msg = message.payload
        msg_dict = json.loads(self.msg)
        print(msg_dict)
        print(self.origin_id)
        try: 
            if isinstance(msg_dict, list): 
                if msg_dict[0]["origin_id"] != self.origin_id:
                    return
            elif msg_dict["origin_id"] != self.origin_id:
                return

            res = oc.OriginFfmpegKillAll.delay(self.msg)
            threading.Thread(target=self.monitorTaskResult,
                             args=(res,)).start()

        except Exception as e:
            print(e)

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


if __name__ == "__main__":
    main()
