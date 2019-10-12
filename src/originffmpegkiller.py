import socket
import time
import json
from MQTTPubSub import MQTTPubSub
import OriginCelery as oc
import threading

origin_ffmpeg_kill = [0, ""]
origin_ffmpeg_killall = [0, ""]


''' TODO: Beautify later '''


def on_message(client, userdata, message):
    msg = str(message.payload.decode("utf-8"))
    topic = str(message.topic.decode("utf-8"))
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    msg = json.loads(msg)
    print(msg, topic)
    if isinstance(msg, list):
        for i in msg:
            if str(i["origin_ip"]) == str(s.getsockname()[0]):
                print(topic)
                if topic == "origin/ffmpeg/kill":
                    origin_ffmpeg_kill[0] = 1
                    origin_ffmpeg_kill[1] = msg
            break
    elif isinstance(msg, dict):
        if msg["origin_ip"] == str(s.getsockname()[0]):
            if topic == "origin/ffmpeg/killall":
                origin_ffmpeg_killall[0] = 1
    else:
        pass
    s.close()


def monitorTaskResult(res):
    global client
    while(True):
        if res.ready():
            retval = res.get()
            if retval:
                if isinstance(retval, dict):
                    client.publish(retval["topic"],
                                   json.dumps(retval["msg"]))
                    time.sleep(0.1)
                elif isinstance(retval, list):
                    for i in retval.keys():
                        client.publish(i["topic"], json.dumps(i["msg"]))
                        time.sleep(30)
            break


# MQTT Params
mqttServerParams = {}
mqttServerParams["url"] = "10.156.14.138"
mqttServerParams["port"] = 1883
mqttServerParams["timeout"] = 60
mqttServerParams["topic"] = [
    ("origin/ffmpeg/kill", 0), ("origin/ffmpeg/killall", 0)]
mqttServerParams["onMessage"] = on_message
client = MQTTPubSub(mqttServerParams)

if __name__ == "__main__":
    client.run()
    print("Started")
    while(True):
        if origin_ffmpeg_kill[0] == 1:
            res = oc.OriginFfmpegKill.delay(origin_ffmpeg_kill)
            threading.Thread(target=monitorTaskResult, args=(res,)).start()
            origin_ffmpeg_kill = [0, ""]
        elif origin_ffmpeg_killall[0] == 1:
            res = oc.OriginFfmpegKillAll.delay(origin_ffmpeg_killall)
            threading.Thread(target=monitorTaskResult, args=(res,)).start()
            origin_ffmpeg_killall = [0, ""]
        else:
            continue
