import paho.mqtt.client as mqtt
import socket
import psutil
import signal
import os
import subprocess as sp
import time
import json
from MQTTPubSub import MQTTPubSub


origin_ffmpeg_kill=[0,""]
origin_ffmpeg_killall=[0,""]



def on_message(client, userdata, message):
	msg=str(message.payload.decode("utf-8"))
	topic=str(message.topic.decode("utf-8"))
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	s.connect(("8.8.8.8", 80))
	ddict=json.loads(msg)
	print ddict,topic
	if ddict["Origin_IP"]==str(s.getsockname()[0]):
		if topic=="origin/ffmpeg/kill":
			print ddict["CMD"]
			origin_ffmpeg_kill[0]=1
			origin_ffmpeg_kill[1]=ddict
		elif topic=="origin/ffmpeg/killall":
			origin_ffmpeg_killall[0]=1
		else:
			pass
	s.close()


#MQTT Params
mqttServerParams = {}
mqttServerParams["url"] = "10.156.14.141"
mqttServerParams["port"] = 1883
mqttServerParams["timeout"] = 60
mqttServerParams["topic"] = [("origin/ffmpeg/kill",0),("origin/ffmpeg/killall",0)]
mqttServerParams["onMessage"] = on_message
client = MQTTPubSub(mqttServerParams)

if __name__=="__main__":
	FNULL = open(os.devnull, 'w')
	client.run()
	print "Started"
	while(True):
		if origin_ffmpeg_kill[0]==1:
			print origin_ffmpeg_kill[1]["CMD"].split()[1:-1]
			sp.Popen(["pkill","-f"," ".join(origin_ffmpeg_kill[1]["CMD"].split()[1:-1])],stdin=FNULL,stdout=FNULL,stderr=FNULL,shell=False)
			time.sleep(1)
			origin_ffmpeg_kill=[0,""]
		elif origin_ffmpeg_killall[0]==1:
			sp.Popen(["pkill","-f","/usr/bin/ffmpeg"],stdin=FNULL,stdout=FNULL,stderr=FNULL,shell=False)
			time.sleep(1)
			origin_ffmpeg_killall=[0,""]
		else:
			continue
