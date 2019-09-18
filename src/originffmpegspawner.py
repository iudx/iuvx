import os
import sys
import signal
import time
import requests
import socket
import subprocess as sp
import threading
from MQTTPubSub import MQTTPubSub
import OriginCelery as oc
import json
import threading
#Flags
origin_ffmpeg_spawn=[0,""]
origin_ffmpeg_dist=[0,""]
origin_ffmpeg_respawn=[0,""]
origin_ffmpeg_dist_respawn=[0,""]

def monitorTaskResult(res):
	global client
	while(True):
		if res.ready():
			retval=res.get()
			if retval:
				if isinstance(retval,dict):
					client.publish(retval["topic"],json.dumps(retval["ddict"]))
					time.sleep(0.1)
				elif isinstance(retval,list):
					for i in retval:
						client.publish(u["topic"],json.dumps(i["ddict"]))
						time.sleep(30)
			break


class DefunctCleaner(threading.Thread):
  def __init__(self):
	threading.Thread.__init__(self,target=sys.exit)
  def run(self):
	while(True):
		try:
			os.waitpid(-1, os.WNOHANG)
		except Exception as exc:
			continue





def on_message(client, userdata, message):
		global origin_ffmpeg_respawn, origin_ffmpeg_dist_respawn, origin_ffmpeg_spawn, origin_ffmpeg_dist
		msg=str(message.payload.decode("utf-8"))
		topic=str(message.topic.decode("utf-8"))
		s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		s.connect(("8.8.8.8", 80))
		ddict=json.loads(msg)
		if ddict["Origin_IP"]==str(s.getsockname()[0]):
			if topic=="origin/ffmpeg/stream/spawn":
				print topic,msg
				origin_ffmpeg_spawn[0]=1
				origin_ffmpeg_spawn[1]=ddict
			if topic=="origin/ffmpeg/dist/spawn":
				print topic,msg
				origin_ffmpeg_dist[0]=1
				origin_ffmpeg_dist[1]=ddict
			if topic=="origin/ffmpeg/respawn":
				print topic,msg
				origin_ffmpeg_respawn[0]=1
				origin_ffmpeg_respawn[1]=ddict
			if topic=="origin/ffmpeg/dist/respawn":
				print topic,msg
				origin_ffmpeg_dist_respawn[0]=1
				origin_ffmpeg_dist_respawn[1]=ddict




#MQTT Params
mqttServerParams = {}
mqttServerParams["url"] = "10.156.14.138"
mqttServerParams["port"] = 1883
mqttServerParams["timeout"] = 60
mqttServerParams["topic"] = [("origin/ffmpeg/dist/respawn",0),("origin/ffmpeg/stream/spawn",0),("origin/ffmpeg/dist/spawn",0),("origin/ffmpeg/respawn",0)]
mqttServerParams["onMessage"] = on_message
client = MQTTPubSub(mqttServerParams)


if __name__=="__main__":
	t1=DefunctCleaner()
	t1.setDaemon(True)
	t1.start()
	client.run()
	
	while(True):
		if origin_ffmpeg_spawn[0]==1:
			res=oc.OriginFfmpegSpawn.delay(origin_ffmpeg_spawn)
			threading.Thread(target=monitorTaskResult,args=(res,)).start()
			origin_ffmpeg_spawn=[0,""]
		elif origin_ffmpeg_dist[0]==1:
			res=oc.OriginFfmpegDist.delay(origin_ffmpeg_dist)
			threading.Thread(target=monitorTaskResult,args=(res,)).start()
			origin_ffmpeg_dist=[0,""]
		elif origin_ffmpeg_respawn[0]:
			res=oc.OriginFfmpegRespawn.delay(origin_ffmpeg_respawn)
			threading.Thread(target=monitorTaskResult,args=(res,)).start()
			origin_ffmpeg_respawn=[0,""]
		elif origin_ffmpeg_dist_respawn[0]==1:
			res=oc.OriginFfmpegDistRespawn.delay(origin_ffmpeg_dist_respawn)
			threading.Thread(target=monitorTaskResult,args=(res,)).start()
			origin_ffmpeg_dist_respawn=[0,""]
		



	



	

