import paho.mqtt.client as mqtt
import os
import sys
import signal
import time
import requests
import socket
import subprocess as sp
import threading
from MQTTPubSub import MQTTPubSub


#Flags
origin_ffmpeg_spawn=[0,""]
origin_ffmpeg_dist=[0,""]


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
		global origin_ffmpeg_spawn, origin_ffmpeg_dist
		msg=str(message.payload.decode("utf-8")).split()
		topic=str(message.topic.decode("utf-8"))
		s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		s.connect(("8.8.8.8", 80))
		if msg[0]==str(s.getsockname()[0]):
			if topic=="origin/ffmpeg/stream/spawn":
				origin_ffmpeg_spawn[0]=1
				origin_ffmpeg_spawn[1]=str(msg[0])+" "+str(msg[1])+" "+str(msg[2])
			elif topic=="origin/ffmpeg/dist/spawn":
				origin_ffmpeg_dist[0]=1
				origin_ffmpeg_dist[1]=str(msg[0])+" "+str(msg[1])+" "+str(msg[2])



#MQTT Params
mqttServerParams = {}
mqttServerParams["url"] = "10.156.14.141"
mqttServerParams["port"] = 1883
mqttServerParams["timeout"] = 60
mqttServerParams["topic"] = [("origin/ffmpeg/stream/spawn",0),("origin/ffmpeg/dist/spawn",0)]
mqttServerParams["onMessage"] = on_message
client = MQTTPubSub(mqttServerParams)


if __name__=="__main__":
	t1=DefunctCleaner()
	t1.setDaemon(True)
	t1.start()
	client.run()
	FNULL = open(os.devnull, 'w')
	while(True):
		if origin_ffmpeg_spawn[0]==1:
			msg=origin_ffmpeg_spawn[1].split()
			cmd=["nohup","/usr/bin/ffmpeg", "-i", "rtsp://"+str(msg[2])+"/h264", "-an", "-vcodec", "copy", "-f","flv", "rtmp://"+str(msg[0]).strip()+":1935/dynamic/"+str(msg[1]).strip(),"&"]
			print " ".join(cmd)
			proc=sp.Popen(" ".join(cmd),stdout=FNULL, stderr=FNULL,stdin=FNULL,shell=True,preexec_fn=os.setpgrp)
			print "FFMPEG spawned for "+str(cmd[3])+"----------->"+str(cmd[-2])
			client.publish("db/origin/ffmpeg/stream/spawn"," ".join(cmd) + " "+str(proc.pid)+ " "+ str(msg[0])+" "+str(msg[1])+" "+str(msg[2]))
			origin_ffmpeg_spawn=[0,""]
		elif origin_ffmpeg_dist[0]==1:
			msg=origin_ffmpeg_dist[1].split()
			cmd=["nohup","/usr/bin/ffmpeg", "-i", "rtmp://"+str(msg[0]).strip()+":1935/dynamic/"+str(msg[2]).strip(), "-an", "-vcodec", "copy", "-f","flv", "rtmp://"+str(msg[1]).strip()+":1935/dynamic/"+str(msg[2]).strip(),"&"]
			proc=sp.Popen(" ".join(cmd),stdin=FNULL,stdout=FNULL, stderr=FNULL,shell=True,preexec_fn=os.setpgrp)
			print "FFMPEG spawned for "+str(cmd[3])+"----------->"+str(cmd[-2])
			print " ".join(cmd)
			client.publish("db/origin/ffmpeg/dist/spawn"," ".join(cmd) + " "+str(proc.pid)+ " "+ str(msg[0])+" "+str(msg[1])+" "+str(msg[2]))
			origin_ffmpeg_dist=[0,""]

	



	

