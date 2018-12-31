import paho.mqtt.client as mqtt
import os
import sys
import signal
import time
import requests
import socket
import subprocess as sp
import threading

class DefunctCleaner(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self,target=sys.exit)
  def run(self):
	while(True):
		try:
			os.waitpid(-1, os.WNOHANG)
		except Exception as exc:
			continue

origin_ffmpeg_spawn=[0,""]
origin_ffmpeg_dist=[0,""]


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
if __name__=="__main__":
	t1=DefunctCleaner()
	t1.setDaemon(True)
	t1.start()
	broker_address="10.156.14.141"
	client = mqtt.Client("ffmpegspawner") 
	client.connect(broker_address)
	client.on_message=on_message #connect to broker
	client.loop_start()
	client.subscribe([("origin/ffmpeg/stream/spawn",0),("origin/ffmpeg/dist/spawn",0)])
	while(True):
		if origin_ffmpeg_spawn[0]==1:
			msg=origin_ffmpeg_spawn[1].split()
			cmd=["/usr/bin/ffmpeg", "-i", "rtsp://"+str(msg[2])+"/h264", "-an", "-vcodec", "copy", "-f","flv", "rtmp://"+str(msg[0]).strip()+":1935/dynamic/"+str(msg[1]).strip()]
			print " ".join(cmd)
			proc=sp.Popen(cmd,stdout=sp.PIPE, stderr=sp.PIPE,shell=False)
			print "FFMPEG spawned for "+str(cmd[2])+"----------->"+str(cmd[-1])
			client.publish("db/origin/ffmpeg/stream/spawn"," ".join(cmd) + " "+str(proc.pid)+ " "+ str(msg[0])+" "+str(msg[1])+" "+str(msg[2]))
			origin_ffmpeg_spawn=[0,""]
		elif origin_ffmpeg_dist[0]==1:
			msg=origin_ffmpeg_dist[1].split()
			cmd=["/usr/bin/ffmpeg", "-i", "rtmp://"+str(msg[0]).strip()+":1935/dynamic/"+str(msg[2]).strip(), "-an", "-vcodec", "copy", "-f","flv", "rtmp://"+str(msg[1]).strip()+":1935/dynamic/"+str(msg[2]).strip()]
			proc=sp.Popen(cmd,stdout=sp.PIPE, stderr=sp.PIPE,shell=False)
			print "FFMPEG spawned for "+str(cmd[2])+"----------->"+str(cmd[-1])
			print " ".join(cmd)
			client.publish("db/origin/ffmpeg/dist/spawn"," ".join(cmd) + " "+str(proc.pid)+ " "+ str(msg[0])+" "+str(msg[1])+" "+str(msg[2]))
			origin_ffmpeg_dist=[0,""]
	client.loop_stop()

	



	

