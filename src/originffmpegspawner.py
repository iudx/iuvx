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

import json
#Flags
origin_ffmpeg_spawn=[0,""]
origin_ffmpeg_dist=[0,""]
origin_ffmpeg_respawn=[0,""]
origin_ffmpeg_dist_respawn=[0,""]

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
				origin_ffmpeg_spawn[0]=1
				origin_ffmpeg_spawn[1]=ddict
			if topic=="origin/ffmpeg/dist/spawn":
				origin_ffmpeg_dist[0]=1
				origin_ffmpeg_dist[1]=ddict
			if topic=="origin/ffmpeg/respawn":
				origin_ffmpeg_respawn[0]=1
				origin_ffmpeg_respawn[1]=ddict
			if topic=="origin/ffmpeg/dist/respawn":
				origin_ffmpeg_dist_respawn[0]=1
				origin_ffmpeg_dist_respawn[1]=ddict




#MQTT Params
mqttServerParams = {}
mqttServerParams["url"] = "10.156.14.141"
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
	FNULL = open(os.devnull, 'w')
	while(True):
		if origin_ffmpeg_spawn[0]==1:
			msg=origin_ffmpeg_spawn[1]
			cmd=["nohup","/usr/bin/ffmpeg", "-i", "rtsp://"+str(msg["Stream_IP"])+"/h264", "-an", "-vcodec", "copy", "-f","flv", "rtmp://"+str(msg["Origin_IP"]).strip()+":1935/dynamic/"+str(msg["Stream_ID"]).strip(),"&"]
			print " ".join(cmd)
			proc=sp.Popen(" ".join(cmd),stdout=FNULL, stderr=FNULL,stdin=FNULL,shell=True,preexec_fn=os.setpgrp)
			print "FFMPEG spawned for "+str(cmd[3])+"----------->"+str(cmd[-2])
			senddict={"CMD":" ".join(cmd),"FROM_IP":msg["Stream_IP"],"Stream_ID":msg["Stream_ID"],"TO_IP":msg["Origin_IP"]}
			client.publish("db/origin/ffmpeg/stream/spawn",json.dumps(senddict))
			# col.insert_one(msg)
			origin_ffmpeg_spawn=[0,""]
		elif origin_ffmpeg_dist[0]==1:
			msg=origin_ffmpeg_dist[1]
			cmd=["nohup","/usr/bin/ffmpeg", "-i", "rtmp://"+str(msg["Origin_IP"]).strip()+":1935/dynamic/"+str(msg["Stream_ID"]).strip(), "-an", "-vcodec", "copy", "-f","flv", "rtmp://"+str(msg["Dist_IP"]).strip()+":1935/dynamic/"+str(msg["Stream_ID"]).strip(),"&"]
			proc=sp.Popen(" ".join(cmd),stdin=FNULL,stdout=FNULL, stderr=FNULL,shell=True,preexec_fn=os.setpgrp)
			print "FFMPEG spawned for "+str(cmd[3])+"----------->"+str(cmd[-2])
			print " ".join(cmd)
			senddict={"CMD":" ".join(cmd),"FROM_IP":msg["Origin_IP"],"Stream_ID":msg["Stream_ID"],"TO_IP":msg["Dist_IP"]}
			client.publish("db/origin/ffmpeg/dist/spawn",json.dumps(senddict))
			# col.insert_one(msg)
			origin_ffmpeg_dist=[0,""]
		elif origin_ffmpeg_respawn[0]:
			print "Entered respawner"
			print origin_ffmpeg_respawn[1]
			# # for msg in origin_ffmpeg_respawn[1]["Stream_list"]:
			# 	cmd=["nohup","/usr/bin/ffmpeg", "-i", "rtsp://"+str(msg["Stream_IP"])+"/h264", "-an", "-vcodec", "copy", "-f","flv", "rtmp://"+str(msg["Origin_IP"]).strip()+":1935/dynamic/"+str(msg["Stream_ID"]).strip(),"&"]
			# 	print " ".join(cmd)
			# 	proc=sp.Popen(" ".join(cmd),stdout=FNULL, stderr=FNULL,stdin=FNULL,shell=True,preexec_fn=os.setpgrp)
			# 	print "FFMPEG spawned for "+str(cmd[3])+"----------->"+str(cmd[-2])
			# 	senddict={"CMD":" ".join(cmd),"FROM_IP":msg["Stream_IP"],"Stream_ID":msg["Stream_ID"],"TO_IP":msg["Origin_IP"]}
			# 	client.publish("db/origin/ffmpeg/stream/spawn",json.dumps(senddict))
			msg=origin_ffmpeg_respawn[1]
			cmd=["nohup","/usr/bin/ffmpeg", "-i", "rtsp://"+str(msg["Stream_IP"])+"/h264", "-an", "-vcodec", "copy", "-f","flv", "rtmp://"+str(msg["Origin_IP"]).strip()+":1935/dynamic/"+str(msg["Stream_ID"]).strip(),"&"]
			print " ".join(cmd)
			proc=sp.Popen(" ".join(cmd),stdout=FNULL, stderr=FNULL,stdin=FNULL,shell=True,preexec_fn=os.setpgrp)
			print "FFMPEG spawned for "+str(cmd[3])+"----------->"+str(cmd[-2])
			senddict={"CMD":" ".join(cmd),"FROM_IP":msg["Stream_IP"],"Stream_ID":msg["Stream_ID"],"TO_IP":msg["Origin_IP"]}
			client.publish("db/origin/ffmpeg/stream/sspawn",json.dumps(senddict))
			origin_ffmpeg_respawn=[0,""]
		elif origin_ffmpeg_dist_respawn[0]==1:
			msg=origin_ffmpeg_dist_respawn[1]
			cmd=["nohup","/usr/bin/ffmpeg", "-i", "rtmp://"+str(msg["Origin_IP"]).strip()+":1935/dynamic/"+str(msg["Stream_ID"]).strip(), "-an", "-vcodec", "copy", "-f","flv", "rtmp://"+str(msg["Dist_IP"]).strip()+":1935/dynamic/"+str(msg["Stream_ID"]).strip(),"&"]
			proc=sp.Popen(" ".join(cmd),stdin=FNULL,stdout=FNULL, stderr=FNULL,shell=True,preexec_fn=os.setpgrp)
			print "FFMPEG spawned for "+str(cmd[3])+"----------->"+str(cmd[-2])
			print " ".join(cmd)
			senddict={"CMD":" ".join(cmd),"FROM_IP":msg["Origin_IP"],"Stream_ID":msg["Stream_ID"],"TO_IP":msg["Dist_IP"]}
			client.publish("db/origin/ffmpeg/dist/spawn",json.dumps(senddict))
			# col.insert_one(msg)
			origin_ffmpeg_dist_respawn=[0,""]
		



	



	

