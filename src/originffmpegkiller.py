import paho.mqtt.client as mqtt
import socket
import psutil
import signal
import os
import subprocess as sp
import time

origin_ffmpeg_kill=[0,""]
origin_ffmpeg_killall=[0,""]

def get_pname(id):
		p = sp.Popen(["ps -o cmd= {}".format(id)], stdout=sp.PIPE, shell=True)
		return str(p.communicate()[0])

def on_message(client, userdata, message):
	msg=str(message.payload.decode("utf-8")).split()
	topic=str(message.topic.decode("utf-8"))
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	s.connect(("8.8.8.8", 80))
	print msg,topic
	if msg[0]==str(s.getsockname()[0]):
		if topic=="origin/ffmpeg/kill":
			origin_ffmpeg_kill[0]=1
			origin_ffmpeg_kill[1]=msg[1]
		elif topic=="origin/ffmpeg/killall":
			origin_ffmpeg_killall[0]=1
		else:
			pass
	s.close()




if __name__=="__main__":
	broker_address="10.156.14.141"
	client = mqtt.Client("ffmpegcleaner")
	client.connect(broker_address)
	client.on_message=on_message #connect to broker
	client.loop_start()
	client.subscribe([("origin/ffmpeg/kill",0),("origin/ffmpeg/killall",0)])
	while(True):
		if origin_ffmpeg_kill[0]==1:
			os.kill(int(origin_ffmpeg_kill[1]),signal.SIGTERM)
			time.sleep(1)
			origin_ffmpeg_kill=[0,""]
		elif origin_ffmpeg_killall[0]==1:
			allprocs=psutil.pids()
			print allprocs
			for i in allprocs:
				print get_pname(i)
				if "/usr/bin/ffmpeg" in get_pname(i):
					os.kill(int(i),signal.SIGTERM)
					time.sleep(1)
			origin_ffmpeg_killall=[0,""]
		else:
			continue
	client.loop_stop()
