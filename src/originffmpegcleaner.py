import paho.mqtt.client as mqtt
import socket
import psutil
import signal
import os

def get_pname(id):
		p = sp.Popen(["ps -o cmd= {}".format(id)], stdout=sp.PIPE, shell=True)
		return str(p.communicate()[0])

def on_message(client, userdata, message):
	msg=str(message.payload.decode("utf-8")).split()
	topic=str(message.topic.decode("utf-8"))
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	s.connect(("8.8.8.8", 80))
	print "Enetred topic"
	print msg
	if msg[0]==str(s.getsockname()[0]):
		if topic=="origin/ffmpeg/kill":
			os.kill(int(msg[1]),signal.SIGTERM)
			time.sleep(1)
		elif topic=="origin/ffmpeg/killall":
			allprocs=psutil.pids()
			for i in allprocs:
				if "/usr/bin/ffmpeg" in get_pname(i):
					os.kill(i,signal.SIGTERM)
					time.sleep(1)
		else:
			pass
	s.close()




if __name__=="__main__":
	broker_address="localhost"
	client = mqtt.Client("ffmpegcleaner") 
	client.connect(broker_address)
	client.on_message=on_message #connect to broker
	client.loop_start()
	client.subscribe([("origin/ffmpeg/kill",0),("origin/ffmpeg/killall",0)])
	client.loop_forever()