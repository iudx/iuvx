import paho.mqtt.client as mqtt
import os
import sys
import signal
import time
import requests
import subprocess as sp
from MQTTPubSub import MQTTPubSub
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
import json
import socket
import pymongo
from datetime import datetime
from datetime import timedelta

import OriginCelery as oc
origin_ffmpeg_archive=[0,""]
origin_ffmpeg_archive_del=[0,""]

import logging
log = logging.getLogger('apscheduler.executors.default')
log.setLevel(logging.INFO)  # DEBUG
fmt = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
h = logging.StreamHandler()
h.setFormatter(fmt)
log.addHandler(h)



s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect(("8.8.8.8", 80))
origin_ip=str(s.getsockname()[0])
s.close()

FNULL = open(os.devnull, 'w')



def time_diff(time1,time2):
 	fmt = '%H:%M:%S'
 	print time1, time2
	d2 = datetime.strptime(time2, fmt)
	d1 = datetime.strptime(time1, fmt)
	diff=d2-d1
	return int(diff.total_seconds())


def on_message(client, userdata, message):
	msg=str(message.payload.decode("utf-8"))
	topic=str(message.topic.decode("utf-8"))
	
	print(msg)
	ddict=json.loads(msg)
	print(ddict,topic)
	if ddict["Origin_IP"]==origin_ip:
		if topic=="origin/ffmpeg/archive/add":
			origin_ffmpeg_archive[0]=1
			origin_ffmpeg_archive[1]=ddict
		elif topic=="origin/ffmpeg/archive/delete":
			print(ddict)
			origin_ffmpeg_archive_del[0]=1
			origin_ffmpeg_archive_del[1]=ddict
		else:
			pass


def ffmpeg_archiver(msg,length):
	oc.OriginFfmpegArchive.delay(msg,length)
	# client.publish("db/origin/ffmpeg/archiver/spawn","Archiver started")

#MQTT Params
mqttServerParams = {}
mqttServerParams["url"] = "10.156.14.141"
mqttServerParams["port"] = 1883
mqttServerParams["timeout"] = 60
mqttServerParams["topic"] = [("origin/ffmpeg/archive/add",0),("origin/ffmpeg/archive/delete",0)]
mqttServerParams["onMessage"] = on_message
client = MQTTPubSub(mqttServerParams)



	

if __name__=="__main__":
	client.run()
	scheduler = BackgroundScheduler()
	scheduler.add_jobstore('mongodb', collection='ffmpeg_jobs')
	scheduler.start()
	print ("Started")
	while(True):
		if origin_ffmpeg_archive[0]:
			msg=origin_ffmpeg_archive[1]
			if msg["start_date"]==None and msg["end_date"]==None:
				print "Everyday"
				starthour=msg["start_time"].split(":")[0]
				startminute=msg["start_time"].split(":")[1]
				startsecond=msg["start_time"].split(":")[2]
				length=time_diff(str(msg["start_time"]),str(msg["end_time"]))
				try:
					scheduler.add_job(ffmpeg_archiver,'cron',year="*",month="*",day="*",hour=starthour,minute=startminute,second=startsecond,id=str(msg["User_IP"])+"_"+str(msg["Stream_ID"])+"_"+str(msg["job_id"]),args=[msg,length])
				except Exception as e:
					print e
					print "Repeated Job_ID, won't be added...."
			elif msg["start_date"]==None:
				print "End date given"
				length=time_diff(str(msg["start_time"]),str(msg["end_time"]))
				print length
				starthour=msg["start_time"].split(":")[0]
				startminute=msg["start_time"].split(":")[1]
				startsecond=msg["start_time"].split(":")[2]
				try:
					scheduler.add_job(ffmpeg_archiver,'cron',year="*",month="*",day="*",hour=starthour,minute=startminute,second=startsecond,end_date=msg["end_date"],id=str(msg["User_IP"])+"_"+str(msg["Stream_ID"])+"_"+str(msg["job_id"]),args=[msg,length])			
				except Exception as e:
					print e
					print "Repeated Job_ID, won't be added...."
			elif msg["end_date"]==None:
				print "Start Date given"
				length=time_diff(str(msg["start_time"]),str(msg["end_time"]))
				print length
				starthour=msg["start_time"].split(":")[0]
				startminute=msg["start_time"].split(":")[1]
				startsecond=msg["start_time"].split(":")[2]
				try:
					scheduler.add_job(ffmpeg_archiver,'cron',year="*",month="*",day="*",hour=starthour,minute=startminute,second=startsecond,start_date=msg["start_date"],id=str(msg["User_IP"])+"_"+str(msg["Stream_ID"])+"_"+str(msg["job_id"]),args=[msg,length])
				except Exception as e:
					print e
					print "Repeated Job_ID, won't be added...."
			else:
				pass	
			origin_ffmpeg_archive=[0,""]
		elif origin_ffmpeg_archive_del[0]:
			msg=origin_ffmpeg_archive_del[1]
			scheduler.remove_job(str(msg["User_IP"])+"_"+str(msg["Stream_ID"])+"_"+str(msg["job_id"]))
			print ("Job Deleted")
			origin_ffmpeg_archive_del=[0,""]
		else:
			continue
	



