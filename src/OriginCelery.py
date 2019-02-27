from celery import Celery
import os
import sys
import signal
import time
import requests
import socket
import subprocess as sp
import json
from celery.utils.log import get_task_logger


app=Celery('OriginCelery',backend="redis://",broker="redis://")
logger = get_task_logger(__name__)

FNULL = open(os.devnull, 'w')

@app.task
def OriginFfmpegSpawn(origin_ffmpeg_spawn):
	logger.info("Spawn")
	msg=origin_ffmpeg_spawn[1]
	cmd=["nohup","/usr/bin/ffmpeg", "-i", "rtsp://"+str(msg["Stream_IP"])+"/h264", "-an", "-vcodec", "copy", "-f","flv", "rtmp://"+str(msg["Origin_IP"]).strip()+":1935/dynamic/"+str(msg["Stream_ID"]).strip(),"&"]
	logger.info( " ".join(cmd))
	proc=sp.Popen(" ".join(cmd),stdout=FNULL, stderr=FNULL,stdin=FNULL,shell=True,preexec_fn=os.setpgrp)
	logger.info( "FFMPEG spawned for "+str(cmd[3])+"----------->"+str(cmd[-2]))
	senddict={"CMD":" ".join(cmd),"FROM_IP":msg["Stream_IP"],"Stream_ID":msg["Stream_ID"],"TO_IP":msg["Origin_IP"]}
	return {"topic":"db/origin/ffmpeg/stream/spawn","ddict":senddict}
	# col.insert_one(msg)


@app.task
def OriginFfmpegDist(origin_ffmpeg_dist):
	logger.info(":DistSpawn")
	msg=origin_ffmpeg_dist[1]
	cmd=["nohup","/usr/bin/ffmpeg", "-i", "rtmp://"+str(msg["Origin_IP"]).strip()+":1935/dynamic/"+str(msg["Stream_ID"]).strip(), "-an", "-vcodec", "copy", "-f","flv", "rtmp://"+str(msg["Dist_IP"]).strip()+":1935/dynamic/"+str(msg["Stream_ID"]).strip(),"&"]
	proc=sp.Popen(" ".join(cmd),stdin=FNULL,stdout=FNULL, stderr=FNULL,shell=True,preexec_fn=os.setpgrp)
	logger.info( "FFMPEG spawned for "+str(cmd[3])+"----------->"+str(cmd[-2]))
	logger.info( " ".join(cmd))
	senddict={"CMD":" ".join(cmd),"FROM_IP":msg["Origin_IP"],"Stream_ID":msg["Stream_ID"],"TO_IP":msg["Dist_IP"]}
	return {"topic":"db/origin/ffmpeg/dist/spawn","ddict":senddict}
	# col.insert_one(msg)


@app.task
def OriginFfmpegRespawn(origin_ffmpeg_respawn):
	logger.info("respawn")
	logger.info( "Entered respawner")
	logger.info( origin_ffmpeg_respawn[1])
	msg=origin_ffmpeg_respawn[1]
	cmd=["nohup","/usr/bin/ffmpeg", "-i", "rtsp://"+str(msg["Stream_IP"])+"/h264", "-an", "-vcodec", "copy", "-f","flv", "rtmp://"+str(msg["Origin_IP"]).strip()+":1935/dynamic/"+str(msg["Stream_ID"]).strip(),"&"]
	logger.info( " ".join(cmd))
	proc=sp.Popen(" ".join(cmd),stdout=FNULL, stderr=FNULL,stdin=FNULL,shell=True,preexec_fn=os.setpgrp)
	logger.info( "FFMPEG spawned for "+str(cmd[3])+"----------->"+str(cmd[-2]))
	senddict={"CMD":" ".join(cmd),"FROM_IP":msg["Stream_IP"],"Stream_ID":msg["Stream_ID"],"TO_IP":msg["Origin_IP"]}
	return {"topic":"db/origin/ffmpeg/stream/sspawn","ddict":senddict}


@app.task 
def OriginFfmpegDistRespawn(origin_ffmpeg_dist_respawn):
	logger.info("DistRespawn")
	msg=origin_ffmpeg_dist_respawn[1]
	cmd=["nohup","/usr/bin/ffmpeg", "-i", "rtmp://"+str(msg["Origin_IP"]).strip()+":1935/dynamic/"+str(msg["Stream_ID"]).strip(), "-an", "-vcodec", "copy", "-f","flv", "rtmp://"+str(msg["Dist_IP"]).strip()+":1935/dynamic/"+str(msg["Stream_ID"]).strip(),"&"]
	proc=sp.Popen(" ".join(cmd),stdin=FNULL,stdout=FNULL, stderr=FNULL,shell=True,preexec_fn=os.setpgrp)
	logger.info( "FFMPEG spawned for "+str(cmd[3])+"----------->"+str(cmd[-2]))
	logger.info( " ".join(cmd))
	senddict={"CMD":" ".join(cmd),"FROM_IP":msg["Origin_IP"],"Stream_ID":msg["Stream_ID"],"TO_IP":msg["Dist_IP"]}
	return {"topic":"db/origin/ffmpeg/dist/spawn","ddcit":(senddict)}
	# col.insert_one(msg)

@app.task
def OriginFfmpegKill(origin_ffmpeg_kill):
	for i in origin_ffmpeg_kill[1]:
		logger.info( i["CMD"].split()[1:-1])
		sp.Popen(["pkill","-f"," ".join(i["CMD"].split()[1:-1])],stdin=FNULL,stdout=FNULL,stderr=FNULL,shell=False)
		time.sleep(1)
	return 0



@app.task
def OriginFfmpegKillAll(origin_ffmpeg_killall):
	
	sp.Popen(["pkill","-f","/usr/bin/ffmpeg"],stdin=FNULL,stdout=FNULL,stderr=FNULL,shell=False)
	return 0

