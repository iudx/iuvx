import sys
from celery import Celery
import os
import time
import subprocess as sp
import json
from celery.utils.log import get_task_logger
sys.path.insert(1, './src')

'''
    originffmpegspawner.py
    Contains all the originServer functions
    TODO:
'''

''' Celery app '''
''' TODO: Parameterize '''
app = Celery('originffmpegspawner', backend="redis://localhost/1",
             broker="redis://localhost/1")

''' Logger '''
logger = get_task_logger(__name__)

''' Stderr file handler '''
FNULL = open(os.devnull, 'w')


@app.task
def OriginFfmpegSpawn(msg):
    '''
        Input: {origin_ip: string, origin_id: string,
                stream_id: string, stream_ip: string}
        Trigger: originffmpegspawner.py
        Handles: Spawns a media link between camera
                 and origin NGINX server.
                 "stream_ip" represents the
                 IP address for stream playback from the IP camera.
    '''
    logger.info("Spawning ", msg["stream_id"])
    msg = json.loads(msg)
    ''' spawn rtsp push '''
    rtsp_cmd = ["nohup", "/usr/bin/ffmpeg", "-i", msg["stream_ip"],
                "-an", "-vcodec", "copy", "-f", "rtsp", "-rtsp_transport",
                "tcp", "rtsp://" + msg["origin_ip"].strip() +
                ":80/dynamic/" + msg["stream_id"].strip(), "&"]
    ''' FFMPEG RTMP push to origin server '''
    '''
        TODO: Get PID and send to
        loadbalancercelery.py:UpdateOriginStream()
    '''
    cmd = ["nohup", "/usr/bin/ffmpeg", "-i", msg["stream_ip"],
           "-an", "-vcodec", "copy", "-f", "flv", "rtmp://" +
           str(msg["origin_ip"]).strip() + ":1935/dynamic/" +
           str(msg["stream_id"]).strip(), "&"]
    logger.info("Executing command ", "".join(cmd))
    sp.Popen(" ".join(cmd), stdout=FNULL, stderr=FNULL,
             stdin=FNULL, shell=True, preexec_fn=os.setpgrp)
    sp.Popen(" ".join(rtsp_cmd), stdin=FNULL, stdout=FNULL,
             stderr=FNULL, shell=True, preexec_fn=os.setpgrp)

    out = {"cmd": " ".join(cmd), "from_ip": msg["stream_ip"],
           "stream_id": msg["stream_id"], "to_ip": msg["origin_ip"],
           "rtsp_cmd": " ".join(rtsp_cmd)}
    return {"topic": "db/origin/ffmpeg/stream/spawn", "msg": out}


@app.task
def OriginFfmpegDistSpawn(msg):
    '''
        Input: {origin_ip: string, origin_id: string,
                dist_ip: string, dist_id: string,
                stream_id: string, stream_ip: string}
        Trigger: originffmpegspawner.py
        Handles: Spawns a media link between origin
                 and distribution NGINX server.
        TODO: Remove RTSP push
    '''
    logger.info("Spawning FFMPEG push to distribution server ",)
    msg = json.loads(msg)

    rtsp_cmd = ["nohup", "/usr/bin/ffmpeg", "-i",
                "rtmp://" + str(msg["origin_ip"]).strip() +
                ":1935/dynamic/" + str(msg["stream_id"]).strip(),
                "-an", "-vcodec", "copy", "-f", "rtsp", "-rtsp_transport",
                "tcp", "rtsp://" + str(msg["dist_ip"]).strip() +
                ":80/dynamic/" + str(msg["stream_id"]).strip(), "&"]

    cmd = ["nohup", "/usr/bin/ffmpeg", "-i", "rtmp://" +
           str(msg["origin_ip"]).strip() +
           ":1935/dynamic/" + str(msg["stream_id"]).strip(),
           "-an", "-vcodec", "copy", "-f", "flv",
           "rtmp://" + str(msg["dist_ip"]).strip() +
           ":1935/dynamic/" + str(msg["stream_id"]).strip(), "&"]

    sp.Popen(" ".join(cmd), stdin=FNULL, stdout=FNULL,
             stderr=FNULL, shell=True, preexec_fn=os.setpgrp)
    sp.Popen(" ".join(rtsp_cmd), stdin=FNULL, stdout=FNULL,
             stderr=FNULL, shell=True, preexec_fn=os.setpgrp)
    logger.info("Execution command ".join(cmd))
    out = {"cmd": " ".join(cmd), "from_ip": msg["origin_ip"],
           "stream_id": msg["stream_id"], "to_ip": msg["dist_ip"],
           "rtsp_cmd": " ".join(rtsp_cmd)}
    return {"topic": "db/origin/ffmpeg/dist/spawn", "msg": out}


@app.task
def OriginFfmpegDistRespawn(msg):
    '''
        Input: {origin_ip: string, origin_id: string,
                dist_ip: string, dist_id: string,
                stream_id: string, stream_ip: string}
        Trigger: originffmpegspawner.py
        Handles: ReSpawns a media link between origin
                 and distribution NGINX server.
        TODO: Use OriginFfmpegDistSpawn in it's lieu
    '''
    msg = OriginFfmpegDistSpawn(msg)["msg"]
    return {"topic": "db/origin/ffmpeg/stream/sspawn", "msg": msg}


@app.task
def OriginFfmpegKill(msg):
    '''
        Input: [{cmd: string, rtsp_cmd: string}]
        Trigger: originffmpegspawner.py
        Handles: Kills streams
    '''
    for stream in msg:
        logger.info(stream["cmd"].split()[1:-1])
        sp.Popen(["pkill", "-f", " ".join(stream["cmd"].split()[1:-1])],
                 stdin=FNULL, stdout=FNULL, stderr=FNULL, shell=False)
        time.sleep(1)
        sp.Popen(["pkill", "-f", " ".join(stream["rtsp_cmd"].split()[1:-1])],
                 stdin=FNULL, stdout=FNULL, stderr=FNULL, shell=False)
        time.sleep(1)
    return 0


@app.task
def OriginFfmpegKillAll():
    '''
        Input: [{cmd: string, rtsp_cmd: string}]
        Trigger: originffmpegspawner.py
        Handles: Kills all streams
    '''
    sp.Popen(["pkill", "-f", "/usr/bin/ffmpeg"], stdin=FNULL,
             stdout=FNULL, stderr=FNULL, shell=False)
    return 0


@app.task
def OriginFfmpegArchive(msg, length):
    '''
        Input: [{cmd: string, rtsp_cmd: string}]
        Trigger: originffmpegarchiver.py
        Handles: Archive db info
    '''
    logger.info("Archiving ", msg["stream_id"])
    cmd = ["nohup", "/usr/bin/ffmpeg", "-i",
           "rtmp://" + str(msg["origin_ip"]).strip() +
           ":1935/ dynamic/" + str(msg["stream_id"]).strip(),
           "-an", "-vcodec", "copy", "-t", str(length), "-f",
           "flv",  "JobID_"+str(msg["job_id"]) + "_stream_id_" +
           str(msg["stream_id"]) + "_start_date_" +
           str(msg["start_date"]) + "_start_time_" +
           str(msg["start_time"]) + "_end_date_" +
           str(msg["end_date"]) + "_end_time_" +
           str(msg["end_time"]) + "_length_" +
           str(length)+".flv", "&"]
    logger.info(" ".join(cmd))
    sp.Popen(" ".join(cmd), stdout=FNULL, stderr=FNULL,
             stdin=FNULL, shell=True, preexec_fn=os.setpgrp)
