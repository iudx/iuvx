import loadbalancercelery as lbc
import pymongo
import os
import sys
import signal
import time
import pymongo
import datetime
from MQTTPubSub import MQTTPubSub
import json
import threading
from enum import Enum


''' Global Variables '''
sorigin = ""
sdist = ""
norigin = {}
ndist = {}

results = []

# All Flags
insert_origin = [0, ""]
delete_origin = [0, ""]
origin_ffmpeg_stream = [0, ""]
origin_ffmpeg_dist = [0, ""]
insert_dist = [0, ""]
delete_dist = [0, ""]
insert_stream = [0, ""]
delete_stream = [0, ""]
reqstream = [0, ""]
origin_ffmpeg_respawn = [0, ""]
archive_stream_add = [0, ""]
archive_stream_del = [0, ""]
req_all_streams = [0, ""]
origin_ffmpeg_dist_respawn = [0, ""]
user_add = [0, ""]
user_del = [0, ""]
verify_user = [0, ""]
get_origins = [0, ""]
get_dists = [0, ""]
get_users = [0, ""]
get_streams = [0, ""]
get_archives = [0, ""]




class LB():
    def __init__(self):
        self.action = "idle"
        self.msg = ""
        pass

    def on_message(self, client, userdata, message):
        self.msg = str(message.payload.decode("utf-8"))
        self.action = str(message.topic.decode("utf-8"))
        elif topic == "db/origin/ffmpeg/stream/spawn":
            origin_ffmpeg_stream[0] = 1
            origin_ffmpeg_stream[1] = msg
        elif topic == "origin/delete":
            delete_origin[0] = 1
            delete_origin[1] = msg
        elif topic == "dist/add":
            insert_dist[0] = 1
            insert_dist[1] = msg
        elif topic == "dist/delete":
            delete_dist[0] = 1
            delete_dist[1] = msg
        elif topic == "stream/add":
            insert_stream[0] = 1
            insert_stream[1] = msg
        elif topic == "stream/delete":
            delete_stream[0] = 1
            delete_stream[1] = msg
        elif topic == "stream/request":
            reqstream[0] = 1
            reqstream[1] = msg
        elif topic == "db/origin/ffmpeg/dist/spawn":
            origin_ffmpeg_dist[0] = 1
            origin_ffmpeg_dist[1] = msg
            # originstreams[ip]=streams
            # print topic+" "+msg
        elif topic == "db/origin/ffmpeg/respawn":
            origin_ffmpeg_respawn[0] = 1
            origin_ffmpeg_respawn[1] = msg
        elif topic == "db/dist/ffmpeg/respawn":
            origin_ffmpeg_dist_respawn[0] = 1
            origin_ffmpeg_dist_respawn[1] = msg
        elif topic == "archive/add":
            archive_stream_add[0] = 1
            archive_stream_add[1] = msg
        elif topic == "archive/delete":
            archive_stream_del[0] = 1
            archive_stream_del[1] = msg
        elif topic == "origin/stat":
            lbc.OriginStat.delay(msg)
        elif topic == "dist/stat":
            lbc.DistStat.delay(msg)
        elif topic == "user/add":
            user_add[0] = 1
            user_add[1] = msg
        elif topic == "user/del":
            user_del[0] = 1
            user_del[1] = msg
        elif topic == "verify/user":
            verify_user[0] = 1
            verify_user[1] = msg
        elif topic == "origin/get":
            get_origins[0] = 1
        elif topic == "dist/get":
            get_dists[0] = 1
        elif topic == "stream/get":
            get_streams[0] = 1
        elif topic == "user/get":
            get_users[0] = 1
        elif topic == "archive/get":
            get_archives[0] = 1

        # print topic+" "+msg
        # diststreams[ip]=streams


# MQTT Params
mqttServerParams = {}
mqttServerParams["url"] = "10.156.14.138"
mqttServerParams["port"] = 1883
mqttServerParams["timeout"] = 60
mqttServerParams["topic"] = [("origin/get", 0), ("dist/get", 0), ("archive/get", 0), ("stream/get", 0), ("user/get", 0), ("verify/user", 0), ("user/add", 0), ("user/del", 0), ("db/dist/ffmpeg/respawn", 0), ("archive/delete", 0), ("request/allstreams", 0), ("archive/add", 0),
                             ("db/origin/ffmpeg/respawn", 0), ("origin/stat", 0), ("dist/stat", 0), ("origin/add", 0), ("origin/delete", 0), ("dist/add", 0), ("dist/delete", 0), ("stream/add", 0), ("stream/delete", 0), ("stream/request", 0), ("db/origin/ffmpeg/dist/spawn", 0), ("db/origin/ffmpeg/stream/spawn", 0)]
mqttServerParams["onMessage"] = on_message
client = MQTTPubSub(mqttServerParams)

norigin = {}
ndist = {}


def monitorTaskResult(res):
    global client
    while(True):
        if res.ready():
            retval = res.get()
            if retval:
                if isinstance(retval, dict):
                    client.publish(retval["topic"],
                                   json.dumps(retval["msg"]))
                    time.sleep(0.1)
                elif isinstance(retval, list):
                    for i in retval:
                        if i["topic"] == "origin/ffmpeg/stream/stat/spawn" or i["topic"] == "dist/ffmpeg/stream/stat/spawn":
                            time.sleep(30)
                        client.publish(i["topic"], json.dumps(i["ddict"]))
                        time.sleep(0.1)
            break


if __name__ == "__main__":
    client.run()
    while(True):
        msg = {}
        # Always setting Load Balancer Params
        if self.action == "request/allstreams":
            res = lbc.ReqAllStreams.delay(self.msg)
            threading.Thread(target=monitorTaskResult, args=(res,)).start()
            self.action = "idle"
            self.msg = ""
        elif self.action == "origin/add":
            res = lbc.InsertOrigin.delay(self.msg)
            threading.Thread(target=monitorTaskResult, args=(res,)).start()
            self.action = "idle"
            self.msg = ""
        elif delete_origin[0]:
            res = lbc.DeleteOrigin.delay(delete_origin)
            threading.Thread(target=monitorTaskResult, args=(res,)).start()
            delete_origin = [0, ""]
        elif origin_ffmpeg_stream[0]:
            res = lbc.OriginFfmpegStream.delay(origin_ffmpeg_stream)
            threading.Thread(target=monitorTaskResult, args=(res,)).start()
            origin_ffmpeg_stream = [0, ""]
        elif insert_dist[0]:
            res = lbc.InsertDist.delay(insert_dist)
            threading.Thread(target=monitorTaskResult, args=(res,)).start()
            insert_dist = [0, ""]
        elif delete_dist[0]:
            res = lbc.DeleteDist.delay(delete_dist)
            threading.Thread(target=monitorTaskResult, args=(res,)).start()
            delete_dist = [0, ""]
        elif insert_stream[0]:
            res = lbc.InsertStream.delay(insert_stream)
            threading.Thread(target=monitorTaskResult, args=(res,)).start()
            insert_stream = [0, ""]
        elif delete_stream[0]:
            res = lbc.DeleteStream.delay(delete_stream)
            threading.Thread(target=monitorTaskResult, args=(res,)).start()
            delete_stream = [0, ""]
        elif reqstream[0]:
            res = lbc.RequestStream.delay(reqstream)
            threading.Thread(target=monitorTaskResult, args=(res,)).start()
            reqstream = [0, ""]
        elif origin_ffmpeg_dist[0]:
            res = lbc.OriginFfmpegDist.delay(origin_ffmpeg_dist)
            threading.Thread(target=monitorTaskResult, args=(res,)).start()
            origin_ffmpeg_dist = [0, ""]
        elif origin_ffmpeg_respawn[0]:
            res = lbc.OriginFfmpegRespawn.delay(origin_ffmpeg_respawn)
            threading.Thread(target=monitorTaskResult, args=(res,)).start()
            origin_ffmpeg_respawn = [0, ""]
        elif archive_stream_add[0]:
            res = lbc.ArchiveAdd.delay(archive_stream_add)
            threading.Thread(target=monitorTaskResult, args=(res,)).start()
            archive_stream_add = [0, ""]
        elif archive_stream_del[0]:
            res = lbc.ArchiveDel.delay(archive_stream_del)
            threading.Thread(target=monitorTaskResult, args=(res,)).start()
            archive_stream_del = [0, ""]
        elif origin_ffmpeg_dist_respawn[0]:
            res = lbc.OriginFFmpegDistRespawn.delay(origin_ffmpeg_dist_respawn)
            threading.Thread(target=monitorTaskResult, args=(res,)).start()
            origin_ffmpeg_dist_respawn = [0, ""]
        elif user_add[0]:
            print user_add[1]
            res = lbc.AddUser.delay(user_add)
            threading.Thread(target=monitorTaskResult, args=(res,)).start()
            user_add = [0, ""]
        elif user_del[0]:
            res = lbc.DelUser.delay(user_del)
            threading.Thread(target=monitorTaskResult, args=(res,)).start()
            user_del = [0, ""]
        elif verify_user[0]:
            res = lbc.VerifyUser.delay(verify_user)
            threading.Thread(target=monitorTaskResult, args=(res,)).start()
            verify_user = [0, ""]
        elif get_users[0]:
            res = lbc.GetUsers.delay()
            threading.Thread(target=monitorTaskResult, args=(res,)).start()
            get_users = [0, ""]
        elif get_archives[0]:
            res = lbc.GetArchives.delay()
            threading.Thread(target=monitorTaskResult, args=(res,)).start()
            get_archives = [0, ""]
        elif get_dists[0]:
            res = lbc.GetDists.delay()
            threading.Thread(target=monitorTaskResult, args=(res,)).start()
            get_dists = [0, ""]
        elif get_origins[0]:
            res = lbc.GetOrigins.delay()
            threading.Thread(target=monitorTaskResult, args=(res,)).start()
            get_origins = [0, ""]
        elif get_streams[0]:
            res = lbc.GetStreams.delay()
            threading.Thread(target=monitorTaskResult, args=(res,)).start()
            get_streams = [0, ""]
        else:
            continue
    #
