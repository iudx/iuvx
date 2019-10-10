# Needs to update the database with all the details of everythign that's going on. Plus handle all the other important requests.


from __future__ import absolute_import
from celery import Celery
# celery
from celery.utils.log import get_task_logger


import json
from MQTTPubSub import MQTTPubSub
import pymongo
import time
import datetime
import os
import sys
# LBS Params


'''
    TODO:
        1. Validation at mongodb
'''


''' Celery app '''
app = Celery('loadbalancercelery', backend="redis://", broker="redis://")

'''Logger '''
logger = get_task_logger(__name__)
# originstreams={}
# diststreams={}


''' mongo initializations '''
mongoclient = pymongo.MongoClient('mongodb://localhost:27017/')
mongoDB = mongoclient["ALL_Streams"]
col2 = mongoDB["Distribution_Servers"]
col3 = mongoDB["Streams"]
col4 = mongoDB["Ffmpeg_Procs"]
col5 = mongoDB["Users"]
col6 = mongoDB["Archives"]

# Update Origin, Dists and Streams


class Table():
    '''
        Perform operations on mongo table
        Args:
            mc: Mongo DB
    '''

    def __init__(self, mongoDB, name):
        self.collection = mongoDB[name]

    def insertOne(self, doc):
        res = self.collection.insert_one(doc)
        if(res.inserted_id is not None):
            return 1
        else:
            return 0

    def update(self, key, doc):
        res = self.collection.update_one(key, {"$set": doc}, upsert=True)
        return res.modified_count()

    def findOne(self, doc, args=None):
        res = self.collection.find_one(doc, {"_id": 0})
        return res

    def findAll(self, doc=None):
        if (doc is not None):
            res = self.collection.find(doc, {"_id": 0})
            return res
        else:
            return self.collection.find({}, {"_id": 0})

    def delete(self, doc):
        self.collection.delete_one(doc)

    def deleteMany(self, doc):
        self.collection.delete_many(doc)

    def count(self):
        return self.collection.count()


'''
    {origin_id: string, origin_ip: string[uri], num_clients: int}
'''
originTable = Table(mongoDB, "originTable")

'''
    {cmd: string, from_ip: string, stream_ip: string,
     to_ip: string, rtsp_cmd: string}
    col4
'''
ffmpegProcsTable = Table(mongoDB, "ffmpegProcsTable")

'''
    {stream_id: string, stream_ip: string[uri], 
     origin_ip: string[uri], dist_ip: string[uri]
     status: enum[onboadrding, deleting, active, down]
     }
    col3
'''
streamsTable = Table(mongoDB, "streams")

'''
    {dist_id: string, dist_ip: string[uri], num_clients: int}
'''
distTable = Table(mongoDB, "distTable")


def choose_origin():
    ''' Algorithm to choose origin server on which to onboard a stream '''
    ''' TODO: Proper origin load balancing algorithm '''

    #Currently, choose the first origin server available in the collection
    o_id = originTable.findAll()[0]["origin_id"]
    o_ip = originTable.findOne({"origin_id":o_id})["origin_ip"]
    return o_id, o_ip


def search_archives():
    global col6
    archivejobid = []
    archivestream = []
    if col6.count() != 0:
        for i in col6.find():
            archivejobid.append(i["Job_ID"])
            archivestream.append(i["stream_id"])
        return archivestream, archivejobid
    else:
        return [], []



def search_dist():
    global col2
    dist_ips = []
    dist_ids = []
    if col2.count() != 0:
        for i in col2.find():
            dist_ips.append(i["dist_ip"])
            dist_ids.append(i["dist_id"])
        return dist_ips, dist_ids
    else:
        return [], []


def search_stream():
    global col3
    stream_ips = []
    stream_ids = []
    if col3.count() != 0:
        for i in col3.find():
            stream_ips.append(i["stream_ip"])
            stream_ids.append(i["stream_id"])
        return stream_ips, stream_ids
    else:
        return [], []


def search_users():
    global col5
    usernames = []
    if col5.count() != 0:
        for i in col5.find():
            usernames.append(i["User"])
        return usernames
    else:
        return []


@app.task
def GetUsers():
    usernames = search_users()
    return {"topic": "lbsresponse/user/all", "msg": usernames}


@app.task
def GetOrigins():
    '''
        Trigger: celeryLBmain.py
        Handles: Show all origin servers
        Response: HTTPServer.py
    '''
    res = json.dumps(originTable.findAll())
    return {"topic": "lbsresponse/origin/all", "msg": res}


@app.task
def InsertOrigin(msg):
    '''
        Trigger: celeryLBmain.py
        Handles: Origin insertion requests
        Response: HTTPServer.py
    '''
    logger.info("Inserting Origin")
    msg = json.loads(msg)
    ret = originTable.insertOne(msg)
    if ret == 1:
        logger.info("Added origin ", msg["origin_id"])
        return {"topic": "lbsresponse/origin/add", "msg": True}
    else:
        logger.info("Origin already present", msg["origin_ip"])
        return {"topic": "lbsresponse/origin/add", "msg": False}


@app.task
def OriginStat(msg):
    '''
        Trigger: celeryLBmain.py
        Handles: Update num_clients
        Response: None
    '''
    msg = json.loads(msg)
    originTable.update({"origin_id": msg["origin_id"]},
                       {"num_clients": msg["num_clients"]})


@app.task
def DeleteOrigin(msg):
    '''
        Trigger: celeryLBmain.py
        Handles: Origin deletion requests
        Response: HTTPServer.py
        TODO: Kill origin streams
    '''
    logger.info("Deleting Origin")
    ret = originTable.delete({"origin_id": msg["origin_id"]})
    if ret == 1:
        logger.info("Deleted origin ", msg["origin_id"])
        ffmpegProcsTable.deleteMany({"to_id": msg["origin_id"]})
        ffmpegProcsTable.deleteMany({"origin_id": msg["origin_id"]})
        streamsTable.deleteMany({"from_id": msg["origin_id"]})
        logger.info("Origin Deleted----> ID:" + " ID:"+str(msg["origin_id"]))
        return [{"topic": "lbsresponse/origin/del", "msg": True},
                {"topic": "origin/ffmpeg/killall", "msg": msg}]
    else:
        return {"topic": "lbsresponse/origin/del", "msg": False}


@app.task
def UpdateOriginStream(msg):
    '''
        Trigger: OriginCelery.py
        Handles: adding ffmpeg stream to db once
                 it's added at the origin server
    '''
    msg = json.loads(msg)
    logger.info(str(msg["stream_id"]) +
                " stream has been started to origin " + str(msg["to_ip"]))
    ffmpegProcsTable.insertOne(msg)
    streamsTable.update({"stream_id": msg["stream_id"]},
                        {"$set": {"origin_ip": msg["to_ip"]}})
    time.sleep(0.1)
    return 0


@app.task
def InsertDist(msg):
    '''
        Trigger: celeryLBmain.py
        Handles: Dist insertion requests
        Response: HTTPServer.py
    '''
    logger.info("Inserting Dist")
    dist_ips, dist_ids = search_dist()
    msg = json.loads(msg)
    ret = distTable.insertOne(msg)
    if ret == 1:
        logger.info("Added dist", msg["dist_id"])
        return {"topic": "lbsresponse/dist/add", "msg": True}
    else:
        logger.info("Dist already present", msg["dist_ip"])


@app.task
def DeleteDist(msg):
    '''
        Trigger: celeryLBmain.py
        Handles: Dist deletion requests
        Response: HTTPServer.py
    '''
    logger.info("Deleting Dist")
    msg = json.loads(msg)
    ret = distTable.delete({"dist_id": msg["dist_id"]})
    killlist = []
    if ret == 1:
        logger.info("Deleted dist ", msg["dist_id"])
        killlist = ffmpegProcsTable.findAll({"dist_id": msg["dist_id"]})
        ffmpegProcsTable.deleteMany({"to_id": msg["dist_id"]})
        ffmpegProcsTable.deleteMany({"dist_id": msg["dist_id"]})
        streamsTable.deleteMany({"from_id": msg["dist_id"]})
        logger.info("Origin Deleted----> ID:" + " ID:"+str(msg["dist_id"]))
        return [{"topic": "lbsresponse/dist/del", "msg": True},
                {"topic": "origin/ffmpeg/kill", "msg": killlist}]
    else:
        return {"topic": "lbsresponse/dist/del", "msg": False}


@app.task
def GetDists():
    '''
        Trigger: celeryLBmain.py
        Handles: Show all dist servers
        Response: HTTPServer.py
    '''
    res = json.dumps(distTable.findAll())
    return {"topic": "lbsresponse/dist/all", "msg": res}


@app.task
def DistStat(msg):
    '''
        Trigger: celeryLBmain.py
        Handles: Update num_clients
        Response: None
    '''
    msg = json.loads(msg)
    distTable.update({"dist_id": msg["dist_id"]},
                     {"num_clients": msg["num_clients"]})


@app.task()
def OriginFfmpegDist(msg):
    '''
        Trigger: celeryLBmain.py
        Handles: Update num_clients
        Response: None
    '''
    msg = json.loads(msg)
    logger.info(msg)
    logger.info(msg["stream_id"]+" stream push has been started from origin " +
                msg["from_ip"]+" to distribution "+msg["to_ip"])

    ffmpegProcsTable.insertOne({"cmd": msg["cmd"], "to_ip": msg["to_ip"],
                                "from_ip": msg["from_ip"],
                                "stream_id": msg["stream_id"],
                                "rtsp_cmd": msg["rtsp_cmd"]})

    streamsTable.update({"stream_id": msg["stream_id"],
                         "origin_ip": msg["from_ip"]},
                        {"$set": {"dist_ip": msg["to_ip"]}})
    logger.info(str(msg["cmd"].split()[-2]))
    time.sleep(0.1)
    return {"topic": "lbsresponse/rtmp", "msg": str(msg["cmd"].split()[-2])}


@app.task
def GetStreams():
    msg = {}
    stream_ips, stream_ids = search_stream()
    for i in range(len(stream_ips)):
        msg[stream_ips[i]] = stream_ids[i]
    return {"topic": "lbsresponse/stream/all", "msg": msg}


@app.task
def GetArchives():
    msg = {}
    streamarch, jobarch = search_archives()
    for i in range(len(streamarch)):
        msg[streamarch[i]] = jobarch[i]
    return {"topic": "lbsresponse/archive/all", "msg": msg}


@app.task
def ReqAllStreams(msg):
    ''' Handles stream requests '''
    msg = json.loads(msg)
    ip = msg["origin_ip"]
    resp = []
    ''' TODO !! '''
    for i in originTable.findAll({"origin_ip": msg["origin_ip"]}):
        msg = {"origin_ip": i["origin_ip"], "Stream_List": []}
        for j in col3.find():
            if j["origin_ip"] == i["origin_ip"]:
                msg["Stream_List"].append(
                    {"stream_id": j["stream_id"], "stream_ip": j["stream_ip"]})
        logger.info(msg)
    for j in col2.find():
        msg = {"dist_ip": ip, "Stream_List": []}
        for i in col3.find():
            if i["dist_ip"] == j["dist_ip"]:
                msg["Stream_List"].append(
                    {"stream_id": i["stream_id"], "stream_ip": i["stream_ip"]})
        logger.info(msg)
    return {"topic": "lb/request/allstreams", "msg": msg}






@app.task
def InsertStream(insert_stream):
    if originTable.count() == 0:
        logger.info("No Origin Server Present")
        return 0
    else:
        ''' TODO: Logic of load balancing sits here '''
        o_id, o_ip = choose_origin()

        ''' TODO: Replace '''
        stream_ips, stream_ids = search_stream()
        msg = json.loads(insert_stream[1])
        new_stream_ip = msg["Stream_IP"]
        new_stream_id = msg["Stream_ID"]
        if new_stream_ip not in stream_ips:
            if new_stream_id not in stream_ids:
               streamsTable.insertOne( {"Stream_IP": new_stream_ip, "Stream_ID": new_stream_id, 
                                       "Origin_IP": o_ip, "Dist_IP": ""})
               logger.info(str(o_id)+" " +
                           str(new_stream_id)+" "+str(new_stream_ip))
               ddict = {"Origin_IP": o_ip,
                        "Stream_ID": new_stream_id, "Stream_IP": new_stream_ip}
               logger.info("Sending Dict")
               logger.info("Stream Added----> ID:" +
                           str(msg["Stream_ID"])+" IP:"+str(msg["Stream_IP"]))
               return [{"topic": "lbsresponse/stream/add", "ddict": True}, {"topic": "origin/ffmpeg/stream/spawn", "ddict": ddict}, {"topic": "origin/ffmpeg/stream/stat/spawn", "ddict": ddict}]
         
            else: 
              logger.info("Stream ID already present (choose another ID) ---> ID:" +
                          str(msg["Stream_ID"])+" IP:"+str(msg["Stream_IP"]))
              return {"topic": "lbsresponse/stream/add", "ddict": False}

        else:
            logger.info("Stream IP already present ---> ID:" +
                        str(msg["Stream_ID"])+" IP:"+str(msg["Stream_IP"]))
            return {"topic": "lbsresponse/stream/add", "ddict": False}

        ## client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(msg))
        #if msg["Stream_ID"] not in stream_ids:
        #    col3.insert_one(
        #        {"Stream_IP": msg["Stream_IP"], "Stream_ID": msg["Stream_ID"], "Origin_IP": "", "Dist_IP": ""})
        #    logger.info(str(sorigin)+" " +
        #                str(msg["Stream_ID"])+" "+str(msg["Stream_IP"]))
        #    ddict = {"Origin_IP": sorigin,
        #             "Stream_ID": msg["Stream_ID"], "Stream_IP": msg["Stream_IP"]}
        #    logger.info("Sending Dict")
        #    logger.info("Stream Added----> ID:" +
        #                str(msg["Stream_ID"])+" IP:"+str(msg["Stream_IP"]))
        #    return [{"topic": "lbsresponse/stream/add", "ddict": True}, {"topic": "origin/ffmpeg/stream/spawn", "ddict": ddict}, {"topic": "origin/ffmpeg/stream/stat/spawn", "ddict": ddict}]
        #else:
        #    logger.info("Stream already present----> ID:" +
        #                str(msg["Stream_ID"])+" IP:"+str(msg["Stream_IP"]))
        #    return {"topic": "lbsresponse/stream/add", "ddict": False}


@app.task
def DeleteStream(delete_stream):
    killist = []
    stream_ips, stream_ids = search_stream()
    msg = json.loads(delete_stream[1])
    logger.info(msg)
    #client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(msg))
    if msg["stream_id"] not in stream_ids and msg["stream_ip"] not in stream_ips:
        logger.info("Stream not present----> ID:" +
                    str(msg["stream_id"])+" IP:"+str(msg["stream_ip"]))
        return {"topic": "lbsresponse/stream/del", "msg": False}
    else:
        norigin = []
        for j in originTable.findAll():
            norigin.append(j["origin_ip"])
        for i in col4.find():
            if str(i["stream_id"]) == msg["stream_id"]:
                if i["to_ip"] in norigin:
                    killist.append({"origin_ip": i["to_ip"], "cmd": i["cmd"],
                                    "stream_id": i["stream_id"], "dist_ip": "", "rtsp_cmd": i["rtsp_cmd"]})
                elif i["from_ip"] in norigin:
                    killist.append({"origin_ip": i["to_ip"], "cmd": i["cmd"],
                                    "stream_id": i["stream_id"], "dist_ip": i["to_ip"], "rtsp_cmd": i["rtsp_cmd"]})
        col3.delete_one(
            {"stream_ip": msg["stream_ip"], "stream_id": msg["stream_id"]})
        col4.delete_many({"stream_id": msg["stream_id"]})
        logger.info("Stream Deleted----> ID:" +
                    str(msg["stream_id"])+" IP:"+str(msg["stream_ip"]))
        return [{"topic": "lbsresponse/stream/del", "msg": True}, {"topic": "origin/ffmpeg/kill", "msg": killist}]


@app.task
def RequestStream(reqstream):
    logger.info("Here")
    msg = json.loads(reqstream[1])
    stream_id = msg["stream_id"]
    logger.info(stream_id)
    alreadypushedflag = 0
   # client.publish("logger","Date/Time: "+str(datetime.datetime.now())+" Message: "+str(msg))
    if col3.count() != 0:
        stream_ips, stream_ids = search_stream()
        if stream_id not in stream_ids:
            return[{"topic": "lbsresponse/rtmp", "msg": False}]
        if col2.count() != 0:
            ndist = {}
            for j in col2.find():
                ndist[j["dist_ip"]] = j["NClients"]
            for i in col4.find():
                if (str(i["stream_id"]) == stream_id) and (str(i["to_ip"]) in ndist.keys()):
                    logger.info("Should come here if already pushed to a dist")
                    return {"topic": "lbsresponse/rtmp", "msg": "RTMP: "+str(i["cmd"].split()[-2])+" RTSP: "+str(i["rtsp_cmd"].split()[-2])+" HLS: http://"+str(i["to_ip"]+":8080/hls/"+str(i["stream_id"]+".m3u8"))}
                    alreadypushedflag = 0
                    break
                else:
                    alreadypushedflag = 1
            if alreadypushedflag == 1:
                sdist = col2.find_one(sort=[("NClients", 1)])["dist_ip"]
                ''' TODO: Logic of load balancing lies here '''
                sorigin = originTable.findAll()[0]["origin_ip"]
                logger.info(str(sorigin)+" "+str(sdist)+" "+str(stream_id))
                stream_ip = col3.find_one({"stream_id": stream_id})[
                    "stream_ip"]
                distdict = {"origin_ip": sorigin, "dist_ip": sdist,
                            "stream_id": stream_id, "stream_ip": stream_ip}
                return [{"topic": "lbsresponse/rtmp", "msg": "RTMP: "+"rtmp://"+str(sdist)+":1935/dynamic/"+str(stream_id)+" RTSP: rtsp://"+str(sdist)+":80/dynamic/"+str(stream_id)+" HLS: http://"+str(i["to_ip"]+":8080/hls/"+str(i["stream_id"]+".m3u8"))}, {"topic": "origin/ffmpeg/dist/spawn", "msg": distdict}, {"topic": "dist/ffmpeg/stream/stat/spawn", "msg": distdict}]
        else:
            for i in col4.find():
                if i["stream_id"] == stream_id:
                    return {"topic": "lbsresponse/rtmp", "msg": "RTMP: "+str(i["cmd"].split()[-2])+" RTSP: "+str(i["rtsp_cmd"].split()[-2])+" HLS: http://"+str(i["to_ip"]+":8080/hls/"+str(i["stream_id"]+".m3u8"))}
    else:
        return[{"topic": "lbsresponse/rtmp", "msg": False}]




@app.task
def OriginFfmpegRespawn(origin_ffmpeg_respawn):
    msg = json.loads(origin_ffmpeg_respawn[1])
    logger.info(str(msg)+" should come here only when missing becomes active")
    return {"topic": "origin/ffmpeg/respawn", "msg": msg}


@app.task
def ArchiveAdd(archive_stream_add):
    msg = json.loads(archive_stream_add[1])
    logger.info(msg)
    msg["stream_ip"] = col3.find_one(
        {"stream_id": msg["stream_id"]})["stream_ip"]
    msg["origin_ip"] = col3.find_one(
        {"stream_id": msg["stream_id"]})["origin_ip"]

    archivedstreams, archivedjobs = search_archives()
    if msg["job_id"] not in archivedjobs:
        col6.insert_one(
            {"Job_ID": msg["job_id"], "stream_id": msg["stream_id"]})
        return [{"topic": "lbsresponse/archive/add", "msg": True}, {"topic": "origin/ffmpeg/archive/add", "msg": msg}]
        logger.info(str(msg)+" archiving this......")
    else:
        return {"topic": "lbsresponse/archive/add", "msg": False}


@app.task
def ArchiveDel(archive_stream_del):
    msg = json.loads(archive_stream_del[1])
    logger.info(msg)
    msg["stream_ip"] = col3.find_one(
        {"stream_id": msg["stream_id"]})["stream_ip"]
    msg["origin_ip"] = col3.find_one(
        {"stream_id": msg["stream_id"]})["origin_ip"]
    archivedstreams, archivedjobs = search_archives()
    if msg["job_id"] not in archivedjobs:
        return {"topic": "lbsresponse/archive/del", "msg": False}
    else:
        logger.info(str(msg)+"  deleting this archive......")
        col6.delete_one({"Job_ID": msg["job_id"]})
        return [{"topic": "lbsresponse/archive/del", "msg": True}, {"topic": "origin/ffmpeg/archive/delete", "msg": msg}]


@app.task
def OriginFFmpegDistRespawn(origin_ffmpeg_dist_respawn):
    msg = json.loads(origin_ffmpeg_dist_respawn[1])
    logger.info(str(msg)+" should come here only when missing becomes active")
    msg["origin_ip"] = col3.find_one(
        {"stream_ip": msg["stream_ip"], "stream_id": msg["stream_id"], "dist_ip": msg["dist_ip"]})["origin_ip"]
    return {"topic": "origin/ffmpeg/dist/respawn", "msg": msg}




@app.task
def DistStat(msg):
    msg = json.loads(msg)
    for i in col2.find():
        if msg["dist_ip"] == i["dist_ip"]:
            col2.update({"dist_ip": msg["dist_ip"]}, {
                        "$set": {"NClients": int(msg["NClients"])}}, upsert=True)


@app.task
def AddUser(user_add):
    msg = json.loads(user_add[1])
    if col5.count() == 0:
        col5.insert_one({"User": msg["User"], "Password": msg["Password"]})
        return {"topic": "lbsresponse/user/add", "msg": True}
    else:
        usernames = search_users()
        if msg["User"] not in usernames:
            col5.insert_one({"User": msg["User"], "Password": msg["Password"]})
            return {"topic": "lbsresponse/user/add", "msg": True}
        else:
            return {"topic": "lbsresponse/user/add", "msg": False}


@app.task
def DelUser(user_del):
    msg = json.loads(user_del[1])
    if col5.count() == 0:
        return{"topic": "lbsresponse/user/del", "msg": False}
    else:
        usernames = search_users()
        if msg["User"] not in usernames:
            return{"topic": "lbsresponse/user/del", "msg": False}
        else:
            count = 0
            for i in col5.find():
                if i["User"] == msg["User"]:
                    if i["Password"] == msg["Password"]:
                        col5.delete_one({"User": msg["User"]})
                        return {"topic": "lbsresponse/user/del", "msg": True}
                    else:
                        return {"topic": "lbsresponse/user/del", "msg": False}


@app.task
def VerifyUser(verify_user):
    print("Verifying ")
    print(verify_user)
    msg = json.loads(verify_user[1])
    if col5.count != 0:
        for i in col5.find():
            if i["User"] == msg["User"]:
                if i["Password"] == msg["Password"]:
                    return {"topic": "lbsresponse/verified", "msg": True}
    return {"topic": "lbsresponse/verified", "msg": False}
