from __future__ import absolute_import
from celery import Celery
import logging
import json
import pymongo
import time
import os


'''
    loadbalancercelery.py
    Contains all the loadbalancing and bookeeping tasks
    TODO:
        1. Validation at mongodb
        2. Parameterize celery app parameters
        3. Return json everywhere
        4. Spawn rtmp dist push as soon as it is added in origin

'''


''' Celery app '''
app = Celery('loadbalancercelery', backend="redis://", broker="redis://")

'''Logger '''
logger = logging.getLogger(__name__)


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
        if bool(res.inserted_id):
            return 1
        else:
            return 0

    def update(self, key, doc):
        res = self.collection.update_one(key, {"$set": doc}, upsert=True)
        return res.modified_count

    def findOne(self, doc, args=None):
        res = self.collection.find_one(doc, {"_id": 0})
        print(res)
        logger.info(res)
        if not bool(res):
            return {}
        return res

    def findAll(self, doc=None, args=None):
        '''
            Input:
                doc - refine results
                args - exlude keys
        '''
        ''' Exclude id from result '''
        arg = {"_id": 0}
        if bool(args):
            arg.update(args)
        if bool(doc):
            res = list(self.collection.find(doc, arg))
            return res
        else:
            return list(self.collection.find({}, arg))

    def delete(self, doc):
        res = self.collection.delete_one(doc)
        return res.deleted_count

    def deleteMany(self, doc):
        res = self.collection.delete_many(doc)
        return res.deleted_count

    def count(self):
        return self.collection.count()


''' mongo initializations '''
''' TODO: Parameterize '''
uname = os.environ["MONGO_INITDB_ROOT_USERNAME"]
pwd = os.environ["MONGO_INITDB_ROOT_PASSWORD"]
url = os.environ["MONGO_URL"]
mongoclient = pymongo.MongoClient("mongodb://" + uname + ":" + pwd +
                                  "@" + url + ":27017/", connect=False)
mongoDB = mongoclient["vid-iot"]

'''
    {origin_id: string, origin_ip: string[uri], num_clients: int}
'''
originTable = Table(mongoDB, "originTable")

'''
    {cmd: string, from_ip: string, stream_ip: string,
     to_ip: string, rtsp_cmd: string}
'''
ffmpegProcsTable = Table(mongoDB, "ffmpegProcsTable")

'''
    {stream_id: string, stream_ip: string[uri],
     origin_ip: string[uri], dist_ip: string[uri]
     status: enum[onboadrding, deleting, active, down]
     }
'''
streamsTable = Table(mongoDB, "streams")

'''
    {dist_id: string, dist_ip: string[uri], num_clients: int}
'''
distTable = Table(mongoDB, "distTable")

'''
    {"user_ip": string, "stream_id": string,
     "start_date": string[date],
     "start_time": string[time], "end_date": string[date],
     "end_time": string[time], "job_id": string}
'''
archivesTable = Table(mongoDB, "archivesTable")

'''
    {"username": "string", "password": "string"}
'''
usersTable = Table(mongoDB, "usersTable")


''' TASKS '''


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
        Input: {origin_id: string, origin_ip: string[uri]}
        Trigger: celeryLBmain.py
        Handles: Origin insertion requests
        Response: HTTPServer.py
    '''
    logger.info("Inserting Origin")
    msg = json.loads(msg)
    ret = originTable.insertOne(msg)
    if ret == 1:
        logger.info("Added origin " + msg["origin_id"])
        return {"topic": "lbsresponse/origin/add", "msg": True}
    else:
        logger.info("Origin already present" + msg["origin_ip"])
        return {"topic": "lbsresponse/origin/add", "msg": False}


@app.task
def OriginStat(msg):
    '''
        Input: {origin_id: string, num_clients: number}
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
        Input: {origin_id: string}
        Trigger: celeryLBmain.py
        Handles: Origin deletion requests
        Response: HTTPServer.py
        TODO: Kill origin streams
    '''
    logger.info("Deleting Origin")
    msg = json.loads(msg)
    ret = originTable.delete({"origin_id": msg["origin_id"]})
    if ret == 1:
        logger.info("Deleted origin " + msg["origin_id"])
        ffmpegProcsTable.deleteMany({"to_id": msg["origin_id"]})
        ffmpegProcsTable.deleteMany({"origin_id": msg["origin_id"]})
        streamsTable.deleteMany({"from_id": msg["origin_id"]})
        logger.info("Origin Deleted----> ID:" + " ID:"+str(msg["origin_id"]))
        return [{"topic": "lbsresponse/origin/del", "msg": True},
                {"topic": "origin/ffmpeg/killall", "msg": json.dumps(msg)}]
    else:
        return {"topic": "lbsresponse/origin/del", "msg": False}


@app.task
def UpdateOriginStream(msg):
    '''
        Input: {cmd: string, from_ip: string, stream_id: string,
                to_ip: string, rtsp_cmd: string}
        Trigger: OriginCelery.py
        Handles: adding ffmpeg stream to db once
                 it's added at the origin server
    '''
    '''
       TODO: 'status' field needs to be added in the ffmpegProcsTable row
              status will indicate if the ffmpeg process is alive or not alive
              Check with pid from OriginFfmpegSpawn
    '''
    msg = json.loads(msg)
    logger.info(msg["stream_id"] +
                " stream has been started to origin " + msg["to_ip"])
    ffmpegProcsTable.insertOne(msg)
    return 0


@app.task
def ReqAllOriginStreams(msg):
    '''
        Input: {origin_id: string}
        Trigger: OriginCelery.py
        Handles: show all streams belonging to an origin ip
                 it's added at the origin server
    '''
    msg = json.loads(msg)
    streams = streamsTable.findAll(msg)
    resp = {"origin_id": msg["origin_id"], "stream_list": streams}
    return {"topic": "lb/request/origin/streams", "msg": json.dumps(resp)}


@app.task
def ReqAllDistStreams(msg):
    '''
        Input: {dist_id: string}
        Trigger: OriginCelery.py
        Handles: show all streams belonging to a dist id
                 after it's added at the dist server
    '''
    msg = json.loads(msg)
    streams = streamsTable.findAll(msg)
    resp = {"dist_id": msg["dist_id"], "stream_list": streams}
    return {"topic": "lb/request/dist/streams", "msg": json.dumps(resp)}


@app.task
def InsertDist(msg):
    '''
        Input: {dist_id: string, dist_ip: string[uri]}
        Trigger: celeryLBmain.py
        Handles: Dist insertion requests
        Response: HTTPServer.py
    '''
    logger.info("Inserting Dist")
    msg = json.loads(msg)
    ret = distTable.insertOne(msg)
    if ret == 1:
        logger.info("Added dist" + msg["dist_id"])
        return {"topic": "lbsresponse/dist/add", "msg": True}
    else:
        logger.info("Dist already present" + msg["dist_ip"])
        return {"topic": "lbsresponse/dist/add", "msg": False}


@app.task
def DeleteDist(msg):
    '''
        Input: {dist_id: string}
        Trigger: celeryLBmain.py
        Handles: Dist deletion requests
        Response: HTTPServer.py
    '''
    logger.info("Deleting Dist")
    msg = json.loads(msg)
    ret = distTable.delete({"dist_id": msg["dist_id"]})
    killlist = []
    if ret == 1:
        logger.info("Deleted dist " + msg["dist_id"])
        killlist = ffmpegProcsTable.findAll({"dist_id": msg["dist_id"]})
        ffmpegProcsTable.deleteMany({"to_id": msg["dist_id"]})
        ffmpegProcsTable.deleteMany({"dist_id": msg["dist_id"]})
        streamsTable.deleteMany({"from_id": msg["dist_id"]})
        return [{"topic": "lbsresponse/dist/del", "msg": True},
                {"topic": "origin/ffmpeg/kill", "msg": json.dumps(killlist)}]
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
        Input: {dist_id: string, dist_clients: number}
        Trigger: celeryLBmain.py
        Handles: Update num_clients
        Response: None
    '''
    msg = json.loads(msg)
    distTable.update({"dist_id": msg["dist_id"]},
                     {"num_clients": msg["num_clients"]})


@app.task
def OriginFfmpegDistPush(msg):
    '''
        Input: {stream_id: string, cmd: string, rtsp_cmd: string,
                from_ip: string, to_ip: string}

        Trigger: originCelery.py
        Handles: Inserts origin stream info into db upon succesful
                 pull from camera
        Response: HTTPServer
        TODO: This is called from origin after stream has been pushed
    '''
    msg = json.loads(msg)
    logger.info(msg)
    logger.info(msg["stream_id"] +
                " stream push has been started from origin " +
                msg["from_ip"]+" to distribution " + msg["to_ip"])

    ffmpegProcsTable.insertOne({"cmd": msg["cmd"], "to_ip": msg["to_ip"],
                                "from_ip": msg["from_ip"],
                                "stream_id": msg["stream_id"],
                                "rtsp_cmd": msg["rtsp_cmd"]})

    ''' TODO: Handle if update didn't happen '''
    streamsTable.update({"stream_id": msg["stream_id"],
                         "origin_ip": msg["from_ip"]},
                        {"dist_ip": msg["to_ip"]})
    ''' TODO: return something else '''
    return 0


@app.task
def OriginFfmpegRespawn(msg):
    '''
        Input: {stream_id: string}
        Trigger: celeryLBmain.py
        Handles: Respawn origin stream
        Response: HTTPServer.py
        TODO: Respawn based on logic
        TODO: Use OriginFfmpegSpawn instead
    '''
    msg = json.loads(msg)
    logger.info("Respawning " + msg["stream_id"])
    logger.info(msg["origin_id"])
    origin = originTable.findOne({"origin_id": msg["origin_id"]})
    msg["origin_ip"] = origin["origin_ip"]
    return {"topic": "origin/ffmpeg/stream/spawn", "msg": json.dumps(msg)}


@app.task
def OriginFFmpegDistRespawn(msg):
    '''
        Input: {stream_id: string}
        Trigger: celeryLBmain.py
        Handles: Respawns origin to distribution published stream
        Response: HTTPServer.py
        TODO: Respawn based on logic
    '''
    msg = json.loads(msg)
    logger.info("Respawning" + msg["stream_id"])
    return {"topic": "origin/ffmpeg/dist/respawn", "msg": msg}


@app.task
def InsertStream(msg):
    '''
        Input: {stream_id: string, stream_ip: string}
        Trigger: celeryLBmain.py
        Handles: add a stream to origin server
        Response: HTTPServer.py, celeryLBmain.py
    '''
    msg = json.loads(msg)

    if originTable.count() == 0:
        logger.info("No Origin Server Present")
        return 0

    ''' Origin Load balancer logic '''
    origins = originTable.findAll()
    bestOrigin = {}
    bestNumClients = 100
    for origin in origins:
        if (origin["num_clients"] < bestNumClients):
            bestOrigin = origin
            bestNumClients = origin["num_clients"]
    origin = bestOrigin

    ''' TODO: Add 'status' to the streamsTable '''
    stream_ips = []
    stream_ids = []
    if streamsTable.count() != 0:
        for stream in streamsTable.findAll({"origin_ip": origin["origin_ip"]}):
            stream_ips.append(stream["stream_ip"])
            stream_ids.append(stream["stream_id"])

    if msg["stream_ip"] not in stream_ips:
        if msg["stream_id"] not in stream_ids:
            streamsTable.insertOne({"stream_ip": msg["stream_ip"],
                                    "stream_id": msg["stream_id"],
                                    "origin_ip": origin["origin_ip"],
                                    "origin_id": origin["origin_id"],
                                    "status": "onboarding",
                                    "dist_ip": ""})
            logger.info("Added stream " + msg["stream_id"] +
                        " with IP " + msg["stream_ip"] +
                        " to " + origin["origin_id"])
            out = json.dumps({"origin_ip": origin["origin_ip"],
                              "origin_id": origin["origin_id"],
                              "stream_id": msg["stream_id"],
                              "stream_ip": msg["stream_ip"]})
            #####################################################################
            #Ab: Multi-Dist-Server-Mode Changes
            #The following logic for spawning from origin-dist should anyway
            #be moved from Request to this place for multi-dist-server-mode
            #For multiple distribution servers, issue origin/ffmpeg/dist/ spawn 
            #origin/ffmpeg/dist/stat here with the appropriate params
            # if (0)
            #   #Choose the distribution server to which origin should spawn
            #   dists = choose_dists_to spawn()
            #   for dist in dists:
            #     resp = {"origin_id": stream["origin_id"],
            #             "origin_ip": stream["origin_ip"],
            #             "dist_id": dist["dist_id"], "dist_ip": dist["dist_ip"],
            #             "stream_id": stream["stream_id"],
            #             "stream_ip": stream["stream_ip"]}
            #    #Append to the return command the following 
            #     dist_spawn_msg =  {"topic": "origin/ffmpeg/dist/spawn",
            #              "msg": json.dumps(resp)}
            #     dist_stat_spawn_msg =  {"topic": "origin/ffmpeg/dist/stat/spawn",
            #              "msg": json.dumps(resp)}
            #####################################################################
            return [{"topic": "lbsresponse/stream/add",
                     "msg": json.dumps({"info": "inserting"})},
                    {"topic": "origin/ffmpeg/stream/spawn", "msg": out},
                    {"topic": "origin/ffmpeg/stream/stat/spawn", "msg": out}]
        else:
            logger.warning("Stream ID " + msg["stream_id"],
                           " to ", origin["origin_id"],
                           " already present.  Choose different ID.")
            return {"topic": "lbsresponse/stream/add",
                    "msg": json.dumps({"info": "present"})}
    else:
        logger.warning("Stream IP " + msg["stream_ip"],
                       " to ", origin["origin_id"], " already present")
        return {"topic": "lbsresponse/stream/add",
                "msg": json.dumps({"info": "fail"})}


@app.task
def DeleteStream(msg):
    '''
        Input: {stream_id: string}
        Trigger: celeryLBmain.py
        Handles: delete a stream of the origin server
        Response: HTTPServer.py
    '''
    msg = json.loads(msg)
    killlist = []
    streams = streamsTable.findAll(msg)
    logger.info("Deleting " + msg["stream_id"])
    if len(streams) is 0:
        logger.info("Stream " + msg["stream_id"] + " not found")
        return {"topic": "lbsresponse/stream/del", "msg": False}
    else:
        killlist = json.dumps(ffmpegProcsTable.findAll(msg))
        streamsTable.delete(msg)
        ffmpegProcsTable.deleteMany(msg)
        return [{"topic": "lbsresponse/stream/del", "msg": True},
                {"topic": "origin/ffmpeg/kill", "msg": killlist}]


@app.task
def RequestStream(msg):
    '''
        Input: {stream_id: string}
        Trigger: celeryLBmain.py
        Handles: Gives the user a stream from the distribution server
        Response: HTTPServer.py
        TODO: Spawn stream irrespective of user asking for it
    '''
    msg = json.loads(msg)

    stream = streamsTable.findOne(msg)
    #Not required for same-origin-dist-mode
    #ffproc = ffmpegProcsTable.findOne(msg)

    if not bool(stream):
        ''' Steram not present at the origin server '''
        logger.error("Stream not present")
        return {"topic": "lbsresponse/rtmp",
                "msg": json.dumps({"info": "unavailable"})}
    elif (stream["status"] != "active"):
        ''' Steram present but not active at the origin server '''
        logger.error("Stream not active")
        return {"topic": "lbsresponse/rtmp",
                "msg": json.dumps({"info": "not active"})}
    else :  
        ''' All required conditions to send link are met '''
        logger.info("Stream " + msg["stream_id"] + " already present")
        userresp = {"stream_id": msg["stream_id"],
                    "rtmp": "rtmp://" + stream["origin_ip"] +
                    ":1935/dynamic/" + msg["stream_id"],
                    "hls": "http://" + stream["origin_ip"] +
                           ":8080/hls/" + msg["stream_id"] + ".m3u8",
                    "info": "active"}
        return {"topic": "lbsresponse/rtmp", "msg": json.dumps(userresp)}

@app.task
def RequestStreamMultiDist(msg):
    '''
        Input: {stream_id: string}
        Trigger: celeryLBmain.py
        Handles: Gives the user a stream from the distribution server
        Response: HTTPServer.py
        TODO: Spawn stream irrespective of user asking for it
    '''
    msg = json.loads(msg)

    stream = streamsTable.findOne(msg)
    ffproc = ffmpegProcsTable.findOne(msg)

    if not bool(stream):
        ''' Steram not present at the origin server '''
        logger.error("Stream not present")
        return {"topic": "lbsresponse/rtmp",
                "msg": json.dumps({"info": "unavailable"})}

    if bool(stream) and not bool(ffproc):

        ''' Load balancer logic '''
        dists = distTable.findAll()
        bestDist = {}
        bestNumClients = 100
        for dist in dists:
            if (dist["num_clients"] < bestNumClients):
                bestDist = dist
        dist = bestDist
        resp = {"origin_id": stream["origin_id"],
                "origin_ip": stream["origin_ip"],
                "dist_id": dist["dist_id"], "dist_ip": dist["dist_ip"],
                "stream_id": stream["stream_id"],
                "stream_ip": stream["stream_ip"]}
        userresp = {"info": "processing"}

        return [{"topic": "lbsresponse/rtmp",
                 "msg": json.dumps(userresp)},
                {"topic": "origin/ffmpeg/dist/spawn",
                 "msg": json.dumps(resp)},
                {"topic": "dist/ffmpeg/stream/stat/spawn",
                 "msg": json.dumps(resp)},
                ]

    elif bool(stream) and bool(ffproc):
        ''' All required conditions to send link are met '''
        logger.info("Stream " + msg["stream_id"] + " already present")
        userresp = {"stream_id": msg["stream_id"],
                    "rtmp": "rtmp://" + ffproc["to_ip"] +
                    ":1935/dynamic/" + msg["stream_id"],
                    "hls": "http://" + ffproc["to_ip"] +
                           ":8080/hls/" + msg["stream_id"] + ".m3u8",
                    "info": "active"}
        return {"topic": "lbsresponse/rtmp", "msg": json.dumps(userresp)}



@app.task
def GetStreams():
    '''
        Input: {}
        Trigger: celeryLBmain.py
        Handles: Shows all stream available
        Response: HTTPServer.py
    '''
    streams = streamsTable.findAll()
    return {"topic": "lbsresponse/stream/all", "msg": json.dumps(streams)}


@app.task
def StreamStat(msg):
    '''
        Input: {"stream_id": string, "status": string[up, down]}
        Trigger: OriginCelery.py
        Handles: Updates the status of the stream
    '''
    msg = json.loads(msg)
    streamsTable.update({"stream_id": msg["stream_id"]},
                        {"status": msg["status"]})
    return 0





@app.task
def ArchiveAdd(msg):
    '''
        Input: {"user_ip": string, "stream_id": string,
                "start_date": string[date],
                "start_time": string[time], "end_date": string[date],
                "end_time": string[time], "job_id": string}

        Trigger: celeryLBmain.py
        Handles: Add an archive
        Response: HTTPServer.py
        TODO: Archives unlike streams belong to a user
    '''
    msg = json.loads(msg)
    logger.info("Adding archive")
    stream = streamsTable.findone({"stream_id": msg["stream_id"]})
    archive = archivesTable.findone({"stream_id": msg["stream_id"]})
    if (len(stream) is 1) and (len(archive) is 0):
        logger.info("Archiving " + msg["stream_id"])
        archivesTable.insertOne(msg)
        return [{"topic": "lbsresponse/archive/add", "msg": True},
                {"topic": "origin/ffmpeg/archive/add", "msg": msg}]
    else:
        return {"topic": "lbsresponse/archive/add", "msg": False}


@app.task
def ArchiveDel(msg):
    '''
        Input: {"user_ip": string, "stream_id": string,
                "start_date": string[date],
                "start_time": string[time], "end_date": string[date],
                "end_time": string[time], "job_id": string}

        Trigger: celeryLBmain.py
        Handles: Add an archive
        Response: HTTPServer.py
        TODO: Archives unlike streams belong to a user
    '''
    msg = json.loads(msg)
    stream = streamsTable.findone({"stream_id": msg["stream_id"]})
    archive = archivesTable.findone({"stream_id": msg["stream_id"]})
    if (len(stream) is not 0) and (len(archive) is not 0):
        logger.info("Deleting archive for" + msg["stream_id"])
        archivesTable.delete(stream["stream_id"])
        return [{"topic": "lbsresponse/archive/del", "msg": True},
                {"topic": "origin/ffmpeg/archive/delete", "msg": msg}]
    else:
        logger.info("Archive for " + msg["stream_id"], " not present")
        return {"topic": "lbsresponse/archive/del", "msg": False}


@app.task
def GetArchives():
    '''
        Input: {}
        Trigger: celeryLBmain.py
        Handles: Show all archives
        Response: HTTPServer.py
        TODO: Archives unlike streams belong to a user
    '''
    resp = archivesTable.findAll()
    return {"topic": "lbsresponse/archive/all", "msg": json.dumps(resp)}


@app.task
def GetUsers():
    '''
        Input: {}
        Trigger: celeryLBmain.py
        Handles: Show all users of the server
        Response: HTTPServer.py
    '''
    users = usersTable.findAll(args={"password": 0})
    logger.info("Showing all users")
    return {"topic": "lbsresponse/user/all", "msg": json.dumps(users)}


@app.task
def AddUser(msg):
    '''
        Input: {"username": string, "password": string}
        Trigger: celeryLBmain.py
        Handles: Add user to the server
        Response: HTTPServer.py
    '''
    msg = json.loads(msg)
    user = usersTable.findOne({"username": msg["username"]})
    if not bool(user):
        logger.info("Added user" + msg["username"])
        usersTable.insertOne(msg)
        return {"topic": "lbsresponse/user/add", "msg": True}
    else:
        return {"topic": "lbsresponse/user/add", "msg": False}


@app.task
def DelUser(msg):
    '''
        Input: {"username": string, "password": string}
        Trigger: celeryLBmain.py
        Handles: Delete user to the server
        Response: HTTPServer.py
    '''
    msg = json.loads(msg)
    logger.info("Deleting " + msg["username"])
    user = usersTable.findOne({"username": msg["username"]})
    logger.info("User is ... " + str(user))
    if not bool(user):
        logger.info("User " + msg["username"] + " not present")
        return {"topic": "lbsresponse/user/del", "msg": False}
    else:
        usersTable.delete(msg)
        logger.info("User " + msg["username"] + " deleted")
        return {"topic": "lbsresponse/user/del", "msg": True}


@app.task
def VerifyUser(msg):
    '''
        Input: {"username": string, "password": string}
        Trigger: celeryLBmain.py
        Handles: Verify if user is valid
        Response: HTTPServer.py
    '''
    msg = json.loads(msg)
    logger.info("Verifying " + msg["username"])
    user = usersTable.findOne({"username": msg["username"]})
    if bool(user):
        if msg["password"] != user["password"]:
            return {"topic": "lbsresponse/verified", "msg": False}
    return {"topic": "lbsresponse/verified", "msg": True}
