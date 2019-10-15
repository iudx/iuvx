import os
from flask import Flask, request
from MQTTPubSub import MQTTPubSub
import json
import hashlib
from flask import Response
import logging
import sys
import time

'''
    TODO: 
        Add logger
        (R) Dist
        (R) Stream
        (R) ReqStream
        (R) Auth
        (N) Timeout instead of while loop
    Doing:
        (R) Origin
    Done:
'''


''' Logger utility '''
logger = logging.getLogger("werkzeug")


''' Environment Variables '''
LB_IP = os.environ["LB_IP"]
LB_PORT = os.environ["LB_PORT"]
MQTT_PORT = os.environ["MQTT_PORT"]
MQTT_UNAME = os.environ["MQTT_UNAME"]
MQTT_PASSWD = os.environ["MQTT_PASSWD"]
if LB_IP is None or LB_PORT is None:
    logger.info("Error! LB_IP and LB_PORT not set")
    sys.exit(0)


''' Global variables '''
stream_link = ""
''' user params '''
addusers = ""
delusers = ""
verified = ""
allusers = ""
''' origin params '''
allorigins = ""
addorigin = ""
delorigin = ""
''' dist params '''
alldists = ""
adddists = ""
deldists = ""
''' stream params '''
addstreams = ""
allstreams = ""
delstreams = ""
''' archive params '''
allarchives = ""
addarchives = ""
delarchives = ""


''' Seconds '''
timeout = 5


def on_message(client, userdata, message):
    ''' MQTT Subscribe callback '''
    '''
        TODO: make all messages json
        TODO: Add time out
    '''
    global msg, action
    global addusers, delusers, verified, allusers
    global allorigins, addorigin, delorigin, alldists
    global adddists, deldists, addstreams, allstreams
    global delstreams, allarchives, addarchives, delarchives
    global stream_link
    msg = message.payload
    topic = message.topic
    print(msg)
    print(topic)
    if topic == "lbsresponse/rtmp":
        stream_link = msg
    if topic == "lbsresponse/user/add":
        addusers = msg
    if topic == "lbsresponse/user/del":
        delusers = msg
    if topic == "lbsresponse/verified":
        verified = msg
    if topic == "lbsresponse/archive/all":
        allarchives = msg
    if topic == "lbsresponse/user/all":
        allusers = msg
    if topic == "lbsresponse/origin/all":
        allorigins = msg
    if topic == "lbsresponse/dist/all":
        alldists = msg
    if topic == "lbsresponse/stream/all":
        allstreams = msg
    if topic == "lbsresponse/origin/add":
        addorigin = msg
    if topic == "lbsresponse/origin/del":
        delorigin = msg
    if topic == "lbsresponse/stream/add":
        addstreams = msg
    if topic == "lbsresponse/stream/del":
        delstreams = msg
    if topic == "lbsresponse/dist/add":
        adddists = msg
    if topic == "lbsresponse/dist/del":
        deldists = msg
    if topic == "lbsresponse/archive/add":
        addarchives = msg
    if topic == "lbsresponse/archive/del":
        delarchives = msg


''' MQTT params and client '''
mqParams = {}
mqParams["url"] = LB_IP
''' TODO: read this port from env variables '''
mqParams["port"] = int(MQTT_PORT)
mqParams["username"] = MQTT_UNAME
mqParams["password"] = MQTT_PASSWD
mqParams["timeout"] = 60
''' Mqtt list of topics the server users '''
mqParams["topic"] = [
        ("lbsresponse/archive/add", 0), ("lbsresponse/archive/del", 0),
        ("lbsresponse/user/add", 0), ("lbsresponse/user/del", 0),
        ("lbsresponse/dist/add", 0), ("lbsresponse/dist/del", 0),
        ("lbsresponse/stream/add", 0), ("lbsresponse/stream/del", 0),
        ("lbsresponse/origin/del", 0), ("lbsresponse/origin/add", 0),
        ("lbsresponse/user/all", 0), ("lbsresponse/origin/all", 0),
        ("lbsresponse/dist/all", 0), ("lbsresponse/stream/all", 0),
        ("lbsresponse/archive/all", 0), ("lbsresponse/verified", 0),
        ("lbsresponse/rtmp", 0), ("lbsresponse/user", 0)
        ]
mqParams["onMessage"] = on_message
client = MQTTPubSub(mqParams)

''' HTTP App '''
app = Flask(__name__)



''' 
    HTTP Callbacks -
     1. Create/Delete/Show users
     2. Create/Delete/Show origin/distribution/streams
'''


def verify_password(username, password):
    ''' Auth '''
    global verified, timeout
    hashed_password = hashlib.sha512(password).hexdigest()
    reqdict = {"username": username, "password": hashed_password}
    ''' TODO: don't use mqtt for user authentiaction '''
    client.publish("verify/user", json.dumps(reqdict))
    timeout_start = time.time()
    while time.time() < timeout_start + timeout and verified == "":
        continue
    retval = verified
    verified = ""
    if retval:
        return True
    else:
        return False


@app.route('/user', methods=['POST', 'DELETE', 'GET'])
def userfunc():
    '''
    Admin related tasks like adding user, deleting user
    '''
    global addusers, delusers, verified, allusers, timeout
    logger.info("Reached userfunc")
    if request.method == "POST":
        '''
            Create new user
            Args:
                dict (str): {"username": "username", "password": "password"}
            Returns:
                str: If success - 200 {}
                     If already - present 409 empty
        '''
        data = request.get_json(force=True)
        user_name = data["username"]
        user_pass = data["password"]
        hashed_password = hashlib.sha512(user_pass).hexdigest()
        reqdict = {"username": user_name, "password": hashed_password}
        ''' TODO: Remove mqtt publish for creating user '''
        client.publish("user/add", json.dumps(reqdict))
        timeout_start = time.time()
        while time.time() < timeout_start + timeout and addusers == "":
            continue
        retval = addusers
        addusers = ""
        if retval:
            return Response(json.dumps({}),
                            status=200, mimetype="application/json")
        else:
            return Response(json.dumps({}),
                            status=409, mimetype="application/json")

    elif request.method == "DELETE":
        '''
            Delete specified user
            Args:
                dict (str): {"username": "username", "password": "password"}
            Returns:
                str: If success - 204 {"username": "username"}
                     If no content - 404
        '''
        data = request.get_json(force=True)
        user_name = data["username"]
        user_pass = data["password"]
        hashed_password = hashlib.sha512(user_pass).hexdigest()
        reqdict = {"username": user_name, "password": hashed_password}
        ''' TODO: Remove mqtt publish for creating user '''
        client.publish("user/del", json.dumps(reqdict))
        timeout_start = time.time()
        while time.time() < timeout_start + timeout and delusers == "":
            continue
        retval = delusers
        delusers = ""
        if retval:
            return Response(json.dumps({"username":  user_name}),
                            status=204, mimetype="application/json")
        else:
            return Response(json.dumps({}), status=404,
                            mimetype="application/json")

    elif request.method == "GET":
        '''
            Get all users of the system
            Returns:
                str: If success - 200 [{"username": "username"},..]
        '''
        client.publish('user/get', "All users")
        timeout_start = time.time()
        while time.time() < timeout_start + timeout and allusers == "":
            continue
        retval = allusers
        allusers = ""
        if retval:
            return Response(retval,
                            status=200, mimetype='application/json')
        else:
            return Response(json.dumps({}), status=408,
                            mimetype='application/json')

    else:
        return Response(json.dumps({}), status=405,
                        mimetype='application/json')


@app.route('/request', methods=['POST'])
def reqstream():
    '''
        TODO
        Request for a  specified stream
        Header: {"username": "username", "password": "password"}
        Args:
            dict (str): {"stream_id": "xyz"}
        Returns:
            str: If success - 204 {"username": "username"}
                 If no content - 404
    '''
    if (verify_password(request.headers["username"],
                        request.headers["password"])):
        logger.info("Stream request received")
        stream_link = ""
        data = request.get_json(force=True)
        stream_id = data["stream_id"]
        ''' TODO: make schema changes in accordance to schema '''
        reqdict = {"stream_id": stream_id}
        client.publish("stream/request", json.dumps(reqdict))
        ''' TODO: Remove mqtt publish for creating user '''
        timeout_start = time.time()
        while time.time() < timeout_start + timeout and stream_link == "":
            continue
        retval = stream_link
        stream_link = ""
        if retval:
            ''' TODO: make schema changes in accordance to schema '''
            return Response(json.dumps({}), status=409, mimetype='application/json')
        else:
            return Response(retval,
                            status=200, mimetype="application/json")
    else:
        return Response(json.dumps({}), status=403, mimetype='application/json')


@app.route('/streams', methods=['POST', 'DELETE', 'GET'])
def stream():
    '''
    Push/Delete stream to origin server or 
    Get all streams flowing into origin server
    Header: {"username": "username", "password": "password"}
    '''
    global addstreams, allstreams, delstreams
    ''' TODO: Remove mqtt publish for creating user '''
    if(verify_password(request.headers["username"], request.headers["password"])):
        if request.method == "POST":
            '''
                Request for a  specified stream
                Args:
                    dict (str): {"stream_id": "xyz", "stream_ip": "uri-format"}
                    Note: stream_ip is a playback ip with uname and passwd
                Returns:
                    str: If success - 200 {origin_id, stream_id, origin_ip, stream_ip}
                         If failed - 409 {}
            '''
            data = request.get_json(force=True)
            stream_ip = data["stream_ip"]
            stream_id = data["stream_id"]
            logger.info("Added Stream " + str(stream_id))
            streamadddict = {"stream_id": stream_id, "stream_ip": stream_ip}
            client.publish("stream/add", json.dumps(streamadddict))
            timeout_start = time.time()
            while time.time() < timeout_start + timeout and addstreams == "":
                continue
            retval = addstreams
            addstreams = ""
            print("Insert streammmmmmmmmmmm ", retval)
            if retval:
                return Response(json.dumps(retval),
                                status=200, mimetype="application/json")
            else:
                return Response(json.dumps({}),
                                status=409, mimetype="application/json")

        elif request.method == "DELETE":
            '''
                Delete a  specified stream
                Args:
                    dict (str): {"stream_id": "xyz"}
                Returns:
                    str: If success - 200 {}
                         If failed - 404 {}
            '''
            data = request.get_json(force=True)
            stream_ip = data["stream_ip"]
            stream_id = data["stream_id"]
            logger.info("Deleted Stream " + str(stream_id))
            ''' TODO: make schema changes in accordance to schema '''
            streamdeldict = {"stream_id": stream_id}
            client.publish("stream/delete", json.dumps(streamdeldict))
            timeout_start = time.time()
            while time.time() < timeout_start + timeout and delstreams == "":
                continue
            retval = delstreams
            delstreams = ""
            if retval:
                return Response(json.dumps({}),
                                status=200, mimetype="application/json")
            else:
                return Response(json.dumps({}),
                                status=404, mimetype="application/json")

        elif request.method == "GET":
            '''
                Get info about all streams
                Args:
                    dict (str): {"stream_id": "xyz", "stream_ip": "uri-format"}
                Returns:
                    str: If success - 200 [{"origin_ip": "xyz", "origin_id": "xyz", 
                                            "streams": [
                                                "stream_id": "xyz",
                                                "stream_ip": "xyz"
                                            ]}
                         If failed - 404 {}
            '''
            client.publish("stream/get", json.dumps({}))
            timeout_start = time.time()
            while time.time() < timeout_start + timeout and allstreams == "":
                continue
            retval = allstreams
            allstreams = ""
            ''' TODO: make schema changes in accordance to schema '''
            if retval:
                return Response(retval, status=200, mimetype='application/json')
            else:
                return Response(json.dumps({}), status=408, mimetype='application/json')
        else:
            return Response(json.dumps({}), status=405, mimetype='application/json')

    else:
        return Response(json.dumps({}), status=403, mimetype='application/json')


@app.route('/origin', methods=['POST', 'DELETE', 'GET'])
def origin():
    '''
        Add, delete or show all origin servers
    '''
    global allorigins, addorigin, delorigin
    if(verify_password(request.headers["username"],
                       request.headers["password"])):
        if request.method == "POST":
            '''
                Add an origin server
                Args:
                    dict (str): {"origin_id": "xyz", "origin_ip": "uri-format"}
                Returns:
                    str: If success - 200 {}
                         If failed - 409 {}
            '''
            data = request.get_json(force=True)
            origin_ip = data["origin_ip"]
            origin_id = data["origin_id"]
            logger.info("Added Origin Server " + str(origin_ip))
            originadddict = {"origin_id": origin_id, "origin_ip": origin_ip,
                             "num_clients": 0}
            client.publish("origin/add", json.dumps(originadddict))
            timeout_start = time.time()
            while time.time() < timeout_start + timeout and addorigin == "":
                continue
            retval = addorigin
            addorigin = ""
            print("Adding originnnnnnnn")
            print(retval)
            if retval is True:
                return Response(json.dumps({}),
                                status=200, mimetype="application/json")
            else:
                return Response(json.dumps({}), status=409,
                                mimetype="application/json")

        elif request.method == "DELETE":
            '''
                Delete a  specified origin server
                Args:
                    dict (str): {"origin_id": "xyz"}
                Returns:
                    str: If success - 200 {}
                         If failed - 404 {}
            '''
            data = request.get_json(force=True)
            origin_id = data["origin_id"]
            logger.info("Deleted Origin Server " + str(origin_id))
            origindeldict = {"origin_id": origin_id}
            client.publish("origin/delete", json.dumps(origindeldict))
            while (delorigin == ""):
                continue
            retval = delorigin
            delorigin = ""
            if retval:
                return Response(json.dumps({}), status=200, mimetype="application/json")
            else:
                return Response(json.dumps({}), status=404, mimetype="application/json")

        elif request.method == "GET":
            '''
                Get info about all origin servers
                Returns:
                    str: If success - 200 [{"origin_ip": "uri-format", "origin_id": "xyz"}]
                         If failed - 404 {}
            '''
            client.publish("origin/get", "")
            timeout_start = time.time()
            while time.time() < timeout_start + timeout and allorigins == "":
                continue
            retval = allorigins
            allorigins = ""
            if retval:
                ''' TODO: make changes in accordance to schema '''
                return Response(retval, status=200, mimetype='application/json')
            else:
                return Response(json.dumps({}), status=408, mimetype='application/json')
        else:
            return Response(json.dumps({}), status=405, mimetype='application/json')
    else:
        return Response(json.dumps({}), status=403, mimetype='application/json')


@app.route('/dist', methods=['POST', 'DELETE', 'GET'])
def dist():
    '''
        Distribution api cb
    '''
    global alldists, adddists, deldists
    if(verify_password(request.headers["username"], request.headers["password"])):
        if request.method == "POST":
            '''
                Add a distribution server
                Args:
                    dict (str): {"dist_id": "xyz", "dist_ip": "uri-format"}
                Returns:
                    str: If success - 200 {}
                         If failed - 409 {}
            '''
            data = request.get_json(force=True)
            dist_ip = data["dist_ip"]
            dist_id = data["dist_id"]
            logger.info("Added Distribution "+str(dist_ip))
            distadddict = {"dist_id": dist_id, "dist_ip": dist_ip, "num_clients": 0}
            client.publish("dist/add", json.dumps(distadddict))
            timeout_start = time.time()
            while time.time() < timeout_start + timeout and adddists == "":
                continue
            retval = adddists
            adddists = ""
            if retval:
                return Response(json.dumps({}), status=200, mimetype="application/json")
            else:
                return Response(json.dumps({}), status=409, mimetype="application/json")

        elif request.method == "DELETE":
            '''
                Delete a distribution server
                Args:
                    dict (str): {"dist_id": "xyz"}
                Returns:
                    str: If success - 200 {}
                         If failed - 404 {}
            '''
            data = request.get_json(force=True)
            dist_id = data["id"]
            logger.info("Deleted Distribution "+str(dist_id))
            distdeldict = {"dist_id": dist_id}
            client.publish("dist/delete", json.dumps(distdeldict))
            while time.time() < timeout_start + timeout and deldists == "":
                continue
            retval = deldists
            deldists = ""
            if retval:
                return Response(json.dumps({}),
                                status=200, mimetype="application/json")
            else:
                return Response(json.dumps({}),
                                status=404, mimetype="application/json")

        elif request.method == "GET":
            '''
                Get info about all distribution servers
                Returns:
                    str: If success - 200 [{"dist_ip": "uri-format", "dist_id": "xyz"}]
                         If failed - 404 {}
            '''
            client.publish("dist/get", "")
            timeout_start = time.time()
            while time.time() < timeout_start + timeout and alldists == "":
                continue
            retval = alldists
            alldists = ""
            if retval:
                return Response(retval, status=200, mimetype='application/json')
            else:
                logger.log("Distribution", "Operation Failed")
                return Response(json.dumps({}), status=408, mimetype='application/json')
        else:
            logger.log("Distribution", "Request not supported")
            ''' TODO: make schema changes in accordance to schema '''
            return Response(json.dumps({"error": "Request not supported"}), status=405, mimetype='application/json')
    else:
        logger.log("Distribution", "Invalid Credentials")
        return Response(json.dumps({}), status=403, mimetype='application/json')


''' TODO: Later '''
@app.route('/archive', methods=['POST', 'DELETE', 'GET'])
def archivestream():
    global allarchives, addarchives, delarchives
    if(verify_password(request.headers["username"], request.headers["password"])):
        user_ip = request.remote_addr
        if request.method == "POST":
            '''
                Request for and archive stream
                Args:
                    dict (str): {"stream_id": "xyz", 
                                 "start_date": "date-forma",
                                 "end_date": "date-format",
                                 "start_time": "time-format",
                                 "end_time": "time-format",
                                 "job_id": "xyz"
                                 }
                Returns:
                    str: If success - 200 {}
                         If failed - 409 {}
            '''
            data = request.get_json(force=True)
            stream_id = data["stream_id"]
            try:
                start_date = data["startDate"]
            except:
                start_date = None
            try:
                end_date = data["endDate"]
            except:
                end_date = None
            try:
                start_time = data["startTime"]
            except:
                start_time = None
            try:
                end_time = data["endTime"]
            except:
                end_time = None
            job_id = data["job_id"]
            archivedict = {"user_ip": user_ip, "stream_id": stream_id, "start_date": start_date,
                           "start_time": start_time, "end_date": end_date, "end_time": end_time, "job_id": job_id}
            client.publish("archive/add", json.dumps(archivedict))
            timeout_start = time.time()
            while time.time() < timeout_start + timeout or addarchives == "":
                continue
            retval = addarchives
            addarchives = ""
            if retval:
                return Response(json.dumps({}), status=200, mimetype="application/json")
            else:
                return Response(json.dumps({}), status=409, mimetype="application/json")

        elif request.method == "DELETE":
            '''
                Delete and archived stream
                Args:
                Args:
                    dict (str): {"stream_id": "xyz", 
                                 "startDate": "date-forma",
                                 "endDate": "date-format",
                                 "startTime": "time-format",
                                 "endTime": "time-format",
                                 "job_id": "xyz"
                                 }
                Returns:
                    str: If success - 200 {}
                         If failed - 409 {}
            '''
            data = request.get_json(force=True)
            stream_id = data["id"]
            try:
                start_date = data["start"]
            except:
                start_date = None
            try:
                end_date = data["end"]
            except:
                end_date = None
            try:
                start_time = data["sTime"]
            except:
                start_time = None
            try:
                end_time = data["eTime"]
            except:
                end_time = None
            job_id = data["job"]
            archivedict = {"user_ip": user_ip, "stream_id": stream_id,
                           "start_date": start_date,
                           "start_time": start_time, "end_date": end_date,
                           "end_time": end_time, "job_id": job_id}
            client.publish("archive/delete", json.dumps(archivedict))
            timeout_start = time.time()
            while time.time() < timeout_start + timeout or delarchives == "":
                continue
            retval = delarchives
            delarchives = ""
            if retval:
                return Response(json.dumps({}), status=200,
                                mimetype="application/json")
            else:
                return Response(json.dumps({}), status=409,
                                mimetype="application/json")

        elif request.method == "GET":
            ''' TODO '''
            client.publish("archive/get", "All Archives")
            timeout_start = time.time()
            while time.time() < timeout_start + timeout or allarchives == "":
                continue
            retval = allarchives
            allarchives = ""
            if retval:
                return Response(json.dumps({"success": " Archive Streams Present: " +
                                            str(retval)}), status=200,
                                            mimetype='application/json')
            else:
                return Response(json.dumps({"error": " Operation Failed"}),
                                            status=408, mimetype='application/json')
        else:
            return Response(json.dumps({"error": "Request not supported"}),
                                        status=405, mimetype='application/json')
    else:
        return Response(json.dumps({"error": "Invalid Credentials"}), status=403, mimetype='application/json')


if __name__ == "__main__":
    logger.info("Starting server on - ", LB_IP + ":" + LB_PORT)
    client.run()
    app.run(host=LB_IP, port=int(LB_PORT), debug=True)
