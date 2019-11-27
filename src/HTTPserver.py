import os
from flask import Flask, request
from MQTTPubSub import MQTTPubSub
import json
import hashlib
from flask import Response
import logging
import sys
import loadbalancercelery as lbc
import threading


'''
    TODO: 
        Add logger
        Add Authorization decorator
    Done:
'''



''' Logger utility '''
logger = logging.getLogger("werkzeug")


''' Environment Variables '''
LB_IP = os.environ["LB_IP"]
HTTP_IP = os.environ["HTTP_IP"]
HTTP_PORT = os.environ["HTTP_PORT"]
MQTT_PORT = os.environ["MQTT_PORT"]
MQTT_UNAME = os.environ["MQTT_UNAME"]
MQTT_PASSWD = os.environ["MQTT_PASSWD"]
if HTTP_IP is None or HTTP_PORT is None:
    logger.info("Error! LB_IP and HTTP_PORT not set")
    sys.exit(0)




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
client = MQTTPubSub(mqParams)


def monitorTaskResult(res):
    ''' Celery result callback monitoring thread '''
    global client
    ''' Celery task monitor '''
    while(True):
        if res.ready():
            ret = res.get()
            if not ret:
                pass
            elif isinstance(ret, dict):
                client.publish(ret["topic"],
                               ret["msg"])
            elif isinstance(ret, list):
                for retDict in ret:
                    client.publish(retDict["topic"],
                                   retDict["msg"])
            return ret


''' HTTP App '''
app = Flask(__name__)


def verify_password(username, password):
    ''' Auth '''
    hashed_password = hashlib.sha512(password).hexdigest()
    msg = {"username": username, "password": hashed_password}
    ''' TODO: don't use mqtt for user authentiaction '''
    res = lbc.VerifyUser.delay(json.dumps(msg))
    ret = ret = monitorTaskResult(res)
    if ret["topic"] == "lbsresponse/verified" and ret["msg"] is True:
        return True
    else:
        return False


@app.route('/user', methods=['POST', 'DELETE', 'GET'])
def userfunc():
    '''
    Admin related tasks like adding user, deleting user
    '''
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
        msg = {"username": user_name, "password": hashed_password}
        res = lbc.AddUser.delay(json.dumps(msg))
        ret = monitorTaskResult(res)

        if ret["topic"] == "lbsresponse/user/add" and ret["msg"] is True:
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
        msg = {"username": user_name, "password": hashed_password}
        ''' TODO: Remove mqtt publish for creating user '''
        res = lbc.DelUser.delay(json.dumps(msg))
        ret = monitorTaskResult(res)
        if ret["topic"] == "lbsresponse/user/del" and ret["msg"] is True:
            return Response(json.dumps({"username":  user_name}),
                            status=200, mimetype="application/json")
        else:
            return Response(json.dumps({}), status=404,
                            mimetype="application/json")

    elif request.method == "GET":
        '''
            Get all users of the system
            Returns:
                str: If success - 200 [{"username": "username"},..]
        '''
        res = lbc.GetUsers.delay()
        ret = monitorTaskResult(res)
        return Response(ret["msg"],
                        status=200, mimetype='application/json')

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
                {"info": "processing"} or
                {"stream_id": "string", "rtmp": "string[uri],
                 "hls": "string[uri]", "rtsp": "string[uri]",
                 "info": string}
    '''
    if (verify_password(request.headers["username"],
                        request.headers["password"])):
        logger.info("Stream request received")
        data = request.get_json(force=True)
        stream_id = data["stream_id"]
        ''' TODO: make schema changes in accordance to schema '''
        msg = {"stream_id": stream_id}
        res = lbc.RequestStream.delay(json.dumps(msg))
        ret = monitorTaskResult(res)
        if isinstance(ret, list):
            m = [d for d in ret if d["topic"] == "lbsresponse/rtmp"][0]
            return Response(m["msg"],
                            status=200, mimetype="application/json")
        elif isinstance(ret, dict):
            return Response(ret["msg"],
                            status=200, mimetype="application/json")
    else:
        return Response(json.dumps({}), status=403,
                        mimetype='application/json')


@app.route('/streams', methods=['POST', 'DELETE', 'GET'])
def stream():
    '''
        Push/Delete stream to origin server or 
        Get all streams flowing into origin server
        Header: {"username": "username", "password": "password"}
    '''
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
            msg = {"stream_id": stream_id, "stream_ip": stream_ip}
            res = lbc.InsertStream.delay(json.dumps(msg))
            logger.info("Insert Stream sent request ")
            ret = monitorTaskResult(res)
            logger.info("Insert Stream got resp")

            if isinstance(ret, list):
                m = [d for d in ret if d["topic"] == "lbsresponse/stream/add"][0]
                return Response(m["msg"],
                                status=200, mimetype="application/json")
            elif isinstance(ret, dict):
                if ret["topic"] == "lbsresponse/stream/add":
                    return Response(ret["msg"],
                                    status=200, mimetype="application/json")

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
            stream_id = data["stream_id"]
            logger.info("Deleted Stream " + str(stream_id))
            ''' TODO: make schema changes in accordance to schema '''
            msg = {"stream_id": stream_id}
            res = lbc.DeleteStream.delay(json.dumps(msg))
            ret = monitorTaskResult(res)
            print(ret)

            if isinstance(ret, list):
                m = [d for d in ret if d["topic"] == "lbsresponse/stream/del"][0]
                if(m["msg"] is True):
                    return Response(json.dumps({}), status=200,
                                    mimetype="application/json")
                else:
                    return Response(json.dumps({}), status=404,
                                    mimetype="application/json")

            elif isinstance(ret, dict):
                if ret["msg"] is True:
                    return Response(json.dumps({}), status=200,
                                    mimetype="application/json")
                else:
                    return Response(json.dumps({}), status=404,
                                    mimetype="application/json")

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
            res = lbc.GetStreams.delay()
            ret = monitorTaskResult(res)
            return Response(ret["msg"], status=200,
                            mimetype="application/json")

    else:
        return Response(json.dumps({}), status=403,
                        mimetype='application/json')


@app.route('/origin', methods=['POST', 'DELETE', 'GET'])
def origin():
    '''
        Add, delete or show all origin servers
    '''
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
            msg = {"origin_id": origin_id, "origin_ip": origin_ip,
                   "num_clients": 0}
            res = lbc.InsertOrigin.delay(json.dumps(msg))
            ret = monitorTaskResult(res)
            if ret["topic"] == "lbsresponse/origin/add" and ret["msg"] is True:
                return Response(json.dumps({}),
                                status=200, mimetype="application/json")
            else:
                return Response(json.dumps(ret["msg"]), status=409,
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
            msg = {"origin_id": origin_id}
            res = lbc.DeleteOrigin.delay(json.dumps(msg))
            ret = monitorTaskResult(res)
            if isinstance(ret, list):
                m = [d for d in ret if d["topic"] == "lbsresponse/origin/del"][0]
                if(m["msg"] is True):
                    return Response(json.dumps({}), status=200,
                                    mimetype="application/json")
                else:
                    return Response(json.dumps({}), status=404,
                                    mimetype="application/json")

            elif isinstance(ret, dict):
                if ret["msg"] is True:
                    return Response(json.dumps({}), status=200,
                                    mimetype="application/json")
                else:
                    return Response(json.dumps({}), status=404,
                                    mimetype="application/json")

        elif request.method == "GET":
            '''
                Get info about all origin servers
                Returns:
                    str: If success - 200 [{"origin_ip": "uri-format",
                                            "origin_id": "xyz"}]
                         If failed - 404 {}
            '''
            res = lbc.GetOrigins.delay()
            ret = monitorTaskResult(res)
            return Response(ret["msg"], status=200,
                            mimetype='application/json')

    else:
        return Response(json.dumps({}), status=401,
                        mimetype='application/json')


@app.route('/dist', methods=['POST', 'DELETE', 'GET'])
def dist():
    '''
        Distribution api cb
    '''
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
            msg = {"dist_id": dist_id, "dist_ip": dist_ip, "num_clients": 0}
            res = lbc.InsertDist.delay(json.dumps(msg))
            ret = monitorTaskResult(res)
            if ret["topic"] == "lbsresponse/dist/add" and ret["msg"] is True:
                return Response(json.dumps({}), status=200,
                                mimetype="application/json")
            elif ret["topic"] == "lbsresponse/dist/add" and ret["msg"] is False:
                return Response(json.dumps({}), status=406,
                                mimetype="application/json")

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
            dist_id = data["dist_id"]
            logger.info("Deleted Distribution " + str(dist_id))
            msg = {"dist_id": dist_id}
            res = lbc.DeleteDist.delay(json.dumps(msg))
            ret = monitorTaskResult(res)
            print(ret)
            if isinstance(ret, list):
                m = [d for d in ret if d["topic"] == "lbsresponse/dist/del"][0]
                if(m["msg"] is True):
                    return Response(json.dumps({}), status=200,
                                    mimetype="application/json")
                else:
                    return Response(json.dumps({}), status=204,
                                    mimetype="application/json")

            elif isinstance(ret, dict):
                if ret["msg"] is True:
                    return Response(json.dumps({}), status=200,
                                    mimetype="application/json")
                else:
                    return Response(json.dumps({}), status=404,
                                    mimetype="application/json")

        elif request.method == "GET":
            '''
                Get info about all distribution servers
                Returns:
                    str: If success - 200 [{"dist_ip": "uri-format",
                                            "dist_id": "xyz"}]
                         If failed - 404 {}
            '''
            res = lbc.GetDists.delay()
            ret = monitorTaskResult(res)
            return Response(ret["msg"], status=200,
                            mimetype='application/json')

    else:
        return Response(json.dumps({}), status=403,
                        mimetype='application/json')


''' TODO: Later '''
@app.route('/archive', methods=['POST', 'DELETE', 'GET'])
def archivestream():
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
            except Exception as e:
                start_date = None
            try:
                end_date = data["endDate"]
            except Exception as e:
                end_date = None
            try:
                start_time = data["startTime"]
            except Exception as e:
                start_time = None
            try:
                end_time = data["endTime"]
            except Exception as e:
                end_time = None
            job_id = data["job_id"]
            msg = {"user_ip": user_ip, "stream_id": stream_id,
                   "start_date": start_date,
                   "start_time": start_time, "end_date": end_date,
                   "end_time": end_time, "job_id": job_id}
            res = lbc.ArchiveAdd.delay(json.dumps(msg))
            ret = monitorTaskResult(res)
            if isinstance(ret, list):
                m = [d for d in ret if d["topic"] == "lbsresponse/archive/add"][0]
                if(m["msg"] is True):
                    return Response(json.dumps({}), status=200,
                                    mimetype="application/json")
                else:
                    return Response(json.dumps({}), status=404,
                                    mimetype="application/json")

            elif isinstance(ret, dict):
                if ret["msg"] is True:
                    return Response(json.dumps({}), status=200,
                                    mimetype="application/json")
                else:
                    return Response(json.dumps({}), status=404,
                                    mimetype="application/json")

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
            except Exception as e:
                start_date = None
            try:
                end_date = data["end"]
            except Exception as e:
                end_date = None
            try:
                start_time = data["sTime"]
            except Exception as e:
                start_time = None
            try:
                end_time = data["eTime"]
            except Exception as e:
                end_time = None
            job_id = data["job"]
            msg = {"user_ip": user_ip, "stream_id": stream_id,
                   "start_date": start_date, "start_time": start_time,
                   "end_date": end_date, "end_time": end_time,
                   "job_id": job_id}
            res = lbc.ArchiveDel.delay(json.dumps(msg))
            ret = monitorTaskResult(res)
            if isinstance(ret, list):
                m = [d for d in ret if d["topic"] == "lbsresponse/archive/del"][0]
                if(m["msg"] is True):
                    return Response(json.dumps({}), status=200,
                                    mimetype="application/json")
                else:
                    return Response(json.dumps({}), status=204,
                                    mimetype="application/json")

            elif isinstance(ret, dict):
                if ret["msg"] is True:
                    return Response(json.dumps({}), status=200,
                                    mimetype="application/json")
                else:
                    return Response(json.dumps({}), status=404,
                                    mimetype="application/json")

        elif request.method == "GET":
            ''' TODO '''
            res = lbc.GetArchives.delay()
            ret = monitorTaskResult(res)
            return Response(json.dumps(ret["msg"]), status=200,
                            mimetype='application/json')
    else:
        return Response(json.dumps({}), status=403, mimetype='application/json')


if __name__ == "__main__":
    logger.info("Starting server on - ", HTTP_IP + ":" + HTTP_PORT)
    client.run()
    app.run(host=HTTP_IP, port=int(HTTP_PORT), debug=True)
