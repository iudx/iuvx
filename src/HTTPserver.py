import os
import signal
from flask import Flask, request
import logging
import paho.mqtt.client as mqtt
from MQTTPubSub import MQTTPubSub
import time
import json
import hashlib
from flask_httpauth import HTTPBasicAuth
from flask import Response
from flask_cors import CORS
import os
import sys
sys.path.insert(1, './src/')



class Server():
    def __init__(self):
        self.LB_IP = os.environ["LB_IP"]
        self.LB_PORT = os.environ["LB_PORT"]
        if self.LB_IP is None or self.LB_PORT is None:
            logging.critical("Error! LB_IP and LB_PORT not set")
            sys.exit(0)
        logging.info("Starting server on - ", self.LB_IP + ":" + self.LB_PORT)

        self.auth = HTTPBasicAuth()
        self.log = logging.getLogger('werkzeug')
        self.log.setLevel(logging.ERROR)
        self.app = Flask(__name__)
        self.cors = CORS(self.app)

        self.self.stream_link = ""

        # userparams
        self.addusers = ""
        self.delusers = ""
        self.verified = ""
        self.allusers = ""


        # originparams
        self.allorigins = ""
        self.addorigin = ""
        self.delorigin = ""


        # distparams
        self.alldists = ""
        self.adddists = ""
        self.deldists = ""


        # streamparams:
        self.addstreams = ""
        self.allstreams = ""
        self.delstreams = ""


        # archiveparams
        self.allarchives = ""
        self.addarchives = ""
        self.delarchives = ""

        ''' Load MQTT Client '''
        mqParams = {}
        mqParams["url"] = self.LB_IP
        ''' TODO: read this port from env variables '''
        mqParams["port"] = 1883
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
        mqParams["onMessage"] = self.on_message
        self.client = MQTTPubSub(mqParams)

        def on_message(self, client, userdata, message):
            ''' TODO: make all messages json'''
            msg = message.payload.decode("utf-8")
            logging.info(msg)
            topic = message.topic.decode("utf-8")
            if topic == "lbsresponse/rtmp":
                self.self.stream_link = msg
            if topic == "lbsresponse/user/add":
                self.addusers = json.loads(msg)
            if topic == "lbsresponse/user/del":
                self.delusers = json.loads(msg)
            if topic == "lbsresponse/verified":
                self.verified = json.loads(msg)
            if topic == "lbsresponse/archive/all":
                self.self.allarchives = msg
            if topic == "lbsresponse/user/all":
                self.allusers = msg
            if topic == "lbsresponse/origin/all":
                self.self.allorigins = msg
            if topic == "lbsresponse/dist/all":
                self.self.alldists = msg
            if topic == "lbsresponse/stream/all":
                self.self.allstreams = msg
            if topic == "lbsresponse/origin/add":
                self.self.addorigin = json.loads(msg)
            if topic == "lbsresponse/origin/del":
                self.self.delorigin = json.loads(msg)
            if topic == "lbsresponse/stream/add":
                self.self.addstreams = json.loads(msg)
            if topic == "lbsresponse/stream/del":
                self.self.delstreams = json.loads(msg)
            if topic == "lbsresponse/dist/add":
                self.self.adddists = json.loads(msg)
            if topic == "lbsresponse/dist/del":
                self.self.deldists = json.loads(msg)
            if topic == "lbsresponse/archive/add":
                self.self.addarchives = json.loads(msg)
            if topic == "lbsresponse/archive/del":
                self.self.delarchives = json.loads(msg)

            @self.auth.verify_password
            def verify_password(self, username, password):
                hashed_password = hashlib.sha512(password).hexdigest()
                reqdict = {"User": username, "Password": hashed_password}
                ''' TODO: don't use mqtt for user authentiaction '''
                self.client.publish("verify/user", json.dumps(reqdict))
                while(self.verified == ""):
                    continue
                retval = self.verified
                self.verified = ""
                if retval:
                    return True
                else:
                    return False

            '''
                Admin related tasks like adding user, deleting user
            '''
            @self.app.route('/user', methods=['POST', 'DELETE', 'GET'])
            def userfunc():
                if request.method == "POST":
                    '''
                        Create new user
                        Args:
                            dict (str): {"username": "username", "password": "password"}
                        Returns:
                            str: If success - 200 {"username": "username"}
                                 If already - present 409 empty
                    '''
                    data = request.get_json(force=True)
                    user_name = data["username"]
                    user_pass = data["password"]
                    hashed_password = hashlib.sha512(user_pass).hexdigest()
                    reqdict = {"User": user_name, "Password": hashed_password}
                    ''' TODO: Remove mqtt publish for creating user '''
                    self.client.publish("user/add", json.dumps(reqdict))
                    while(self.addusers == ""):
                        continue
                    retval = self.addusers
                    self.addusers = ""
                    if retval:
                        return Response(json.dumps({"username":  user_name}),
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
                    reqdict = {"User": user_name, "Password": hashed_password}
                    ''' TODO: Remove mqtt publish for creating user '''
                    self.client.publish("user/del", json.dumps(reqdict))
                    while (self.delusers == ""):
                        continue
                    retval = self.delusers
                    self.delusers = ""
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
                    self.client.publish('user/get', "All users")
                    while(self.allusers == ""):
                        continue
                    retval = self.allusers
                    self.allusers = ""
                    if retval:
                        return Response(json.dumps({"username": json.loads(retval)}),
                                        status=200, mimetype='application/json')
                    else:
                        return Response(json.dumps({}), status=408,
                                        mimetype='application/json')

                else:
                    return Response(json.dumps({}), status=405,
                                    mimetype='application/json')

            '''
                Request Streams
            '''
            @self.app.route('/request', methods=['POST'])
            def reqstream():
                '''
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
                    logging.info("Stream request received")
                    self.stream_link = ""
                    data = request.get_json(force=True)
                    stream_id = data["stream_id"]
                    user_ip = request.remote_addr
                    ''' TODO: make schema changes in accordance to schema '''
                    reqdict = {"User_IP": user_ip, "Stream_ID": stream_id}
                    self.client.publish("stream/request", json.dumps(reqdict))
                    ''' TODO: Remove mqtt publish for creating user '''
                    while(self.stream_link == ""):
                        continue
                    retval = self.stream_link
                    self.stream_link = ""
                    if retval == "false":
                        ''' TODO: make schema changes in accordance to schema '''
                        return Response(json.dumps({}), status=409, mimetype='application/json')
                    else:
                        return Response(json.dumps({"success": retval+" .... Stream will be available in a while.."}),
                                        status=200, mimetype="application/json")
                else:
                    return Response(json.dumps({}), status=403, mimetype='application/json')

            '''
                Push/Delete stream to origin server or 
                Get all streams flowing into origin server
                Header: {"username": "username", "password": "password"}
            '''
            @self.app.route('/streams', methods=['POST', 'DELETE', 'GET'])
            def stream():
                ''' TODO: Remove mqtt publish for creating user '''
                if(verify_password(request.headers["username"], request.headers["password"])):
                    if request.method == "POST":
                        '''
                            Request for a  specified stream
                            Args:
                                dict (str): {"stream_id": "xyz", "stream_ip": "uri-format"}
                                Note: stream_ip is a playback ip with uname and passwd
                            Returns:
                                str: If success - 200 {}
                                     If failed - 409 {}
                        '''
                        data = request.get_json(force=True)
                        stream_ip = data["stream_ip"]
                        stream_id = data["stream_id"]
                        logging.info("Added Stream " + str(stream_id))
                        ''' TODO: make schema changes in accordance to schema '''
                        streamadddict = {"Stream_ID": stream_id, "Stream_IP": stream_ip}
                        self.client.publish("stream/add", json.dumps(streamadddict))
                        while(self.addstreams == ""):
                            continue
                        retval = self.addstreams
                        self.addstreams = ""
                        if retval:
                            return Response(json.dumps({}),
                                            status=200, mimetype="application/json")
                        else:
                            return Response(json.dumps({}),
                                            status=409, mimetype="application/json")

                    elif request.method == "DELETE":
                        '''
                            Delete a  specified stream
                            Args:
                                dict (str): {"stream_id": "xyz", "stream_ip": "uri-format"}
                            Returns:
                                str: If success - 200 {}
                                     If failed - 404 {}
                        '''
                        data = request.get_json(force=True)
                        stream_ip = data["stream_ip"]
                        stream_id = data["stream_id"]
                        logging.info("Deleted Stream " + str(stream_id))
                        ''' TODO: make schema changes in accordance to schema '''
                        streamdeldict = {"Stream_ID": stream_id, "Stream_IP": stream_ip}
                        self.client.publish("stream/delete", json.dumps(streamdeldict))
                        while (self.delstreams == ""):
                            continue
                        retval = self.delstreams
                        self.delstreams = ""
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
                        self.client.publish("stream/get", "All streams")
                        while(self.allstreams == ""):
                            continue
                        retval = self.allstreams
                        self.allstreams = ""
                        ''' TODO: make schema changes in accordance to schema '''
                        if retval:
                            return Response(json.dumps({"success": " Streams Present: "+str(retval)}), status=200, mimetype='application/json')
                        else:
                            return Response(json.dumps({"error": " Operation Failed"}), status=408, mimetype='application/json')
                    else:
                        return Response(json.dumps({"error": "Request not supported"}), status=405, mimetype='application/json')

                else:
                    return Response(json.dumps({"error": "Invalid Credentials"}), status=403, mimetype='application/json')

            '''
                Add, delete or show all origin servers
            '''
            @self.app.route('/origin', methods=['POST', 'DELETE', 'GET'])
            def origin():
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
                        logging.info("Added Origin Server " + str(origin_ip))
                        ''' TODO: make schema changes in accordance to schema '''
                        originadddict = {"Origin_ID": origin_id, "Origin_IP": origin_ip}
                        self.client.publish("origin/add", json.dumps(originadddict))
                        while(self.addorigin == ""):
                            continue
                        retval = self.addorigin
                        self.addorigin = ""
                        if retval:
                            return Response(json.dumps({}),
                                            status=200, mimetype="application/json")
                        else:
                            return Response(json.dumps({}), status=409,
                                            mimetype="application/json")

                    elif request.method == "DELETE":
                        '''
                            Delete a  specified origin server
                            Args:
                                dict (str): {"origin_id": "xyz", "stream_ip": "uri-format"}
                            Returns:
                                str: If success - 200 {}
                                     If failed - 404 {}
                        '''
                        data = request.get_json(force=True)
                        origin_ip = data["origin_ip"]
                        origin_id = data["origin_id"]
                        logging.info("Deleted Origin Server " + str(origin_ip))
                        origindeldict = {"Origin_ID": origin_id, "Origin_IP": origin_ip}
                        self.client.publish("origin/delete", json.dumps(origindeldict))
                        while (self.delorigin == ""):
                            continue
                        retval = self.delorigin
                        self.delorigin = ""
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
                        self.client.publish("origin/get", "All origins")
                        while(self.allorigins == ""):
                            continue
                        retval = self.allorigins
                        self.allorigins = ""
                        if retval:
                            ''' TODO: make schema changes in accordance to schema '''
                            return Response(json.dumps({"success": "Origin Servers Present: "+str(retval)}), status=200, mimetype='application/json')
                        else:
                            return Response(json.dumps({}), status=408, mimetype='application/json')
                    else:
                        return Response(json.dumps({}), status=405, mimetype='application/json')
                else:
                    return Response(json.dumps({}), status=403, mimetype='application/json')


            @self.app.route('/dist', methods=['POST', 'DELETE', 'GET'])
            # @auth.login_required
            def dist():
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
                        logging.info("Added Distribution "+str(dist_ip))
                        distadddict = {"Dist_ID": dist_id, "Dist_IP": dist_ip}
                        self.client.publish("dist/add", json.dumps(distadddict))
                        while(self.adddists == ""):
                            continue
                        retval = self.adddists
                        self.adddists = ""
                        if retval:
                            return Response(json.dumps({}), status=200, mimetype="application/json")
                        else:
                            return Response(json.dumps({}), status=409, mimetype="application/json")

                    elif request.method == "DELETE":
                        '''
                            Delete a distribution server
                            Args:
                                dict (str): {"dist_id": "xyz", "dist_ip": "uri-format"}
                            Returns:
                                str: If success - 200 {}
                                     If failed - 404 {}
                        '''
                        data = request.get_json(force=True)
                        dist_ip = data["ip"]
                        dist_id = data["id"]
                        logging.info("Deleted Distribution "+str(dist_ip))
                        distdeldict = {"Dist_ID": dist_id, "Dist_IP": dist_ip}
                        self.client.publish("dist/delete", json.dumps(distdeldict))
                        while (self.deldists == ""):
                            continue
                        retval = self.deldists
                        self.deldists = ""
                        if retval:
                            return Response(json.dumps({}), status=200, mimetype="application/json")
                        else:
                            return Response(json.dumps({}), status=404, mimetype="application/json")

                    elif request.method == "GET":
                        '''
                            Get info about all distribution servers
                            Returns:
                                str: If success - 200 [{"dist_ip": "uri-format", "dist_id": "xyz"}]
                                     If failed - 404 {}
                        '''
                        self.client.publish("dist/get", "All dists")
                        while(self.alldists == ""):
                            continue
                        retval = self.alldists
                        self.alldists = ""
                        if retval:
                            return Response(json.dumps({}), status=200, mimetype='application/json')
                        else:
                            logging.log("Distribution", "Operation Failed")
                            return Response(json.dumps({}), status=408, mimetype='application/json')
                    else:
                        logging.log("Distribution", "Request not supported")
                        ''' TODO: make schema changes in accordance to schema '''
                        return Response(json.dumps({"error": "Request not supported"}), status=405, mimetype='application/json')
                else:
                    logging.log("Distribution", "Invalid Credentials")
                    return Response(json.dumps({}), status=403, mimetype='application/json')

            ''' TODO: Later '''
            @self.app.route('/archive', methods=['POST', 'DELETE', 'GET'])
            # @auth.login_required
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
                        archivedict = {"User_IP": user_ip, "Stream_ID": stream_id, "start_date": start_date,
                                       "start_time": start_time, "end_date": end_date, "end_time": end_time, "job_id": job_id}
                        self.client.publish("archive/add", json.dumps(archivedict))
                        while(self.addarchives == ""):
                            continue
                        retval = self.addarchives
                        self.addarchives = ""
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
                        archivedict = {"User_IP": user_ip, "Stream_ID": stream_id, "start_date": start_date,
                                       "start_time": start_time, "end_date": end_date, "end_time": end_time, "job_id": job_id}
                        self.client.publish("archive/delete", json.dumps(archivedict))
                        while(self.delarchives == ""):
                            continue
                        retval = self.delarchives
                        self.delarchives = ""
                        if retval:
                            return Response(json.dumps({}), status=200, mimetype="application/json")
                        else:
                            return Response(json.dumps({}), status=409, mimetype="application/json")

                    elif request.method == "GET":
                        ''' TODO '''
                        self.client.publish("archive/get", "All Archives")
                        while(self.allarchives == ""):
                            continue
                        retval = self.allarchives
                        self.allarchives = ""
                        if retval:
                            return Response(json.dumps({"success": " Archive Streams Present: "+str(retval)}), status=200, mimetype='application/json')
                        else:
                            return Response(json.dumps({"error": " Operation Failed"}), status=408, mimetype='application/json')
                    else:
                        return Response(json.dumps({"error": "Request not supported"}), status=405, mimetype='application/json')
                else:
                    return Response(json.dumps({"error": "Invalid Credentials"}), status=403, mimetype='application/json')


        def start(self):
            self.client.run()
            self.app.run(host=self.LB_IP, port=self.LB_PORT, threaded=True, debug=True)

if __name__ == "__main__":
    server = Server()
    server.start()

