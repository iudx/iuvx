'''
    This script creates a test user and display
'''
import requests
import os
import sys
import json
import unittest
import time
import pymongo


class Vid():
    def __init__(self, HTTP_IP, HTTP_PORT, ROOT_UNAME, ROOT_PASSWD):
        self.HTTP_IP = HTTP_IP
        self.HTTP_PORT = HTTP_PORT
        self.ROOT_UNAME = ROOT_UNAME
        self.ROOT_PASSWD = ROOT_PASSWD
        self.test_cred = {"username": self.ROOT_UNAME,
                          "password": self.ROOT_PASSWD,
                          "accept": "application/json",
                          "Content-Type": "application/json"}
        self.root_link = "http://" + HTTP_IP + ":" + HTTP_PORT
        uname = os.environ["MONGO_INITDB_ROOT_USERNAME"]
        pwd = os.environ["MONGO_INITDB_ROOT_PASSWORD"]
        url = os.environ["MONGO_URL"]
        self.mongoclient = pymongo.MongoClient("mongodb://" + uname + ":" + pwd +
                                               "@" + url + ":27017/", connect=False)
        self.mongoclient.drop_database("vid-iot")

    def createUser(self, user_cred=None):
        ''' Root functions '''
        reqLink = self.root_link + "/user"
        if(user_cred is not None):
            resp = requests.post(reqLink, data=json.dumps(user_cred))
        else:
            resp = requests.post(reqLink, data=json.dumps(self.test_cred))
        return resp

    def deleteUser(self, user_cred=None):
        reqLink = self.root_link + "/user"
        if(user_cred is not None):
            resp = requests.delete(reqLink, data=json.dumps(user_cred))
        else:
            resp = requests.delete(reqLink, data=json.dumps(self.test_cred))
        return resp

    def allUsers(self):
        reqLink = self.root_link + "/user"
        resp = requests.get(reqLink)
        return resp

    def addOrigin(self, origin_id, origin_ip):
        reqLink = self.root_link + "/origin"
        d = json.dumps({"origin_id": origin_id, "origin_ip": origin_ip})
        resp = requests.post(reqLink, data=d, headers=self.test_cred)
        return resp

    def deleteOrigin(self, origin_id):
        reqLink = self.root_link + "/origin"
        d = json.dumps({"origin_id": origin_id})
        resp = requests.delete(reqLink, data=d, headers=self.test_cred)
        return resp

    def allOrigin(self):
        reqLink = self.root_link + "/origin"
        resp = requests.get(reqLink, headers=self.test_cred)
        return resp

    def addDist(self, dist_id, dist_ip):
        reqLink = self.root_link + "/dist"
        d = json.dumps({"dist_id": dist_id, "dist_ip": dist_ip})
        resp = requests.post(reqLink, data=d, headers=self.test_cred)
        return resp

    def deleteDist(self, dist_id):
        reqLink = self.root_link + "/dist"
        d = json.dumps({"dist_id": dist_id})
        resp = requests.delete(reqLink, data=d, headers=self.test_cred)
        return resp

    def allDist(self):
        reqLink = self.root_link + "/dist"
        resp = requests.get(reqLink, headers=self.test_cred)
        return resp

    def addStream(self, stream_id, stream_ip):
        reqLink = self.root_link + "/streams"
        d = json.dumps({"stream_id": stream_id, "stream_ip": stream_ip})
        resp = requests.post(reqLink, data=d, headers=self.test_cred)
        return resp

    def deleteStream(self, stream_id):
        reqLink = self.root_link + "/streams"
        d = json.dumps({"stream_id": stream_id})
        resp = requests.delete(reqLink, data=d, headers=self.test_cred)
        return resp

    def allStreams(self):
        reqLink = self.root_link + "/streams"
        resp = requests.get(reqLink, headers=self.test_cred)
        return resp

    def reqStream(self, stream_id):
        reqLink = self.root_link + "/request"
        d = json.dumps({"stream_id": stream_id})
        resp = requests.post(reqLink, data=d, headers=self.test_cred)
        return resp


class VidTest(unittest.TestCase):
    ''' Test different scenarios '''

    def __init__(self, *args, **kwargs):
        super(VidTest, self).__init__(*args, **kwargs)
        self.HTTP_IP = os.environ["LB_IP"]
        self.HTTP_PORT = os.environ["HTTP_PORT"]
        self.ROOT_uname = os.environ["ROOT_UNAME"]
        self.ROOT_passwd = os.environ["ROOT_PASSWD"]
        self.origin_ip = os.environ["ORIGIN_IP"]
        self.origin_id = os.environ["ORIGIN_ID"]
        self.dist_ip = os.environ["DIST_IP"]
        self.dist_id = os.environ["DIST_ID"]
        streamsFile = os.environ["STREAMS"]
        self.streams = []

        if None in [self.HTTP_IP, self.HTTP_PORT,
                    self.ROOT_uname,
                    self.ROOT_passwd, streamsFile]:
            print("Environment not set")
            sys.exit(0)

        with open(streamsFile, "r") as f:
            self.streams = json.load(f)

        print("Inited system")
        self.vs = Vid(self.HTTP_IP, self.HTTP_PORT,
                      self.ROOT_uname, self.ROOT_passwd)

    def test_user(self):
        vec = [{"username": u, "password": u} for u in ["test1", "test2", "test3"]] 
        succ_create = 0.
        succ_get = 0.
        succ_delete = 0.
        num = 1
        for n in range(num):
            for v in vec:
                if self.vs.createUser(v) == 200:
                    succ_create += 1
            for v in vec:
                if self.vs.allUsers() == 3:
                    succ_get += 1
            for v in vec:
                if self.vs.deleteUser(v) == 204:
                    succ_delete += 1

    def test_simpleFlow(self):
        '''
            Forward Pass
        '''

        ''' Create user '''
        resp = self.vs.createUser()
        self.assertEqual(resp.status_code, 200)
        ''' Show all users (only 1) '''
        resp = self.vs.allUsers()
        self.assertEqual(len(resp.json()), 1)
        self.assertEqual(resp.json()[0],
                         {"username": self.vs.test_cred["username"]})
        self.assertEqual(resp.status_code, 200)
        ''' Add origin server '''
        resp = self.vs.addOrigin(self.origin_id, self.origin_ip)
        self.assertEqual(resp.status_code, 200)
        ''' Show all origin servers '''
        resp = self.vs.allOrigin()
        self.assertEqual(resp.json()[0]["origin_ip"], self.origin_ip)
        self.assertEqual(resp.json()[0]["origin_id"], self.origin_id)
        ''' Add a dist server '''
        resp = self.vs.addDist(self.dist_id, self.dist_ip)
        self.assertEqual(resp.status_code, 200)
        ''' Show all dists servers '''
        resp = self.vs.allDist()
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()[0]["dist_ip"], self.dist_ip)
        self.assertEqual(resp.json()[0]["dist_id"], self.dist_id)
        ''' Add streams '''
        for stream in self.streams:
            resp = self.vs.addStream(stream["stream_id"], stream["stream_ip"])
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json()["info"], "inserting")
        ''' Show all streams '''
        resp = self.vs.allStreams()
        stream_ids = [s["stream_id"] for s in self.streams]
        resp_streams = [s["stream_id"] for s in resp.json()]
        self.assertEqual(resp.status_code, 200)
        print("All streams: ", resp_streams)
        raw_input()
        for stream in resp_streams:
            self.assertTrue(stream in stream_ids)
        ''' Request for stream '''
        for stream in self.streams:
            resp = self.vs.reqStream(stream["stream_id"])
            print(resp.json())
            self.assertEqual(resp.status_code, 200)
            resp_json = resp.json()
            self.assertFalse(resp_json["info"] == "unavailable")

        raw_input()
        '''
            Reverse Pass
        '''

        ''' Delete all streams '''
        for stream in self.streams:
            print("Deleting ", stream["stream_id"])
            resp = self.vs.deleteStream(stream["stream_id"])
            self.assertEqual(resp.status_code, 200)
            resp_json = resp.json()
        raw_input()
        ''' Show all streams '''
        #resp = self.vs.allStreams()
        #stream_ids = [s["stream_id"] for s in self.streams]
        #resp_streams = [s["stream_id"] for s in resp.json()]
        #self.assertEqual(resp.status_code, 200)
        #for stream in resp_streams:
        #    self.assertFalse(stream in stream_ids)
        ''' Delete dist server '''
        #resp = self.vs.deleteDist(self.dist_id)
        #self.assertEqual(resp.status_code, 200)
        ''' Show all dists servers '''
        #resp = self.vs.allDist()
        #self.assertEqual(resp.status_code, 200)
        #self.assertEqual(resp.json(), [])
        ''' Delete origin server '''
        #resp = self.vs.deleteOrigin(self.origin_id)
        #print(resp.json())
        #raw_input()
        #self.assertEqual(resp.status_code, 200)
        ''' Show all origin servers '''
        #resp = self.vs.allOrigin()
        #self.assertEqual(resp.status_code, 200)
        #self.assertEqual(resp.json(), [])
        ''' Delete user '''
        resp = self.vs.deleteUser()
        self.assertEqual(resp.status_code, 200)
        ''' Show all users (0) '''
        resp = self.vs.allUsers()
        self.assertEqual(len(resp.json()), 0)
        self.assertEqual(resp.status_code, 200)

    def tearDown(self):
        ''' Cleanup '''
        self.vs.mongoclient.drop_database("vid-iot")


if __name__ == '__main__':
    unittest.main()
