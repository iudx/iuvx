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
    def __init__(self, LB_IP, LB_Port, ROOT_uname, ROOT_passwd):
        self.LB_IP = LB_IP
        self.LB_Port = LB_Port
        self.ROOT_uname = ROOT_uname
        self.ROOT_passwd = ROOT_passwd
        self.test_cred = {"username": self.ROOT_uname,
                          "password": self.ROOT_passwd,
                          "accept": "application/json",
                          "Content-Type": "application/json"}
        self.root_link = "http://" + LB_IP + ":" + LB_Port
        uname = os.environ["MONGO_INITDB_ROOT_USERNAME"]
        pwd = os.environ["MONGO_INITDB_ROOT_PASSWORD"]
        url = os.environ["MONGO_URL"]
        mongoclient = pymongo.MongoClient("mongodb://" + uname + ":" + pwd +
                                          "@" + url + ":27017/", connect=False)
        mongoclient.drop_database("vid-iot")

    def createUser(self, user_cred=None):
        ''' Root functions '''
        reqLink = self.root_link + "/user"
        if(user_cred is not None):
            resp = requests.post(reqLink, data=json.dumps(user_cred))
        else:
            resp = requests.post(reqLink, data=json.dumps(self.test_cred))
        print(resp.status_code)
        return resp.status_code

    def deleteUser(self, user_cred=None):
        reqLink = self.root_link + "/user"
        if(user_cred is not None):
            resp = requests.delete(reqLink, data=json.dumps(user_cred))
        else:
            resp = requests.delete(reqLink, data=json.dumps(self.test_cred))
        print(resp.status_code)
        return resp.status_code

    def allUsers(self):
        reqLink = self.root_link + "/user"
        resp = requests.get(reqLink)
        print(resp.json())
        return len(resp.json())

    def addOrigin(self, origin_id, origin_ip):
        reqLink = self.root_link + "/origin"
        d = json.dumps({"origin_id": origin_id, "origin_ip": origin_ip})
        resp = requests.post(reqLink, data=d, headers=self.test_cred)
        return resp.status_code

    def deleteOrigin(self, origin_id, origin_ip):
        reqLink = self.root_link + "/origin"
        d = json.dumps({"origin_id": origin_id})
        resp = requests.delete(reqLink, data=d, headers=self.test_cred)
        return resp.status_code

    def allOrigin(self):
        reqLink = self.root_link + "/origin"
        resp = requests.get(reqLink, headers=self.test_cred)
        print(resp.json())
        return len(resp.json())

    def addDist(self, dist_id, dist_ip):
        reqLink = self.root_link + "/dist"
        d = json.dumps({"dist_id": dist_id, "dist_ip": dist_ip})
        resp = requests.post(reqLink, data=d, headers=self.test_cred)
        return resp.status_code

    def deleteDist(self, dist_id, dist_ip):
        reqLink = self.root_link + "/dist"
        d = json.dumps({"dist_id": dist_id})
        resp = requests.delete(reqLink, data=d, headers=self.test_cred)
        return resp.status_code

    def allDist(self):
        reqLink = self.root_link + "/dist"
        resp = requests.get(reqLink, headers=self.test_cred)
        print(resp.json())
        return resp.status_code

    def addStream(self, stream_id, stream_ip):
        reqLink = self.root_link + "/streams"
        d = json.dumps({"stream_id": stream_id, "stream_ip": stream_ip})
        resp = requests.post(reqLink, data=d, headers=self.test_cred)
        print(resp.json())
        return resp.status_code

    def deleteStream(self, stream_id, stream_ip):
        reqLink = self.root_link + "/streams"
        d = json.dumps({"stream_id": stream_id, "stream_ip": stream_ip})
        resp = requests.delete(reqLink, data=d, headers=self.test_cred)
        return resp.status_code

    def allStreams(self):
        reqLink = self.root_link + "/streams"
        resp = requests.get(reqLink, headers=self.test_cred)
        print(resp.json())
        return resp.status_code

    def reqStream(self, stream_id):
        reqLink = self.root_link + "/request"
        d = json.dumps({"stream_id": stream_id})
        resp = requests.post(reqLink, data=d, headers=self.test_cred)
        return resp.status_code


class VidTest(unittest.TestCase):
    ''' Test different scenarios '''

    def __init__(self, *args, **kwargs):
        super(VidTest, self).__init__(*args, **kwargs)
        self.LB_IP = os.environ["LB_IP"]
        self.LB_Port = os.environ["LB_PORT"]
        self.ROOT_uname = os.environ["ROOT_UNAME"]
        self.ROOT_passwd = os.environ["ROOT_PASSWD"]
        self.origin_ip = os.environ["ORIGIN_IP"]
        self.origin_id = os.environ["ORIGIN_ID"]
        self.dist_ip = os.environ["DIST_IP"]
        self.dist_id = os.environ["DIST_ID"]
        streamsFile = os.environ["STREAMS"]
        self.streams = []

        if None in [self.LB_IP, self.LB_Port,
                    self.ROOT_uname,
                    self.ROOT_passwd, streamsFile]:
            print("Environment not set")
            sys.exit(0)

        with open(streamsFile, "r") as f:
            self.streams = json.load(f)

        print("Inited system")
        self.vs = Vid(self.LB_IP, self.LB_Port,
                      self.ROOT_uname, self.ROOT_passwd)

    def test_user(self):
        print("Testing User creation functions ")
        vec = [{"username": u, "password": u} for u in ["test1", "test2", "test3"] ] 
        succ_create = 0.
        succ_get = 0.
        succ_delete = 0.
        num = 100
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
        print("Success of creation", succ_create/num)
        print("Success of Get", succ_get/num)
        print("Success of delete", succ_delete/num)

    def test_simpleFlow(self):
        print("Creating user")
        self.vs.createUser()
        print("User Created")
        print("Showing all Users")
        self.vs.allUsers()
        print("Adding Origin")
        self.vs.addOrigin(self.origin_id, self.origin_ip)
        print("Showing all origins")
        self.vs.allOrigin()
        print("Adding dist")
        self.vs.addDist(self.dist_id, self.dist_ip)
        print("Showing all dists")
        self.vs.allDist()
        for stream in self.streams:
            print("Adding stream", stream["id"])
            self.vs.addStream(stream["id"], stream["ip"])
        self.vs.allStreams()
        raw_input()
        time.sleep(1)
        for stream in self.streams:
            print("Requesting stream", stream)
            self.vs.reqStream(stream["id"])
            raw_input()


if __name__ == '__main__':
    unittest.main()
