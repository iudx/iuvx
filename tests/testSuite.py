'''
    This script creates a test user and display
'''
import requests
import os
import sys
import json
import unittest
import pymongo
import time


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
        mongoclient = pymongo.MongoClient('mongodb://localhost:27017/')
        mongoclient.drop_database("ALL_Streams")

    def createUser(self):
        ''' Root functions '''
        reqLink = self.root_link + "/user"
        resp = requests.post(reqLink, data=json.dumps(self.test_cred))
        print(resp)
        print(resp.text)
        print(resp.json())

    def deleteUser(self):
        reqLink = self.root_link + "/user"
        resp = requests.delete(reqLink, data=json.dumps(self.test_cred))
        print(resp.json())

    def allUsers(self):
        reqLink = self.root_link + "/user"
        resp = requests.get(reqLink)
        print(resp.json())

    def addOrigin(self, origin_id, origin_ip):
        print("Adding Origin")
        reqLink = self.root_link + "/origin"
        d = json.dumps({"id": origin_id, "ip": origin_ip})
        resp = requests.post(reqLink, data=d, headers=self.test_cred)
        print(resp.text)

    def deleteOrigin(self, origin_id, origin_ip):
        print("Deleting Origin")
        reqLink = self.root_link + "/origin"
        d = json.dumps({"id": origin_id, "ip": origin_ip})
        resp = requests.delete(reqLink, data=d, headers=self.test_cred)
        print(resp.json())

    def allOrigin(self):
        print("Showing all orgin server")
        reqLink = self.root_link + "/origin"
        resp = requests.get(reqLink, headers=self.test_cred)
        print(resp.json())

    def addDist(self, dist_id, dist_ip):
        print("Adding Distribution")
        reqLink = self.root_link + "/dist"
        d = json.dumps({"id": dist_id, "ip": dist_ip})
        resp = requests.post(reqLink, data=d, headers=self.test_cred)
        print(resp.json())

    def deleteDist(self, dist_id, dist_ip):
        print("Deleting Distribution")
        reqLink = self.root_link + "/dist"
        d = json.dumps({"id": dist_id, "ip": dist_ip})
        resp = requests.delete(reqLink, data=d, headers=self.test_cred)
        print(resp.json())

    def allDist(self):
        print("Showing all distribution")
        reqLink = self.root_link + "/dist"
        resp = requests.get(reqLink, headers=self.test_cred)
        print(resp.json())

    def addStream(self, stream_id, stream_ip):
        print("Adding stream")
        reqLink = self.root_link + "/streams"
        d = json.dumps({"id": stream_id, "ip": stream_ip})
        print(d)
        resp = requests.post(reqLink, data=d, headers=self.test_cred)
        print(resp.json())

    def deleteStream(self, stream_id, stream_ip):
        print("Deleting stream")
        reqLink = self.root_link + "/streams"
        d = json.dumps({"id": stream_id, "ip": stream_ip})
        resp = requests.delete(reqLink, data=d, headers=self.test_cred)
        print(resp.json())

    def allStreams(self):
        print("Showing all streams")
        reqLink = self.root_link + "/streams"
        resp = requests.get(reqLink, headers=self.test_cred)
        print(resp.json())

    def reqStream(self, stream_id):
        print("Requesting stream")
        reqLink = self.root_link + "/request"
        d = json.dumps({"id": stream_id})
        resp = requests.post(reqLink, data=d, headers=self.test_cred)
        print(resp.json())


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


    def test_createUser(self):
        self.vs.createUser()

    def test_simpleFlow(self):
        print("Creating user")
        self.vs.createUser()
        self.vs.allUsers()
        self.vs.addOrigin(self.origin_id, self.origin_ip)
        self.vs.allOrigin()
        self.vs.addDist(self.dist_id, self.dist_ip)
        self.vs.allDist()
        for stream in self.streams:
            print("Adding stream", stream["id"])
            self.vs.addStream(stream["id"], stream["ip"])
        self.vs.allStreams()
        time.sleep(10)
        for stream in self.streams:
            print("Requesting stream", stream)
            self.vs.reqStream(stream["id"])


if __name__ == '__main__':
    unittest.main()
