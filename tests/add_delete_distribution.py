""" To check if the Addition and Deletion of Distribution Servers is successfully being executed.
Inputs required : Distribution Server ID, IP
Output: Test Results
Reponses by the HTTP Server are checked to analyze the test.
Addition of an Distribution Server ensures the Distribution Server is added successfully to the DB.
Deletion of an Distribution Server ensures all streams being pulled/pushed by it will be terminated and collections in the DB will be updated.
"""

import requests
import sys
import pymongo
import time



if __name__=="__main__":
	mongoclient=pymongo.MongoClient('mongodb://localhost:27017/')
	mydb=mongoclient["ALL_Streams"]
	col1=mydb["Distribution_Servers"]
	AdditionFlag=0
	DeletionFlag=0
	r=requests.get("http://localhost:5000/dist?id="+sys.argv[1]+"&ip="+sys.argv[2]+"&opr=add")
	if "added" in r.content:
		for i in col1.find():
			if str(i["Dist_IP"])==sys.argv[2] and str(i["Dist_ID"])==sys.argv[1]:
				AdditionFlag=1
		if AdditionFlag:
			print "Test Passed for Distribution Addition"
		else:
			print "Test Failed for Distribution Addition"
			sys.exit()
	else:
		print "Test Failed for Distribution Addition"
		sys.exit()
	time.sleep(10)
	r=requests.get("http://localhost:5000/dist?id="+sys.argv[1]+"&ip="+sys.argv[2]+"&opr=delete")
	if "deleted" in r.content:
		time.sleep(1)
		for i in col1.find():
			if i["Dist_IP"]==sys.argv[2] and str(i["Dist_ID"])==sys.argv[1]:
				DeletionFlag=1
		if DeletionFlag:
			print "Test Failed for Distribution Deletion"
			sys.exit()
		else:
			print "Test Passed for Distribution Deletion"
	else:
		print "Test Failed for Distribution Deletion"
	sys.exit()

