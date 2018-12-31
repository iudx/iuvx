""" To check if the Addition and Deletion of Origin Servers is successfully being executed.
Inputs required : Origin Server ID, IP
Output: Test Results
Reponses by the HTTP Server are checked to analyze the test.
Addition of an Origin Server ensures the Origin Server is added successfully to the DB.
Deletion of an Origin Server ensures all streams being pulled/pushed by it will be terminated and collections in the DB will be updated.
"""

import requests
import sys
import pymongo
import time



if __name__=="__main__":
	mongoclient=pymongo.MongoClient('mongodb://<main_db_IP>:27017/')
	mydb=mongoclient["ALL_Streams"]
	col1=mydb["Origin_Servers"]
	AdditionFlag=0
	DeletionFlag=0
	r=requests.get("http://<main_Http_server_ip>:5000/origin?id="+sys.argv[1]+"&ip="+sys.argv[2]+"&opr=add")
	print r.content
	if "added" in r.content:
		for i in col1.find():
			if str(i["Origin_IP"])==sys.argv[2] and str(i["Origin_ID"])==sys.argv[1]:
				AdditionFlag=1
		if AdditionFlag:
			print "Test Passed for Origin Addition"
		else:
			print "Test Failed for Origin Addition"
			sys.exit()
	else:
		print "Test Failed for Origin Addition"
		sys.exit()
	time.sleep(1)
	r=requests.get("http://<main_http_server_ip>:5000/origin?id="+sys.argv[1]+"&ip="+sys.argv[2]+"&opr=delete")
	if "deleted" in r.content:
		time.sleep(1)
		for i in col1.find():
			if i["Origin_IP"]==sys.argv[2] and str(i["Origin_ID"])==sys.argv[1]:
				DeletionFlag=1
		if DeletionFlag:
			print "Test Failed for Origin Deletion"
			sys.exit()
		else:
			print "Test Passed for Origin Deletion"
	else:
		print "Test Failed for Origin Deletion"
	sys.exit()
