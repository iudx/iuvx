import requests
import sys
import pymongo
import time



if __name__=="__main__":
	mongoclient=pymongo.MongoClient('mongodb://localhost:27017/')
	mydb=mongoclient["ALL_Streams"]
	col1=mydb["Streams"]
	AdditionFlag=0
	DeletionFlag=0
	r=requests.get("http://localhost:5000/streams?id="+sys.argv[1]+"&ip="+sys.argv[2]+"&opr=add")
	if "added" in r.content:
		for i in col1.find():
			if str(i["Stream_IP"])==sys.argv[2] and str(i["Stream_ID"])==sys.argv[1]:
				AdditionFlag=1
		if AdditionFlag:
			print "Test Passed for Stream Addition"
		else:
			print "Test Failed for Stream Addition"
			sys.exit()
	else:
		print "Test Failed for Stream Addition"
		sys.exit()
	time.sleep(10)
	r=requests.get("http://localhost:5000/streams?id="+sys.argv[1]+"&ip="+sys.argv[2]+"&opr=delete")
	if "deleted" in r.content:
		time.sleep(1)
		for i in col1.find():
			if i["Stream_IP"]==sys.argv[2] and str(i["Stream_ID"])==sys.argv[1]:
				DeletionFlag=1
		if DeletionFlag:
			print "Test Failed for Stream Deletion"
			sys.exit()
		else:
			print "Test Passed for Stream Deletion"
	else:
		print "Test Failed for Stream Deletion"
	sys.exit()