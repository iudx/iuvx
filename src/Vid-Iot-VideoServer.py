import pymongo
import subprocess as sp
import os
import signal
import sys

if __name__=="__main__":
	print "Dropping Existing Database....."
	mongoclient=pymongo.MongoClient('mongodb://localhost:27017/')
	mongoclient.drop_database("ALL_Streams")
	print "Starting HTTP Server........."
	p1=sp.Popen(["python","HTTPserver.py"],shell=False)
	print "Starting LoadBalancer..........."
	p2=sp.Popen(["python","loadbalancer.py"],shell=False)
	print "Starting Logger........"
	p3=sp.Popen(["python","logger.py"],shell=False)
	print "Video Server Setup Complete...."
	if raw_input()=="q":
		os.kill(p1.pid,signal.SIGKILL)
		os.kill(p2.pid,signal.SIGKILL)
		os.kill(p3.pid,signal.SIGKILL)
		print "VS Terminated"
		sys.exit()
	