
import os
import sys
import signal
from flask import Flask , request
import logging
import paho.mqtt.client as mqtt

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)
app = Flask(__name__)

@app.route('/origin')
def origin():
	origin_ip  = request.args.get('ip', None)
	origin_id  = request.args.get('id', None)
	origin_op=request.args.get('opr', None)
	if origin_op=="add":
		client.publish("origin/add",str(origin_id)+" "+str(origin_ip))
		return "ORIGIN server added "+ origin_id +" "+ origin_ip
	elif origin_op=="delete":
		client.publish("origin/delete",str(origin_id)+" "+str(origin_ip))
		return "ORIGIN server deleted "+ origin_id +" "+ origin_ip
	else:
		return "Incorrect request"

@app.route('/dist')
def dist():
	dist_ip  = request.args.get('ip', None)
	dist_id  = request.args.get('id', None)
	dist_op=request.args.get('opr', None)
	if dist_op=="add":
		client.publish("dist/add",str(dist_id)+" "+str(dist_ip))
		return "DIST server added "+ dist_id +" "+ dist_ip
	elif dist_op=="delete":
		client.publish("dist/delete",str(dist_id)+" "+str(dist_ip))
		return "DIST server deleted "+ dist_id +" "+ dist_ip
	else:
		return "Incorrect request"


if __name__=="__main__":
	broker_address="localhost"
	client = mqtt.Client("P1") 
	client.connect(broker_address)
	app.run(host="0.0.0.0",debug = True)
	a=raw_input()
	if a=="exit":
		sys.exit()
	elif a=="q":
		for i in self.col2.find():
				os.kill(i["PROC_ID"],signal.SIGTERM)
				time.sleep(30)
				sys.exit()