
import os
import sys
import signal
from flask import Flask , request
import logging
import paho.mqtt.client as mqtt


log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

app = Flask(__name__)



@app.route('/reqstream')
def reqstream():
	stream_id  = request.args.get('id', None)
	user_ip=request.remote_addr
	client.publish("stream/request", user_ip+" "+stream_id)


@app.route('/streams')
def stream():
   	stream_ip  = request.args.get('ip', None)
	stream_id  = request.args.get('id', None)
	stream_op=request.args.get('opr', None)
	if stream_op=="add":
		client.publish("stream/add",str(stream_id)+" "+str(stream_ip))
		return "Stream added "+ stream_id +" "+ stream_ip
	elif origin_op=="delete":
		client.publish("stream/delete",str(stream_id)+" "+str(stream_ip))
		return "Stream deleted "+ stream_id +" "+ stream_ip
	else:
		return "Incorrect request"

if __name__ == '__main__':
   app.run(host="0.0.0.0",debug = True)
   a=raw_input()
   if a=="exit":
   		sys.exit()
   elif a=="q":
   		for i in self.col2.find():
   			os.kill(i["PROC_ID"],signal.SIGTERM)
   		time.sleep(30)
   		sys.exit()