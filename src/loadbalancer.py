import os
import sys
import signal
from flask import Flask , request
import logging
import paho.mqtt.client as mqtt
import requests
import xmltodict as xd
norigin={}
ndists={}

def on_message(client, userdata, message):
	global norigin
    global ndists
	msg=str(message.payload.decode("utf-8")).split()
    topic=str(message.topic.decode("utf-8"))
    if topic=="lbs/origin":
        client.publish("db/selectedorigin",str(min(norigin.items(), key=lambda x: x[1])[0]))
    elif topic=="lbs/dist":
        client.publish("db/selectedorigin",str(min(ndists.items(), key=lambda x: x[1])[0]))
    elif topic=="origin/stat":
    	norigin[msg[0]]=msg[1]
    elif topic=="dist/stat":
        ndist[msg[0]]=msg[1]
    else:
        pass



if __name__=="__main__":
	broker_address="localhost"
	client = mqtt.Client("lbs") 
	client.connect(broker_address)
	client.on_message=on_message #connect to broker
	client.loop_start()
	client.subscribe([("lbs/origin",0),("lbs/dist",0),("origin/stat",0),("dist/stat",0)])
	client.loop_forever()