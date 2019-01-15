import paho.mqtt.client as mqtt
import os
import sys
import signal
import time
import requests
import subprocess as sp
from MQTTPubSub import MQTTPubSub
import psutil



#MQTT Params
mqttServerParams = {}
mqttServerParams["url"] = "10.156.14.141"
mqttServerParams["port"] = 1883
mqttServerParams["timeout"] = 60
mqttServerParams["topic"] = [("origin/ffmpeg/stream/archive",0)]
mqttServerParams["onMessage"] = on_message
client = MQTTPubSub(mqttServerParams)


if __name__=="__main__":
	







	
		

	