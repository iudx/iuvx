# Log all Actions

import paho.mqtt.client as mqtt
from MQTTPubSub import MQTTPubSub

def on_message(client, userdata, message):
	with open("logs.txt","a+") as f:
		f.write(str(message.payload.decode("utf-8"))+"\n")

mqttServerParams = {}
mqttServerParams["url"] = "10.156.14.141"
mqttServerParams["port"] = 1883
mqttServerParams["timeout"] = 60
mqttServerParams["topic"] = "logger"
mqttServerParams["onMessage"] = on_message
client = MQTTPubSub(mqttServerParams)


if __name__=="__main__":
	client.run()
	while(True):
		pass
