# Log all Actions

import paho.mqtt.client as mqtt

def on_message(client, userdata, message):
	with open("logs.txt","a+") as f:
		f.write(str(message.payload.decode("utf-8"))+"\n")


if __name__=="__main__":
	broker_address="localhost"
	client = mqtt.Client("logger") 
	client.connect(broker_address)
	client.on_message=on_message #connect to broker
	client.loop_start()
	client.subscribe("logger")
	client.loop_forever()
