import requests
import xmltodict as xd
import paho.mqtt.client as mqtt
import socket

if __name__=="__main__":
	broker_address="10.156.14.141"
	client = mqtt.Client("diststat")
	client.connect(broker_address)	#connect to broker
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	s.connect(("8.8.8.8", 80))
	while(True):
		req=requests.get("http://localhost:8080/stat")
		stat=xd.parse(req.content)
		client.publish("dist/stat",str(s.getsockname()[0])+" "+str(stat["rtmp"]["server"]["application"][1]["live"]["nclients"]))
	s.close()
