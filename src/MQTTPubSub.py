import sys
import os
import inspect
import paho.mqtt.client as mqtt
import threading


class MQTTPubSub:
    def __init__(self, params):

        self.url = params["url"]
        self.port = params["port"]
        self.timeout = params["timeout"]
        self.topic = params["topic"]
        self._mqttc = mqtt.Client(None)
        self.mqloop_running = 0
        
        if( "username" in params):
            self.username = params["username"]
            if( "password" in params):
                self.password = params["password"]
                self._mqttc.username_pw_set(self.username,self.password)

        if ("onMessage" in params):
            self._mqttc.on_message = params["onMessage"]
        if ("onConnect" in params):
            self._mqttc.on_connect = params["onConnect"]
        if ("onPublish" in params):
            self._mqttc.on_publish = params["onPublish"]
        if ("onSubscribe" in params):
            self._mqttc.on_subscribe = params["onSubscribe"]
        if ("onDisconnect" in params):
            self._mqttc.on_disconnect = params["onDisconnect"]


        self._mqttc.connect(self.url, self.port, self.timeout)
        a=[]
        if type(self.topic)==type(a):
            self._mqttc.subscribe(self.topic)
        else:
            self._mqttc.subscribe(self.topic, 0)


    def publish(self, topic, payload):
        if self.mqloop_running == 1:
            self._mqttc.publish(topic, payload)
        else:
            self._mqttc.loop_start()
            self._mqttc.publish(topic, payload)
            self._mqttc.loop_stop()

    def run(self):
        self.mqloop_running = 1
        self._mqttc.loop_forever()
        # threading loop_start() crashes program on error in callback
        # threading.Thread(target=self._mqttc.loop_start()).start()
