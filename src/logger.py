from MQTTPubSub import MQTTPubSub
import os
import sys
import time


def on_message(client, userdata, message):
    print(message.topic)


mqtt_ip = os.environ["LB_IP"]
mqtt_port = os.environ["MQTT_PORT"]
if mqtt_ip is None or mqtt_port is None:
    print("Error! LB_IP and LB_PORT not set")
    sys.exit(0)
origin_id = os.environ["ORIGIN_ID"]

mqttServerParams = {}
mqttServerParams["url"] = mqtt_ip
mqttServerParams["port"] = int(mqtt_port)
mqttServerParams["timeout"] = 60
mqttServerParams["topic"] = "+"
mqttServerParams["onMessage"] = on_message
client = MQTTPubSub(mqttServerParams)


if __name__ == "__main__":
    client.run()
    while(True):
        time.sleep(0.01)
        pass
