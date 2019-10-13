from MQTTPubSub import MQTTPubSub
import os
import sys


def on_message(client, userdata, message):
    with open("logs.txt", "a+") as f:
        f.write(str(message.payload.decode("utf-8"))+"\n")


mqtt_ip = os.environ["LB_IP"]
mqtt_port = os.environ["LB_PORT"]
if mqtt_ip is None or mqtt_port is None:
    print("Error! LB_IP and LB_PORT not set")
    sys.exit(0)
origin_id = os.environ["ORIGIN_ID"]

mqttServerParams = {}
mqttServerParams["url"] = mqtt_ip
mqttServerParams["port"] = mqtt_port
mqttServerParams["timeout"] = 60
mqttServerParams["topic"] = "+"
mqttServerParams["onMessage"] = on_message
client = MQTTPubSub(mqttServerParams)


if __name__ == "__main__":
    client.run()
    while(True):
        pass
