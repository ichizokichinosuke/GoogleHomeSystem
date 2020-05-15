import paho.mqtt.client as mqtt
import json

TOKEN = "token_iwZlPmqHXaxtbY3q"
HOSTNAME = "mqtt.beebotte.com"
PORT = 8883
TOPIC = "LocalTest/voice"
CACERT = "mqtt.beebotte.com.pem"

def on_connect(client, userdata, flags, respons_code):
    # print('status {0}'.format(respons_code))
    client.subscribe(TOPIC)

def on_message(client, userdata, msg):
    # print(msg.topic + " " + str(msg.payload))
    data = json.loads(msg.payload.decode("utf-8"))["data"][0]
    data = {key:value.strip() for key, value in data.items()}
    if "room" in data.keys():
        print(data)

client = mqtt.Client()
client.username_pw_set("token:%s"%TOKEN)
client.on_connect = on_connect
client.on_message = on_message
client.tls_set(CACERT)
client.connect(HOSTNAME, port=PORT, keepalive=60)
client.loop_forever()
