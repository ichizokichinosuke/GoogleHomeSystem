import json
import datetime
import os
import pandas as pd
import numpy as np
import paho.mqtt.client as mqtt

COLUMNS = ["Work_start", "Rest_start", "Rest_end", "Work_end"]
PERIODS = 50
CSV_PATH = "./work_table.csv"

TOKEN = "token_iwZlPmqHXaxtbY3q"
HOSTNAME = "mqtt.beebotte.com"
PORT = 8883
TOPIC = "LocalTest/voice"
CACERT = "mqtt.beebotte.com.pem"

def on_connect(client, userdata, flags, respons_code):
    print('status {0}'.format(respons_code))
    client.subscribe(TOPIC)

def on_message(client, userdata, msg):
    print(msg.topic + " " + str(msg.payload))
    data = json.loads(msg.payload.decode("utf-8"))["data"][0]
    data = {key:value.strip() for key, value in data.items()}
    print(data)
    if "スタート" == data["what"] or "開始" == data["what"]:
        print("Start working!")
        start_dt = datetime.datetime.now()
        start_day = start_dt.strftime("%Y-%m-%d")
        start_time = start_dt.strftime("%X")
        time_df.loc[start_day, "Work_start"] = start_time

    elif "終わり" == data["what"] or "終了" == data["what"]:
        print("End working!")
        end_dt = datetime.datetime.now()
        end_day = end_dt.strftime("%Y-%m-%d")
        end_time = end_dt.strftime("%X")
        time_df.loc[end_day, "Work_end"] = end_time

    time_df.to_csv(CSV_PATH)
    print(time_df.head())



if os.path.exists(CSV_PATH):
    time_df = pd.read_csv(CSV_PATH, index_col=0)

else:
    ini_mat = np.empty((PERIODS, 4))
    ini_mat[:,:] = np.nan
    date_index = pd.date_range("2020-5-15", periods=PERIODS, freq="D")
    time_df = pd.DataFrame(ini_mat, index=date_index, columns=COLUMNS)

client = mqtt.Client()
client.username_pw_set("token:%s"%TOKEN)
client.on_connect = on_connect
client.on_message = on_message
client.tls_set(CACERT)
client.connect(HOSTNAME, port=PORT, keepalive=60)
client.loop_forever()
