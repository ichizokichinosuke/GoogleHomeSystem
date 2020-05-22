import json
import datetime
import os
import pandas as pd
import numpy as np
import paho.mqtt.client as mqtt

COLUMNS = ["Work_start", "Rest_start", "Rest_end", "Work_end", "Work_time"]
WORK_INSTRUCTION = ["勤務", "仕事", "ワーク",]
REST_INSTRUCTION = ["休憩", "中断", "一休み"]
START_INSTRUCTION = ["スタート", "開始", "始め"]
END_INSTRUCTION = ["エンド", "終了", "やめ"]
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
    work_or_rest, start_or_end = data["what"].split()
    print(work_or_rest, start_or_end)

    record_time(what=work_or_rest, how=start_or_end)

    time_df.to_csv(CSV_PATH)
    print(time_df.head())

def record_time(what="work", how="start"):
    dt = datetime.datetime.now()
    day = dt.date()
    time = dt.strftime("%X")
    # start_time = start_dt.time()
    if what in WORK_INSTRUCTION:
        if how in START_INSTRUCTION:
            print("Work Start!")
            time_df.at[day, "Work_start"] = time
        elif how in END_INSTRUCTION:
            print("Work End!")
            time_df.at[day, "Work_end"] = time

            work_start_time = time_df.at[day, "Work_start"]
            rest_start_time = time_df.at[day, "Rest_start"]
            rest_end_time = time_df.at[day, "Rest_end"]
            work_start_time = datetime.datetime.strptime(work_start_time, "%X")
            rest_start_time = datetime.datetime.strptime(rest_start_time, "%X")
            rest_end_time = datetime.datetime.strptime(rest_start_time, "%X")

            one_hour = datetime.time(hour=1)
            rest_time = rest_end_time - rest_start_time
            rest_time = one_hour if rest_time < one_hour else rest_time

            end_time = datetime.datetime.strptime(time, "%X")
            time_df.at[day, "Work_time"] = end_time - start_time - rest_time
        else:
            print(" Error! When record about work.")
    elif what in REST_INSTRUCTION:
        if how in START_INSTRUCTION:
            print("Rest Start!")
            time_df.at[day, "Rest_start"] = time
        elif how in END_INSTRUCTION:
            print("Rest End!")
            time_df.at[day, "Rest_end"] = time
        else:
            print(" Error! When record about rest.")

def load_table():
    if os.path.exists(CSV_PATH):
        time_df = pd.read_csv(CSV_PATH, index_col=0)

    else:
        ini_mat = np.empty((PERIODS, len(COLUMNS)))
        ini_mat[:,:] = np.nan
        date_index = pd.date_range(datetime.date.today(), periods=PERIODS, freq="D")
        time_df = pd.DataFrame(ini_mat, index=date_index, colusmn=COLUMNS)

    return time_df

time_df = load_table()
client = mqtt.Client()
client.username_pw_set("token:%s"%TOKEN)
client.on_connect = on_connect
client.on_message = on_message
client.tls_set(CACERT)
client.connect(HOSTNAME, port=PORT, keepalive=60)
client.loop_forever()
