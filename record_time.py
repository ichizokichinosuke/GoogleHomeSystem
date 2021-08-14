import json
import datetime
import os
import pandas as pd
import numpy as np
import paho.mqtt.client as mqtt
import ssl
import numpy as np

context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
# TLS_v = ssl.PROTOCOL_TLSv1_2

COLUMNS = ["Work_start", "Rest_start", "Rest_end", "Work_end", "Work_time"]
WORK_INSTRUCTION = ["勤務", "仕事", "ワーク",]
REST_INSTRUCTION = ["休憩", "中断", "離席",]
START_INSTRUCTION = ["スタート", "開始", "始め"]
END_INSTRUCTION = ["エンド", "終了", "やめ"]
PERIODS = 50
REST_HOUR = 1
CSV_PATH = "./work_table.csv"

TOKEN = "token_iwZlPmqHXaxtbY3q"
HOSTNAME = "mqtt.beebotte.com"
PORT = 8883
TOPIC = "LocalTest/voice"
CACERT = "mqtt.beebotte.com.pem"
SEC_PEM = '/usr/local/ssl/cert.pem'


def on_connect(client, userdata, flags, respons_code):
    print('status {0}'.format(respons_code))
    client.subscribe(TOPIC)

def on_message(client, userdata, msg):
    print(msg.topic + " " + str(msg.payload))
    data = json.loads(msg.payload.decode("utf-8"))["data"][0]
    data = {key:value.strip() for key, value in data.items()}
    print(data)
    work_or_rest, start_or_end = data["what"].split()

    record_time(what=work_or_rest, how=start_or_end)

    time_df.to_csv(CSV_PATH)
    print(time_df.head())

# 勤務時間を記録
def calc_work_hour(day, work_end_time):
    null_df = time_df.isnull()
    work_end_time = datetime.datetime.strptime(work_end_time, "%X")

    if not null_df.at[day, "Work_start"]:
        work_start_time = time_df.at[day, "Work_start"]
        work_start_time = datetime.datetime.strptime(work_start_time, "%X")
    else:
        return 0

    if not null_df.at[day, "Rest_start"] and not null_df.at[day, "Rest_end"]:
        rest_start_time = time_df.at[day, "Rest_start"]
        rest_end_time = time_df.at[day, "Rest_end"]
        rest_start_time = datetime.datetime.strptime(rest_start_time, "%X")
        rest_end_time = datetime.datetime.strptime(rest_end_time, "%X")

        time_df.at[day, "Work_time"] = work_end_time - work_start_time - (rest_end_time-rest_start_time)

    else:
        if str(work_end_time - work_start_time)[0] == "0":
            one_hour = datetime.timedelta(hours=0)
        else:
            one_hour = datetime.timedelta(hours=REST_HOUR)

        time_df.at[day, "Work_time"] = work_end_time - work_start_time - one_hour

# 指示のあった時間を記録
def record_time(what="work", how="start"):
    dt = datetime.datetime.now()
    day = dt.date()
    day = day.strftime("%Y-%m-%d")
    time = dt.strftime("%X")
    if what in WORK_INSTRUCTION:
        if how in START_INSTRUCTION:
            print("Work Start!")
            time_df.at[day, "Work_start"] = time
        elif how in END_INSTRUCTION:
            print("Work End!")
            time_df.at[day, "Work_end"] = time
            calc_work_hour(day, time)

        else:
            print(" Error! When record about work.")
    elif what in REST_INSTRUCTION:
        if how in START_INSTRUCTION:
            print("Rest Start!")
            print(day)
            time_df.at[day, "Rest_start"] = time
        elif how in END_INSTRUCTION:
            print("Rest End!")
            time_df.at[day, "Rest_end"] = time
        else:
            print(" Error! When record about rest.")

# csvファイルの読み込みもしくは作成
def load_table():
    if os.path.exists(CSV_PATH):
        print("Loading..")
        time_df = pd.read_csv(CSV_PATH, index_col=0, dtype=str)

    else:
        ini_mat = np.empty((PERIODS, len(COLUMNS)))
        ini_mat[:,:] = np.nan
        date_index = pd.date_range(datetime.date.today(), periods=PERIODS, freq="D")
        time_df = pd.DataFrame(ini_mat, index=date_index, columns=COLUMNS)

    return time_df

time_df = load_table()
client = mqtt.Client()
client.tls_set_context(context=context)

client.username_pw_set("token:%s"%TOKEN)
client.on_connect = on_connect
client.on_message = on_message
# client.tls_set(CACERT, keyfile=SEC_PEM, tls_version=TLS_v)
# client.tls_insecure_set(True)

client.connect(HOSTNAME, port=PORT, keepalive=60)
client.loop_forever()
