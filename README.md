# GoogleHomeSystem
## 勤怠管理

### 使用技術
* IFTTT
* Beebotte
* MQTT

IFTTTとBeeBotteの設定を行い，このファイルを実行すると，勤怠表を作成する．
その後，各種指示を与えると，時間を記録する．
退勤すると勤務時間を記録する．

具体的には，MQTTをsubscribeしたときに呼ばれるコールバック関数，on_messageを書き換えることで実装している．
