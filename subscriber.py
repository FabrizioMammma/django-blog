import paho.mqtt.client as mqtt
import sqlite3
import datetime
import json

MQTT_HOST = '192.168.43.87'
MQTT_PORT = 1883
#MQTT_CLIENT_ID = 'Python MQTT client'
#MQTT_USER = 'YOUR MQTT USER'
#MQTT_PASSWORD = 'YOUR MQTT USER PASSWORD'
TOPIC = 'powermeter/channel'

DATABASE_FILE = 'mqtt-db'


def on_connect(mqtt_client, user_data, flags, conn_result):
    mqtt_client.subscribe(TOPIC)
    print("Connected")


def on_message(mqtt_client, user_data, message):
    payload = message.payload
    payloadModified = json.loads(payload)
    dataR = payloadModified['f'][0]
    dataS = payloadModified['f'][1]
    dataT = payloadModified['f'][2]
    dataRModified = json.dumps(dataR)
    dataSModified = json.dumps(dataS)
    dataTModified = json.dumps(dataT)
    DATE = datetime.datetime.now()

    db_conn = user_data['db_conn']
    sql = 'INSERT INTO sensors_data (topic, phaseR, phaseS, phaseT, received) VALUES (?, ?, ?, ?, ?)'
    cursor = db_conn.cursor()
    cursor.execute(sql, (message.topic, dataRModified, dataSModified, dataTModified, DATE))
    db_conn.commit()
    cursor.close()


def main():
    db_conn = sqlite3.connect(DATABASE_FILE)
    sql = """
    CREATE TABLE IF NOT EXISTS sensors_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        topic TEXT NOT NULL,
        phaseR TEXT NOT NULL,
        phaseS TEXT NOT NULL,
        phaseT TEXT NOT NULL,
        received TEXT NOT NULL
    )
    """
    cursor = db_conn.cursor()
    cursor.execute(sql)
    cursor.close()

    mqtt_client = mqtt.Client()
    #mqtt_client.username_pw_set()
    mqtt_client.user_data_set({'db_conn': db_conn})

    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(MQTT_HOST, MQTT_PORT)
    mqtt_client.loop_forever()


main()