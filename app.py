import psycopg2
import paho.mqtt.client as mqtt
import json

from datetime import datetime
import pytz

IST = pytz.timezone("Asia/Kolkata")

# Database Connection Details
DB_CONFIG = {
    "dbname": "beehivedata_ongn",
    "user": "rajesh",
    "password": "RfhhVrH6F0kvP3ztFmGum83NKCzWliOu",
    "host": "dpg-d048dks9c44c739cikpg-a.oregon-postgres.render.com",
    "port": "5432"
}

# MQTT Configuration

MQTT_BROKER = "fa462ba4ab664fe6a8a9c40b953e889f.s1.eu.hivemq.cloud"
MQTT_PORT = 8883
MQTT_USERNAME = "Useitforall123"
MQTT_PASSWORD = "Useitforall123"
SUBSCRIBE_TOPIC = "hive/#"

# Connect to PostgreSQL
def connect_db():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"⚠️ Database Connection Error: {e}")
        return None

# Create Table if Not Exists
def create_table():
    conn = connect_db()
    if conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS HiveSensorData (
            id SERIAL PRIMARY KEY,
            userid VARCHAR(10) NOT NULL,
            hivenumber VARCHAR(10) NOT NULL,
            temperature1 VARCHAR(20) NOT NULL,
            humidity1 VARCHAR(20) NOT NULL,
            temperature2 VARCHAR(20) NOT NULL,
            humidity2 VARCHAR(20) NOT NULL,
            load VARCHAR(20) NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Table HiveSensorData is ready!")

# Insert Data into Database
def insert_data(userid, hivenumber, data):
    conn = connect_db()
    temperature1 = data["temperature1"]
    humidity1 = data["humidity1"]
    temperature2 = data["temperature2"]
    humidity2 = data["humidity2"]
    load = data["weight"]
    print(data)
    
    if conn:
        
            timestamp = datetime.now(pytz.utc).astimezone(IST)
            print(f"{userid}, {hivenumber}, {temperature1}, {humidity1}, {temperature1}, {humidity1}, {load}, {timestamp}")
            
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO HiveSensorData (userid, hivenumber, temperature1, humidity1, temperature2, humidity2, load, timestamp) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """, (userid, hivenumber, temperature1, humidity1, temperature2, humidity2, load, timestamp))
            conn.commit()
            cur.close()
            conn.close()
            print(f"✅ Stored: {userid} | {hivenumber} | {temperature1} | {humidity1} | {temperature2} | {humidity2} | {load} | {timestamp}")
        # except Exception as e:
        #     print(f"⚠️ Error inserting data: {e}")

# MQTT Callbacks
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Connected to MQTT Broker!")
        client.subscribe(SUBSCRIBE_TOPIC)
    else:
        print(f"⚠️ Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
    """ Process MQTT message and store it in the database """
  
    payload = msg.payload.decode()
    print(payload)
    dict = {}

    datas = payload.split(",")
    for data in datas:
        values = data.split(":")
        dict[values[0]] = values[1]
        
        # Extract User ID, Hive Number, and Sensor Type
    _, userid, hivenumber, data = msg.topic.split("/")

        # Store in database
    insert_data(userid, hivenumber, dict)

    # except Exception as e:
    #     print(f"⚠️ Error processing message: {e}")

# Setup MQTT Client
mqtt_client = mqtt.Client()
mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
mqtt_client.tls_set()  # Secure connection

mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Create table before starting MQTT
create_table()

# Connect to MQTT and start listening
mqtt_client.connect(MQTT_BROKER, MQTT_PORT)
mqtt_client.loop_forever()  # Keep running
