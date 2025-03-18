from flask import Flask, jsonify
import psycopg2
import paho.mqtt.client as mqtt
from waitress import serve
from datetime import datetime
import pytz

app = Flask(__name__)

IST = pytz.timezone("Asia/Kolkata")

# Database Connection Details
DB_CONFIG = {
    "dbname": "beehivedata",
    "user": "beehivedata_user",
    "password": "RIlW7IsGOIgGEEIJdIas4hkqfA3yIock",
    "host": "dpg-cv1cfsdds78s73dnfebg-a.oregon-postgres.render.com",
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
    if conn:
        timestamp = datetime.now(pytz.utc).astimezone(IST)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO HiveSensorData (userid, hivenumber, temperature1, humidity1, temperature2, humidity2, load, timestamp) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """, (userid, hivenumber, data["temperature1"], data["humidity1"], data["temperature2"], data["humidity2"], data["weight"], timestamp))
        conn.commit()
        cur.close()
        conn.close()
        #print(f"✅ Stored: {userid} | {hivenumber} | {data}")

# MQTT Callbacks
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Connected to MQTT Broker!")
        client.subscribe(SUBSCRIBE_TOPIC)
    else:
        print(f"⚠️ Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    data_dict = {}
    for item in payload.split(","):
        key, value = item.split(":")
        data_dict[key] = value
    _, userid, hivenumber, _ = msg.topic.split("/")
    insert_data(userid, hivenumber, data_dict)

# Setup MQTT Client
mqtt_client = mqtt.Client()
mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
mqtt_client.tls_set()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Start MQTT in a separate thread
def start_mqtt():
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT)
    mqtt_client.loop_start()

@app.route("/data", methods=["GET"])
def get_data():
    conn = connect_db()
    if conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM HiveSensorData ORDER BY timestamp DESC LIMIT 10;")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify(rows)
    return jsonify({"error": "Database connection failed"}), 500

if __name__ == "__main__":
    create_table()
    start_mqtt()
    serve(app, host="0.0.0.0", port=5000)
