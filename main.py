from flask import Flask, request, jsonify
import psycopg2
from datetime import datetime, timedelta
import pytz
from flask_cors import CORS
from waitress import serve

app = Flask(__name__)
CORS(app)

# Database Connection Details
DB_CONFIG = {
    "dbname": "beehivedata_ongn",
    "user": "rajesh",
    "password": "RfhhVrH6F0kvP3ztFmGum83NKCzWliOu",
    "host": "dpg-d048dks9c44c739cikpg-a.oregon-postgres.render.com",
    "port": "5432"
}

# Mapping time ranges to SQL intervals
TIME_RANGE_MAPPING = {
    "10min": "10 minutes",
    "1hour": "1 hour",
    "1day": "1 day",
    "5days": "5 days",
    "10months": "10 months"
}

@app.route('/', methods=['GET'])
def home():
    return "This is home page"

@app.route('/get_sensor_data', methods=['POST'])
def get_sensor_data():
    """Fetch sensor data for a given user, hive, and time range."""
    try:
        # Parse JSON payload from request
        payload = request.json
        userid = payload.get("userid")
        hivenumber = payload.get("hivenumber")
        number_of_records = payload.get("number_of_records")  
      
        if not userid or not hivenumber or not number_of_records:
            return jsonify({"error": "Invalid request parameters"}), 400

        # Connect to the database
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Safe query using parameterized statements
        query = """
            SELECT 
                temperature1, 
                humidity1, 
                load,  
                CAST(timestamp AS timestamptz) AT TIME ZONE 'Asia/Kolkata' AS ist_timestamp
            FROM HiveSensorData
            WHERE userid::TEXT = %s 
            AND hivenumber::TEXT = %s 
            ORDER BY timestamp DESC
            LIMIT %s;
        """

        cur.execute(query, (str(userid), str(hivenumber), number_of_records))
        results = cur.fetchall()
        temperature = []
        humidity = []
        load = []
        timestamps = []

        for result in results:
            temperature.append(result[0])
            humidity.append(result[1])
            load.append(result[2])
            timestamps.append(result[4])

        # Close the database connection
        cur.close()
        conn.close()

        response = {
            "hivenumber": hivenumber,
            "userid": userid,
            "number_of_records": number_of_records,
            "temperature": temperature[::-1],
            "humidity":humidity[::-1],
            "load": load[::-1],
            "timestamps": timestamps[::-1]
        }

        return jsonify(response)

    except Exception as e:
        print(f"⚠️ Error fetching data: {e}")
        return jsonify({"error": str(e)}), 500
if __name__ == '__main__':
    print("🚀 Running production server on port 5000...")
    serve(app, host="0.0.0.0", port=5000)
