from flask import Flask, request, jsonify
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
import pytz

app = Flask(__name__)

# MySQL connection configuration
db_config = {
    'host': 'mysql-container',
    'database': 'esp_data',
    'user': 'user',
    'password': 'password'
}

# Set timezone to Jakarta
local_tz = pytz.timezone('Asia/Jakarta')

# Define the capacity of a full water gallon (adjust as necessary)
CAPACITY = 19.0  # e.g. 19 liters

# --------------------------------------------------
# IMPORTANT: Create a new table in your MySQL database to store predictions.
#
# For example, you can run the following SQL statement:
#
# CREATE TABLE galon_prediction (
#     id INT AUTO_INCREMENT PRIMARY KEY,
#     galon VARCHAR(255) NOT NULL,
#     predicted_empty_time DATETIME,
#     consumption_rate FLOAT,
#     cumulative_consumption FLOAT,
#     remaining_volume FLOAT,
#     hours_to_empty FLOAT,
#     updated_at DATETIME,
#     UNIQUE KEY unique_galon (galon)
# );
#
# --------------------------------------------------

def insert_data(galon, value):
    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            cursor = connection.cursor()
            # Get current time in local timezone
            local_time = datetime.now(local_tz)
            query = "INSERT INTO galon_data (galon, value, timestamp) VALUES (%s, %s, %s)"
            cursor.execute(query, (galon, value, local_time))
            connection.commit()
            cursor.close()
    except Error as e:
        print(f"Error inserting data: {e}")
    finally:
        if connection.is_connected():
            connection.close()

def compute_prediction(galon):
    """
    Compute the water prediction for a given gallon using historical data.
    
    Returns a dictionary with prediction details if enough data is available,
    otherwise returns None.
    """
    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)
            query = "SELECT * FROM galon_data WHERE galon = %s ORDER BY timestamp ASC"
            cursor.execute(query, (galon,))
            records = cursor.fetchall()
            cursor.close()
        else:
            return None
    except Error as e:
        print("Error computing prediction: ", e)
        return None
    finally:
        if connection.is_connected():
            connection.close()

    if not records:
        return None  # No data available.

    # Filter out records with negative values.
    records_positive = [r for r in records if r['value'] > 0]
    if not records_positive:
        return None  # No valid positive data.
        
    # Get the current time in the local timezone
    current_time = datetime.now(local_tz)
    
    # Use only recent data (last 48 hours) for more accurate recent consumption rate
    recent_cutoff = current_time - timedelta(hours=48)
    recent_records = [r for r in records_positive if r['timestamp'] >= recent_cutoff]
    
    # If we don't have enough recent data, use all data
    if len(recent_records) < 2:
        recent_records = records_positive

    # Sort records by timestamp to ensure proper ordering
    recent_records.sort(key=lambda x: x['timestamp'])
    
    # Determine first and last timestamps from recent records
    first_time = recent_records[0]['timestamp']
    last_time = recent_records[-1]['timestamp']
    
    # Calculate elapsed time in hours
    time_diff_hours = (last_time - first_time).total_seconds() / 3600.0
    if time_diff_hours <= 0.1:  # Less than 6 minutes of data
        time_diff_hours = 0.1  # Prevent division by near-zero
    
    # Calculate recent consumption rate
    recent_consumption = sum(record['value'] for record in recent_records)
    consumption_rate = recent_consumption / time_diff_hours
    
    # Calculate total consumption from all positive records
    total_consumption = sum(record['value'] for record in records_positive)
    
    # Calculate remaining volume based on total consumption
    remaining_volume = max(0, CAPACITY - total_consumption)
    
    if consumption_rate <= 0.001:  # Very low consumption rate
        # Set a minimum consumption rate to avoid unrealistic predictions
        consumption_rate = 0.001
        hours_to_empty = remaining_volume / consumption_rate
        predicted_empty_time = current_time + timedelta(hours=hours_to_empty)
    elif remaining_volume <= 0:
        # The gallon is empty
        predicted_empty_time = last_time
        hours_to_empty = 0
    else:
        # Calculate hours until empty based on recent consumption rate
        hours_to_empty = remaining_volume / consumption_rate
        predicted_empty_time = current_time + timedelta(hours=hours_to_empty)

    prediction = {
        'galon': galon,
        'capacity': CAPACITY,
        'cumulative_consumption': total_consumption,
        'consumption_rate_per_hour': consumption_rate,
        'remaining_volume': remaining_volume,
        'hours_to_empty': hours_to_empty,
        'predicted_empty_time': predicted_empty_time.strftime("%Y-%m-%d %H:%M:%S"),
        'last_time': last_time.strftime("%Y-%m-%d %H:%M:%S"),
        'current_time': current_time.strftime("%Y-%m-%d %H:%M:%S")
    }
    return prediction

def update_prediction(galon):
    """
    Recalculate the prediction for the given gallon and store it in the
    `galon_prediction` table.
    """
    prediction = compute_prediction(galon)
    if prediction is None:
        # Not enough data to compute a valid prediction.
        return

    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            cursor = connection.cursor()
            # Check if a prediction record already exists for this gallon.
            select_query = "SELECT COUNT(*) FROM galon_prediction WHERE galon = %s"
            cursor.execute(select_query, (galon,))
            result = cursor.fetchone()
            record_exists = result[0] > 0

            if record_exists:
                update_query = """
                    UPDATE galon_prediction
                    SET predicted_empty_time = %s,
                        consumption_rate = %s,
                        cumulative_consumption = %s,
                        remaining_volume = %s,
                        hours_to_empty = %s,
                        updated_at = %s
                    WHERE galon = %s
                """
                cursor.execute(update_query, (
                    prediction['predicted_empty_time'],
                    prediction['consumption_rate_per_hour'],
                    prediction['cumulative_consumption'],
                    prediction['remaining_volume'],
                    prediction['hours_to_empty'],
                    prediction['last_time'],
                    galon
                ))
            else:
                insert_query = """
                    INSERT INTO galon_prediction
                    (galon, predicted_empty_time, consumption_rate, cumulative_consumption, remaining_volume, hours_to_empty, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_query, (
                    galon,
                    prediction['predicted_empty_time'],
                    prediction['consumption_rate_per_hour'],
                    prediction['cumulative_consumption'],
                    prediction['remaining_volume'],
                    prediction['hours_to_empty'],
                    prediction['last_time']
                ))
            connection.commit()
            cursor.close()
    except Error as e:
        print("Error updating prediction record: ", e)
    finally:
        if connection.is_connected():
            connection.close()

@app.route('/data', methods=['POST'])
def receive_data():
    """
    Endpoint for receiving water usage data from your ESP32.
    Expected JSON payload:
      {
         "galon": "galon_identifier",
         "value": <numeric water usage value>
      }
    After storing the new record, this endpoint also updates the water prediction.
    """
    try:
        data = request.json
        galon = data.get('galon')
        value = data.get('value')

        if galon is None or value is None:
            return jsonify({'status': 'error', 'message': 'Invalid data format'}), 400

        # Validate data types.
        if not isinstance(galon, str) or not isinstance(value, (int, float)):
            return jsonify({'status': 'error', 'message': 'Invalid data types'}), 400

        insert_data(galon, float(value))
        # After inserting the water usage record, update the prediction.
        update_prediction(galon)

        return jsonify({'status': 'success', 'message': 'Data received and stored successfully!'}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/predict/<galon>', methods=['GET'])
def predict(galon):
    """
    Endpoint to retrieve the current prediction for a specific water gallon.
    It computes (or retrieves) the prediction data and returns it as JSON.
    """
    prediction = compute_prediction(galon)
    if prediction is None:
        return jsonify({'status': 'error', 'message': 'Not enough data for prediction or error occurred'}), 400
    return jsonify({'status': 'success', 'prediction': prediction}), 200

@app.route('/predictions', methods=['GET'])
def get_predictions():
    """
    Endpoint to retrieve all water prediction records from the `galon_prediction` table.
    Grafana can query this endpoint (or query the table directly) to display water predictions.
    """
    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)
            query = "SELECT * FROM galon_prediction ORDER BY updated_at DESC"
            cursor.execute(query)
            records = cursor.fetchall()
            cursor.close()
            return jsonify({'status': 'success', 'predictions': records}), 200
        else:
            return jsonify({'status': 'error', 'message': 'Database connection error'}), 500
    except Error as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500
    finally:
        if connection.is_connected():
            connection.close()

@app.route('/', methods=['GET'])
def index():
    return jsonify({'status': 'success', 'message': 'Welcome to the Water Prediction API!'}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)