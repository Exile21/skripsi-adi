from flask import Flask, request, jsonify
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
import pytz
from contextlib import contextmanager

app = Flask(__name__)

# Configuration
DB_CONFIG = {
    'host': 'mysql-container',
    'database': 'esp_data',
    'user': 'user',
    'password': 'password'
}
LOCAL_TZ = pytz.timezone('Asia/Jakarta')
CAPACITY = 19.0  # Water gallon capacity in liters
MIN_CONSUMPTION_RATE = 0.001  # Minimum consumption rate to use for calculations
RECENT_DATA_HOURS = 48  # Hours of recent data to use for consumption rate calculation

# Database helper functions
@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    connection = None
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        yield connection
    except Error as e:
        print(f"Database connection error: {e}")
        raise
    finally:
        if connection and connection.is_connected():
            connection.close()

def execute_query(query, params=None, fetch=False, dictionary=False):
    """Execute a database query and optionally return results"""
    try:
        with get_db_connection() as connection:
            cursor = connection.cursor(dictionary=dictionary)
            cursor.execute(query, params or ())
            
            if fetch:
                result = cursor.fetchall()
            else:
                connection.commit()
                result = None
                
            cursor.close()
            return result
    except Error as e:
        print(f"Query execution error: {e}")
        raise

def insert_data(galon, value):
    """Insert water usage data into database"""
    current_time = datetime.now(LOCAL_TZ)
    query = "INSERT INTO galon_data (galon, value, timestamp) VALUES (%s, %s, %s)"
    execute_query(query, (galon, value, current_time))

def get_galon_data(galon):
    """Get all water data for a specific gallon"""
    query = "SELECT * FROM galon_data WHERE galon = %s ORDER BY timestamp ASC"
    return execute_query(query, (galon,), fetch=True, dictionary=True)

def create_empty_prediction(galon, status_message):
    """Create a default prediction when no data is available"""
    current_time = datetime.now(LOCAL_TZ)
    return {
        'galon': galon,
        'capacity': CAPACITY,
        'cumulative_consumption': 0.0,
        'consumption_rate_per_hour': 0.0,
        'remaining_volume': CAPACITY,
        'hours_to_empty': float('inf'),
        'predicted_empty_time': 'N/A',
        'last_time': 'N/A',
        'current_time': current_time.strftime("%Y-%m-%d %H:%M:%S"),
        'status': status_message
    }

def ensure_timezone_aware(dt):
    """Ensure a datetime object has timezone information"""
    if dt.tzinfo is None:
        return LOCAL_TZ.localize(dt)
    return dt

def compute_prediction(galon):
    """Compute water prediction for a given gallon"""
    try:
        records = get_galon_data(galon)
        
        if not records:
            return create_empty_prediction(galon, "No data available")

        # Filter and prepare the data
        records_positive = [r for r in records if r['value'] > 0]
        if not records_positive:
            return create_empty_prediction(galon, "No positive consumption data available")
        
        # Make timestamps timezone aware
        for record in records_positive:
            record['timestamp'] = ensure_timezone_aware(record['timestamp'])
        
        current_time = datetime.now(LOCAL_TZ)
        
        # Get recent records for consumption rate calculation
        recent_cutoff = current_time - timedelta(hours=RECENT_DATA_HOURS)
        recent_records = [r for r in records_positive if r['timestamp'] >= recent_cutoff]
        if len(recent_records) < 2:
            recent_records = records_positive
        
        # Sort and get time boundaries
        recent_records.sort(key=lambda x: x['timestamp'])
        first_time = recent_records[0]['timestamp']
        last_time = recent_records[-1]['timestamp']
        
        # Calculate consumption metrics
        time_diff_hours = max(0.1, (last_time - first_time).total_seconds() / 3600.0)
        recent_consumption = sum(record['value'] for record in recent_records)
        consumption_rate = max(MIN_CONSUMPTION_RATE, recent_consumption / time_diff_hours)
        total_consumption = sum(record['value'] for record in records_positive)
        remaining_volume = max(0, CAPACITY - total_consumption)
        
        # Calculate prediction
        if remaining_volume <= 0:
            hours_to_empty = 0
            predicted_empty_time = last_time
        else:
            hours_to_empty = remaining_volume / consumption_rate
            predicted_empty_time = current_time + timedelta(hours=hours_to_empty)
        
        return {
            'galon': galon,
            'capacity': CAPACITY,
            'cumulative_consumption': total_consumption,
            'consumption_rate_per_hour': consumption_rate,
            'remaining_volume': remaining_volume,
            'hours_to_empty': hours_to_empty,
            'predicted_empty_time': predicted_empty_time.strftime("%Y-%m-%d %H:%M:%S"),
            'last_time': last_time.strftime("%Y-%m-%d %H:%M:%S"),
            'current_time': current_time.strftime("%Y-%m-%d %H:%M:%S"),
            'status': 'Success'
        }
    except Exception as e:
        print(f"Error in prediction calculation: {e}")
        return None

def update_prediction(galon):
    """Update prediction records in the database"""
    prediction = compute_prediction(galon)
    
    if prediction is None or prediction.get('status') not in ['Success']:
        return
    
    current_time = datetime.now(LOCAL_TZ)
    
    try:
        # Check if record exists
        check_query = "SELECT COUNT(*) FROM galon_prediction WHERE galon = %s"
        count = execute_query(check_query, (galon,), fetch=True)
        record_exists = count[0][0] > 0
        
        # Prepare data
        data = (
            prediction['predicted_empty_time'],
            prediction['consumption_rate_per_hour'],
            prediction['cumulative_consumption'],
            prediction['remaining_volume'],
            prediction['hours_to_empty'],
            current_time,
            galon
        )
        
        if record_exists:
            query = """
                UPDATE galon_prediction
                SET predicted_empty_time = %s,
                    consumption_rate = %s,
                    cumulative_consumption = %s,
                    remaining_volume = %s,
                    hours_to_empty = %s,
                    updated_at = %s
                WHERE galon = %s
            """
        else:
            query = """
                INSERT INTO galon_prediction
                (predicted_empty_time, consumption_rate, cumulative_consumption, 
                 remaining_volume, hours_to_empty, updated_at, galon)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
        execute_query(query, data)
    except Exception as e:
        print(f"Error updating prediction: {e}")

# API Routes
@app.route('/data', methods=['POST'])
def receive_data():
    """Endpoint for receiving water usage data"""
    try:
        data = request.json
        galon = data.get('galon')
        value = data.get('value')

        if not galon or value is None:
            return jsonify({'status': 'error', 'message': 'Invalid data format'}), 400

        if not isinstance(galon, str) or not isinstance(value, (int, float)):
            return jsonify({'status': 'error', 'message': 'Invalid data types'}), 400

        insert_data(galon, float(value))
        update_prediction(galon)
        return jsonify({'status': 'success', 'message': 'Data received and stored successfully'}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/predict/<galon>', methods=['GET'])
def predict(galon):
    """Endpoint to retrieve prediction for a specific gallon"""
    try:
        prediction = compute_prediction(galon)
        if prediction is None:
            return jsonify({'status': 'error', 'message': 'Error computing prediction'}), 400
        return jsonify({'status': 'success', 'prediction': prediction}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/predictions', methods=['GET'])
def get_predictions():
    """Endpoint to retrieve all prediction records"""
    try:
        records = execute_query("SELECT * FROM galon_prediction ORDER BY updated_at DESC", 
                               fetch=True, dictionary=True)
        return jsonify({'status': 'success', 'predictions': records}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/', methods=['GET'])
def index():
    return jsonify({'status': 'success', 'message': 'Welcome to the Water Prediction API!'}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)