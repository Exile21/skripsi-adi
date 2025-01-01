from flask import Flask, request, jsonify
import mysql.connector
from mysql.connector import Error

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
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            connection.close()

@app.route('/data', methods=['POST'])
def receive_data():
    try:
        data = request.json
        galon = data.get('galon')  # string type
        value = data.get('value')  # float type

        if galon is None or value is None:
            return jsonify({'status': 'error', 'message': 'Invalid data format'}), 400

        # Validate data types
        if not isinstance(galon, str) or not isinstance(value, (int, float)):
            return jsonify({'status': 'error', 'message': 'Invalid data types'}), 400

        insert_data(galon, float(value))
        return jsonify({'status': 'success', 'message': 'Data received and stored successfully!'}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
