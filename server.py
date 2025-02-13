from flask import Flask, send_from_directory, jsonify
import os

app = Flask(__name__, static_folder='static')

# API routes
@app.route('/api/data')
def get_data():
    # Example API endpoint
    return jsonify({'message': 'Hello from backend!'})

# Serve frontend static files
@app.route('/')
def serve_frontend():
    return send_from_directory(app.static_folder, 'index.html')

# Catch all routes and redirect to index.html for client-side routing
@app.route('/<path:path>')
def catch_all(path):
    if os.path.exists(os.path.join(app.static_folder, path)):
        return send_from_directory(app.static_folder, path)
    return send_from_directory(app.static_folder, 'index.html')

if __name__ == '__main__':
    app.run(debug=True, port=5000)
