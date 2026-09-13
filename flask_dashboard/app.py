from flask import Flask
from routes import bp as api_bp
import os
import logging

app = Flask(__name__)
app.secret_key = os.urandom(24)

# Register route blueprint
app.register_blueprint(api_bp)
logging.info(f"Registered blueprint: {api_bp.name}")

if __name__ == "__main__":
    app.run(debug=True, port=5000)