# File: registry.py

from flask import Flask, request, jsonify
import logging
from util import logger_config

logger_config.setup_logger()

app = Flask(__name__)

# In-memory membership list
members = set()


@app.route("/register", methods=["POST"])
def register():
    """Register a new broker."""
    try:
        data = request.json
        broker_id = data.get("broker_id")
        if broker_id:
            if broker_id not in members:
                members.add(broker_id)
                logging.info(f"Broker {broker_id} registered successfully.")
                return "Registered", 200
            else:
                logging.info(f"Broker {broker_id} is already registered.")
                return "Already Registered", 200
        else:
            logging.warning("Invalid registration request: 'broker_id' missing.")
            return "Bad Request: 'broker_id' is required", 400
    except Exception as e:
        logging.exception("Error during registration:")
        return "Internal Server Error", 500


@app.route("/deregister", methods=["POST"])
def deregister():
    """Deregister an existing broker."""
    try:
        data = request.json
        broker_id = data.get("broker_id")
        if broker_id in members:
            members.remove(broker_id)
            logging.info(f"Broker {broker_id} deregistered successfully.")
            return f"Broker {broker_id} deregistered", 200
        else:
            logging.warning(f"Deregistration failed: Broker {broker_id} not found.")
            return "Broker not found", 404
    except Exception as e:
        logging.exception("Error during deregistration:")
        return "Internal Server Error", 500


@app.route("/members", methods=["GET"])
def get_members():
    """Retrieve the current list of registered brokers."""
    try:
        logging.info("Membership list fetched successfully.")
        return jsonify(list(members)), 200
    except Exception as e:
        logging.exception("Error fetching membership list:")
        return "Internal Server Error", 500


if __name__ == "__main__":
    logging.info("Starting Registry Service on port 5000...")
    app.run(host="0.0.0.0", port=5000)
