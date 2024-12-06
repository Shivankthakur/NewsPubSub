# File: registry.py

from flask import Flask, request, jsonify
import logging
import requests
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


@app.route("/remove/<int:broker_id>", methods=["DELETE"])
def remove_broker(broker_id):
    """Remove a broker from the registry."""
    try:
        if broker_id in members:
            members.remove(broker_id)
            logging.info(f"Broker {broker_id} removed from registry.")
            return f"Broker {broker_id} removed", 200
        else:
            logging.warning(f"Remove failed: Broker {broker_id} not found.")
            return "Broker not found", 404
    except Exception as e:
        logging.exception("Error during broker removal:")
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


@app.route("/dcnews", methods=["POST", "GET"])
def dcnews():
    """
    Entry API for redirecting to publish and subscribe endpoints.

    - POST request: Redirect to the publish endpoint of the broker with the max ID.
    - GET request: Redirect to the subscribe endpoint of the broker with the max ID.
    """
    if not members:
        logging.error("No active brokers available for redirection.")
        return jsonify({"error": "No active brokers available"}), 503

    # Get the broker with the maximum ID
    max_broker_id = max(members)
    broker_host = f"localhost:300{max_broker_id-1}"  # Assuming brokers run on ports 5001, 5002, etc.
    logging.info(f"Redirecting request to Broker {max_broker_id} at {broker_host}")

    if request.method == "POST":
        # Redirect to the publish endpoint
        target_url = f"http://{broker_host}/publish"
        try:
            response = requests.post(target_url, json=request.json)
            response.raise_for_status()
            return jsonify(response.json()), response.status_code
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to redirect publish request to {target_url}: {e}")
            return jsonify({"error": "Failed to process publish request"}), 500

    elif request.method == "GET":
        # Redirect to the subscribe endpoint
        topic = request.args.get("topic")
        if not topic:
            logging.error("Missing 'topic' parameter in subscription request.")
            return jsonify({"error": "Missing 'topic' parameter"}), 400
        target_url = f"http://{broker_host}/data/{topic}"
        try:
            response = requests.get(target_url)
            response.raise_for_status()
            return jsonify(response.json()), response.status_code
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to redirect subscribe request to {target_url}: {e}")
            return jsonify({"error": "Failed to process subscribe request"}), 500


if __name__ == "__main__":
    logging.info("Starting Registry Service on port 5000...")
    app.run(host="0.0.0.0", port=4000)
