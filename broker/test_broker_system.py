import subprocess
import time
import requests

# Paths to the broker script
broker_script = "broker.py"  # Update this path if broker.py is in another directory


# Function to start a broker process
def start_broker(broker_id, port, peers):
    return subprocess.Popen(
        [
            "python3",
            broker_script,
            "--broker_id",
            str(broker_id),
            "--port",
            str(port),
            "--peers",
            ",".join(map(str, peers)),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


# Start brokers
broker_ports = [3000, 3001, 3002]
brokers = []
try:
    print("Starting brokers...")
    for i, port in enumerate(broker_ports):
        peers = [j + 1 for j in range(len(broker_ports)) if j != i]  # Exclude self
        broker = start_broker(i + 1, port, peers)
        brokers.append(broker)
        time.sleep(2)  # Allow time for each broker to start
    print("Brokers started.")

    # Publish a message to Broker 1
    publish_url = f"http://127.0.0.1:{broker_ports[0]}/publish"
    message_payload = {"topic": "test_topic", "message": "Hello, World!"}
    print("Publishing message to Broker 1...")
    publish_response = requests.post(publish_url, json=message_payload)
    print(f"Publish response: {publish_response.json()}")

    # Fetch messages from all brokers
    print("Fetching messages from all brokers...")
    fetch_results = {}
    for i, port in enumerate(broker_ports):
        fetch_url = f"http://127.0.0.1:{port}/data/test_topic"
        response = requests.get(fetch_url)
        fetch_results[port] = response.json()
    print(f"Initial fetch results: {fetch_results}")

    # Simulate Broker 2 failure
    print("Simulating Broker 2 failure...")
    brokers[1].terminate()
    brokers[1].wait()

    # Publish another message to Broker 1
    message_payload_2 = {"topic": "test_topic", "message": "Second Message"}
    print("Publishing second message to Broker 1...")
    publish_response_2 = requests.post(publish_url, json=message_payload_2)
    print(f"Publish response: {publish_response_2.json()}")

    # Restart Broker 2
    print("Restarting Broker 2...")
    brokers[1] = start_broker(2, broker_ports[1], [1, 3])
    time.sleep(5)  # Allow time for Broker 2 to resync

    # Fetch messages from all brokers again
    print("Fetching messages from all brokers after restart...")
    fetch_results_after_restart = {}
    for i, port in enumerate(broker_ports):
        fetch_url = f"http://127.0.0.1:{port}/data/test_topic"
        try:
            response = requests.get(fetch_url)
            fetch_results_after_restart[port] = response.json()
        except requests.exceptions.RequestException as e:
            fetch_results_after_restart[port] = {"error": str(e)}
    print(f"Fetch results after restart: {fetch_results_after_restart}")

finally:
    # Cleanup: Ensure all brokers are terminated
    print("Terminating brokers...")
    for broker in brokers:
        broker.terminate()
    print("Test complete.")
