import requests
import time

# Endpoint to publish data to the broker
BROKER_URL = "http://localhost:8081/publish"

def test_publish(topic, message):
    """Send a publish request to the broker."""
    response = requests.post(BROKER_URL, json={"topic": topic, "message": message})
    return response.status_code == 200

def check_data_replication(brokers, topic, expected_message):
    """Check if the message is replicated to all brokers."""
    for broker in brokers:
        response = requests.get(f"http://{broker}/data/{topic}")
        if response.json().get("messages")[-1] != expected_message:
            return False
    return True

if __name__ == "__main__":
    topic = "test_topic"
    message = "Hello, Distributed World!"

    if test_publish(topic, message):
        print("Publish successful. Checking replication...")
        time.sleep(2)  # Wait for replication to complete

        brokers = ["localhost:8081", "localhost:8082", "localhost:8083", "localhost:8084", "localhost:8085"]
        if check_data_replication(brokers, topic, message):
            print("Data replication verified successfully!")
        else:
            print("Data replication failed.")
    else:
        print("Publish failed.")
