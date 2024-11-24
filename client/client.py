import requests

class PubSubClient:
    def __init__(self, broker_url):
        self.broker_url = broker_url

    def publish(self, topic, message):
        """Publish a message to a topic."""
        response = requests.post(f"{self.broker_url}/publish", json={"topic": topic, "message": message})
        if response.status_code == 200:
            print(f"Published message to {topic}")
        else:
            print("Failed to publish message")

    def subscribe(self, topic):
        """Subscribe to a topic and receive messages."""
        response = requests.get(f"{self.broker_url}/subscribe/{topic}")
        if response.status_code == 200:
            messages = response.json().get("messages", [])
            print(f"Messages in topic '{topic}':")
            for message in messages:
                print(f"- {message}")
        else:
            print("Failed to subscribe to topic")

if __name__ == "__main__":
    client = PubSubClient("http://localhost:8081")
    
    # Example usage
    client.publish("weather", "Sunny day in San Francisco")
    client.subscribe("weather")
