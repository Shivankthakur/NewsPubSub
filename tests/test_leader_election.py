import time
import requests

def check_leader(brokers):
    """Check which broker is the current leader."""
    for broker in brokers:
        try:
            response = requests.get(f"http://{broker}/leader")
            if response.status_code == 200:
                print(f"Current leader is Broker {response.json().get('leader')}")
        except Exception:
            print(f"Broker {broker} is down or unreachable.")

if __name__ == "__main__":
    brokers = ["localhost:8081", "localhost:8082", "localhost:8083", "localhost:8084", "localhost:8085"]
    
    # Check initial leader
    check_leader(brokers)

    # Simulate leader failure by stopping a broker
    print("Simulating leader failure...")
    # Assume we manually stop a broker here, like broker1
    time.sleep(5)
    
    # Check new leader after failure
    check_leader(brokers)
