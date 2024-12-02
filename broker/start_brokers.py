import subprocess
import time

# Number of brokers to start
NUM_BROKERS = 10
BASE_PORT = 3000  # Starting port number


# Function to start a broker process
def start_broker(broker_id, port, peers):
    cmd = [
        "python3",
        "broker.py",
        "--broker_id",
        str(broker_id),
        "--port",
        str(port),
        "--peers",
        ",".join(map(str, peers)),
    ]
    print(f"Starting Broker {broker_id} on port {port} with peers {peers}...")
    return subprocess.Popen(cmd)


# Start brokers
brokers = []
for i in range(1, NUM_BROKERS + 1):
    port = BASE_PORT + i - 1
    peers = [
        j for j in range(1, NUM_BROKERS + 1) if j != i
    ]  # All other brokers as peers
    broker = start_broker(i, port, peers)
    brokers.append(broker)
    time.sleep(1)  # Small delay to avoid race conditions

print(f"{NUM_BROKERS} brokers started successfully!")

# Keep the script running to manage processes
try:
    for broker in brokers:
        broker.wait()
except KeyboardInterrupt:
    print("\nShutting down all brokers...")
    for broker in brokers:
        broker.terminate()
