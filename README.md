# NewsPubSub

# Distributed Publish-Subscribe System

This project is a distributed publish-subscribe system using 10 broker nodes, implemented in Python and deployed using Docker. The system supports fault tolerance, leader election, and data replication, providing a scalable solution for message publishing and subscribing.

Front-End Repo: https://github.com/Shivankthakur/PubSub-System

Video Demo [Click on thumbnail to view video]:

[![image](https://img.youtube.com/vi/7mNXbjLrtFE/0.jpg)](https://youtu.be/7mNXbjLrtFE)

## Project Snippets

- Three publishers publishing different topics

![image](https://github.com/user-attachments/assets/2fef2a9a-530c-485b-bb56-0194ecd4b75a)


- Three subscribers receiving messages for subscribed topics
 
![image](https://github.com/user-attachments/assets/c5afd050-6862-48bb-9a25-6e722b3be445)


- Broker Logs

![image](https://github.com/user-attachments/assets/066a2761-25be-4237-844e-4f4f852733ea)

![image](https://github.com/user-attachments/assets/f883a5a2-6964-4c62-aaf6-6b2806e4804f)



## **Project Structure**


```
distributed-pubsub/
│-- broker/
│   │-- broker.py                  # Main broker application with API endpoints
│   │-- data_store.py              # Local data store for the broker
│   │-- election.py                # Leader election algorithm implementation
│   │-- heartbeat.py               # Heartbeat failure detection mechanism
│   │-- replication.py             # Data replication mechanism
│   │-- membership.py              # Maintains membership list
│   │-- registry.py                # For discovery 
│   └-- requirements.txt           # Python dependencies
│-- Dockerfile                     # Dockerfile for the broker
│-- docker-compose.yml             # Docker Compose configuration to run multiple brokers
└-- README.md                      # Instructions for running the project
```

## **Setup Instructions**

### **Prerequisites**

1. Install [Docker](https://www.docker.com/) and [Docker-Compose](https://docs.docker.com/compose/).
2. Clone this repository:
   ```bash
   git clone <repository-url>
   cd NewsPubSub
   cd broker
   ```

### **Running the System**

1. **Build the Docker Image**:
   ```bash
   docker-compose build
   ```

2. **Deploy the Brokers**:
   ```bash
   docker-compose up -d
   ```

3. **Verify that all brokers are running**:
   - Access individual brokers at `http://localhost:8081`, `http://localhost:8082`, ..., `http://localhost:8085`.

4. **View Logs for Debugging**:
   ```bash
   docker-compose logs -f
   ```

### **Stopping the System**

To stop and remove the containers:
```bash
docker-compose down
```

## **Testing**

1. **Publish and Replication Test**:
   ```bash
   python tests/test_publish.py
   ```

2. **Leader Election Test**:
   ```bash
   python tests/test_leader_election.py
   ```

## **Implementation Details**

### **1. Failure Detection**
Each broker sends periodic heartbeats to its peers. If a broker does not receive a heartbeat from another broker within a specified timeout, it marks that broker as failed.

### **2. Leader Election**
The leader election follows a simple Bully Election Algorithm, where the broker with the highest ID becomes the leader if no higher ID broker responds.

### **3. Data Replication**
When a message is published to a broker, it stores the message and replicates it to all its peers. The replication ensures data consistency across all brokers.


### client

```
python3 client_interface.py --mode publish --topic "news" --message "New article on distributed systems"
```

```
python3 client_interface.py --mode subscribe --topic "news" 
```
