import asyncio
import json
import os
import random
from aiohttp import web
from heartbeat import Heartbeat
from election import LeaderElection
from replication import DataReplication
from data_store import DataStore

# Initialize broker configurations
BROKER_ID = os.getenv("BROKER_ID", "1")
PEER_IDS = os.getenv("PEERS", "").split(",")
PORT = 8080
HOST = "0.0.0.0"

# Initialize components
data_store = DataStore()
heartbeat = Heartbeat(BROKER_ID, PEER_IDS)
leader_election = LeaderElection(BROKER_ID, PEER_IDS)
replication = DataReplication(data_store, BROKER_ID, PEER_IDS)

# REST API routes
async def publish(request):
    """Handle a publish request and replicate the message."""
    data = await request.json()
    topic = data.get("topic")
    message = data.get("message")
    
    # Store the message locally and replicate to other brokers
    data_store.store_message(topic, message)
    await replication.replicate_message(topic, message)
    
    return web.json_response({"status": "success"})

async def get_data(request):
    """Fetch messages for a specific topic."""
    topic = request.match_info.get('topic')
    messages = data_store.get_messages(topic)
    return web.json_response({"topic": topic, "messages": messages})

# Server startup
async def init_app():
    app = web.Application()
    app.router.add_post('/publish', publish)
    app.router.add_get('/data/{topic}', get_data)
    
    # Start background tasks
    app.on_startup.append(heartbeat.start_heartbeat)
    app.on_startup.append(leader_election.start_leader_election)
    
    return app

if __name__ == '__main__':
    web.run_app(init_app(), host=HOST, port=PORT)
