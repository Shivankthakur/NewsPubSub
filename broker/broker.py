# broker.py

import argparse
import asyncio
import json  # Ensure this is imported
import os
import logging
from util import logger_config
from aiohttp import web
from heartbeat import Heartbeat
from election import LeaderElection
from replication import DataReplication
from data_store import DataStore
import uuid

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Start a broker instance.")
parser.add_argument('--broker_id', type=int, required=True, help="Broker ID")
parser.add_argument('--port', type=int, required=True, help="Port to run the broker on")
parser.add_argument('--peers', type=str, required=False, help="Comma-separated list of peer broker IDs")
args = parser.parse_args()

# Broker configurations
BROKER_ID = args.broker_id
PORT = args.port
HOST = "0.0.0.0"  # Listen on all interfaces

# Set PEER_IDS dynamically from the --peers argument or fall back to empty list
if args.peers:
    PEER_IDS = args.peers.split(",")
else:
    PEER_IDS = []
    logging.warning("No peers defined. Please provide the --peers argument.")

# Log the CLI args and configuration parameters
logging.debug(f"CLI Arguments: broker_id={BROKER_ID}, port={PORT}")
logging.debug(f"PEER_IDS: {PEER_IDS}")

# Initialize components
data_store = DataStore()
heartbeat = Heartbeat(BROKER_ID, PEER_IDS)
leader_election = LeaderElection(BROKER_ID, PEER_IDS)
replication = DataReplication(data_store, BROKER_ID, PEER_IDS, port=PORT)

# This line ensures spanning tree is built before replication is attempted
async def build_tree_and_start():
    """Build the spanning tree and then start the server."""
    try:
        await replication.build_spanning_tree()
        logging.info(f"Spanning tree built for Broker {BROKER_ID}: {replication.spanning_tree}")
        
        # Start the server and block to keep it open
        app = await start_server()
        # Awaiting the server to run and handle requests
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, HOST, PORT)
        await site.start()
        logging.info(f"Broker {BROKER_ID} is now running at http://{HOST}:{PORT}")
        # Keep the server running
        while True:
            await asyncio.sleep(3600)  # Sleep for 1 hour, preventing server from exiting
    except Exception as e:
        logging.exception(f"Error building spanning tree for Broker {BROKER_ID}: {e}")

# REST API routes
async def publish(request):
    """Handle a publish request and replicate the message."""
    try:
        data = await request.json()
        topic = data.get("topic")
        message = data.get("message")
        message_id = data.get("message_id", str(uuid.uuid4()))  # Generate or receive a message ID

        # Log incoming request
        logging.debug(f"Publish request received: {data}")

        # Store the message locally and replicate to other brokers
        if data_store.store_message(topic, message, message_id):
            await replication.replicate_message(topic, message, message_id)
            return web.json_response({"status": "success"})
        else:
            return web.json_response({"status": "failure", "message": "Duplicate message detected."})
    except Exception as e:
        logging.exception(f"Error in publish route: {e}")
        return web.json_response({"status": "error", "message": str(e)}, status=500)

async def get_data(request):
    """Fetch messages for a specific topic."""
    try:
        topic = request.match_info.get('topic')
        messages = data_store.get_messages(topic)

        # Log request and response
        logging.debug(f"Data request received for topic: {topic}")
        logging.debug(f"Returning messages: {messages}")

        return web.json_response({"topic": topic, "messages": messages})
    except Exception as e:
        logging.exception(f"Error in get_data route: {e}")
        return web.json_response({"status": "error", "message": str(e)}, status=500)

async def test_broker(request):
    """Simple health check route."""
    return web.Response(text="Broker is UP and RUNNING. Hello, world!")

# Background task for heartbeat and leader election
async def start_background_tasks(app):
    """Start background tasks like heartbeat and leader election."""
    app['heartbeat_task'] = asyncio.create_task(heartbeat.start_heartbeat())
    app['leader_election_task'] = asyncio.create_task(leader_election.start_leader_election())

# Cleanup tasks when the server shuts down
async def cleanup_background_tasks(app):
    """Cancel background tasks when the server shuts down."""
    app['heartbeat_task'].cancel()
    app['leader_election_task'].cancel()
    await asyncio.gather(app['heartbeat_task'], app['leader_election_task'], return_exceptions=True)

# Server startup
async def start_server():
    """Initialize the application and add routes and background tasks."""
    app = web.Application()

    # Routes
    app.router.add_get('/test', test_broker)
    app.router.add_post('/publish', publish)
    app.router.add_get('/data/{topic}', get_data)

    # Schedule background tasks
    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)

    return app

if __name__ == '__main__':
    logging.info(f"Starting broker {BROKER_ID} on {HOST}:{PORT}...")
    logging.debug(f"PEER_IDS: {PEER_IDS}")  # Debug log to check PEER_IDS
    try:
        asyncio.run(build_tree_and_start())
    except Exception as e:
        logging.exception(f"Failed to start the broker: {e}")
