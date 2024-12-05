# broker.py

import argparse
import asyncio
import logging
import uuid
from aiohttp import web
from heartbeat import Heartbeat
from election import LeaderElection
from replication import DataReplication
from datatable import DataStore  # Database handler
import aiohttp

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Start a broker instance.")
parser.add_argument("--broker_id", type=int, required=True, help="Broker ID")
parser.add_argument("--port", type=int, required=True, help="Port to run the broker on")
parser.add_argument(
    "--peers", type=str, required=False, help="Comma-separated list of peer broker IDs"
)
parser.add_argument(
    "--registry",
    type=str,
    required=False,
    help="Registry service URL for peer discovery",
)
args = parser.parse_args()

# Broker configurations
BROKER_ID = args.broker_id
PORT = args.port
HOST = "0.0.0.0"  # Listen on all interfaces
REGISTRY_URL = args.registry

# Parse peer IDs if provided
PEER_IDS = args.peers.split(",") if args.peers else []

# Initialize components
data_store = DataStore()  # SQLite database for storing messages
heartbeat = Heartbeat(BROKER_ID, PEER_IDS)  # Pass current broker ID and peers
leader_election = LeaderElection(BROKER_ID, PEER_IDS)
replication = DataReplication(data_store, BROKER_ID, PEER_IDS, port=PORT)


async def discover_peers():
    """Discover peers dynamically from a registry service."""
    global replication, heartbeat, PEER_IDS
    if REGISTRY_URL:
        try:
            logging.info(
                f"Contacting registry service at {REGISTRY_URL} to discover peers..."
            )
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{REGISTRY_URL}/peers/{BROKER_ID}") as response:
                    if response.status == 200:
                        peers = await response.json()
                        PEER_IDS.clear()  # Clear the current peers
                        PEER_IDS.extend(peers)  # Add the new peers
                        replication.update_peers(peers)  # Update peers in Replication
                        heartbeat.update_peers(peers)  # Update peers in Heartbeat
                        logging.info(f"Discovered peers: {peers}")
                    else:
                        logging.warning(
                            f"Failed to contact registry: {response.status}"
                        )
        except Exception as e:
            logging.exception(f"Error discovering peers: {e}")
    else:
        logging.warning("No registry URL provided. Running without peer discovery.")


async def build_tree_and_start():
    """Build the spanning tree and start the server."""
    try:
        await discover_peers()  # Dynamically discover peers if registry is available
        await replication.build_spanning_tree()
        logging.info(
            f"Spanning tree built for Broker {BROKER_ID}: {replication.spanning_tree}"
        )

        # Start the server and block to keep it open
        app = await start_server()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, HOST, PORT)
        await site.start()
        logging.info(f"Broker {BROKER_ID} is running at http://{HOST}:{PORT}")

        while True:
            await asyncio.sleep(3600)
    except Exception as e:
        logging.exception(f"Error building spanning tree for Broker {BROKER_ID}: {e}")


# REST API routes
async def publish(request):
    """Handle a publish request and replicate the message."""
    try:
        data = await request.json()
        topic = data.get("topic")
        message = data.get("message")
        message_id = data.get(
            "message_id", str(uuid.uuid4())
        )  # Generate a message ID if not provided

        # Store the message in the SQLite database
        stored = data_store.store_message(topic, message, message_id)
        if stored:
            logging.info(f"Message published: {topic} -> {message} (ID: {message_id})")

            # Replicate the message to other brokers
            await replication.replicate_message(topic, message, message_id)
            return web.json_response({"status": "success"})
        else:
            logging.warning(f"Duplicate message detected: {message_id}")
            return web.json_response(
                {"status": "failure", "message": "Duplicate message detected."}
            )
    except Exception as e:
        logging.exception(f"Error in publish route: {e}")
        return web.json_response({"status": "error", "message": str(e)}, status=500)


async def get_data(request):
    """Fetch messages for a specific topic."""
    try:
        topic = request.match_info.get("topic")
        messages = data_store.get_messages(topic)  # Fetch messages from SQLite
        return web.json_response({"topic": topic, "messages": messages})
    except Exception as e:
        logging.exception(f"Error in get_data route: {e}")
        return web.json_response({"status": "error", "message": str(e)}, status=500)


async def heartbeat_check(request):
    """Health check endpoint for broker."""
    return web.Response(text=f"Broker {BROKER_ID} is healthy and running.")


# Background task for heartbeat and leader election
async def start_background_tasks(app):
    """Start background tasks."""
    app["heartbeat_task"] = asyncio.create_task(heartbeat.start_heartbeat())
    app["leader_election_task"] = asyncio.create_task(
        leader_election.start_leader_election()
    )
    await replication.start_background_tasks(app)


async def cleanup_background_tasks(app):
    """Cancel background tasks and close database."""
    app["heartbeat_task"].cancel()
    app["leader_election_task"].cancel()
    await asyncio.gather(
        app["heartbeat_task"], app["leader_election_task"], return_exceptions=True
    )
    # Stop replication retries
    await replication.stop_background_tasks(app)
    # Close the database connection
    data_store.close()


async def start_server():
    """Initialize the application and add routes."""
    app = web.Application()
    app.router.add_get("/heartbeat", heartbeat_check)
    app.router.add_post("/publish", publish)
    app.router.add_get("/data/{topic}", get_data)
    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)
    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(build_tree_and_start())
    except Exception as e:
        logging.exception(f"Failed to start broker: {e}")
