import argparse
import asyncio
import logging
from util import logger_config
import uuid
from aiohttp import web
from heartbeat import Heartbeat
from election import LeaderElection
from replication import DataReplication
from datatable import DataStore  # Database handler
import aiohttp
from membership import Membership

logger_config.setup_logger()

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Start a broker instance.")
parser.add_argument("--broker_id", type=int, required=True, help="Broker ID")
parser.add_argument("--port", type=int, required=True, help="Port to run the broker on")
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

# Initialize components
data_store = DataStore()  # SQLite database for storing messages
heartbeat = Heartbeat(BROKER_ID)  # Heartbeat without initial peers
replication = DataReplication(data_store, BROKER_ID, port=PORT)


async def on_membership_change(new_members):
    """Handle membership changes."""
    peers = list(new_members - {BROKER_ID})  # Exclude self
    replication.update_peers(peers)
    heartbeat.update_peers(peers)
    logging.info(f"Updated peers on membership change: {peers}")

    # Start leader election if the membership changes
    await leader_election.start_leader_election()


membership = Membership(
    BROKER_ID, REGISTRY_URL, on_membership_change=on_membership_change
)
# Initialize LeaderElection with dynamic peers (initially empty)
leader_election = LeaderElection(
    BROKER_ID, peers=[], membership_service=membership
)


async def on_peer_failure(failed_peer):
    """Handle peer failure (invoke registry to remove failed broker)."""
    logging.info(f"Handling failure for peer {failed_peer}")
    if REGISTRY_URL:
        try:
            url = f"{REGISTRY_URL}/remove/{failed_peer}"
            async with aiohttp.ClientSession() as session:
                async with session.delete(url) as response:
                    if response.status == 200:
                        logging.info(
                            f"Successfully removed broker {failed_peer} from registry."
                        )
                    else:
                        logging.warning(
                            f"Failed to remove broker {failed_peer} from registry (HTTP {response.status})."
                        )
        except Exception as e:
            logging.error(f"Error removing broker {failed_peer} from registry: {e}")
    else:
        logging.warning(
            "Registry URL is not defined. Broker removal will not be performed."
        )


# Pass the failure callback to Heartbeat
heartbeat.on_peer_failure = on_peer_failure


async def discover_peers():
    """Discover peers dynamically using the membership list."""
    global replication, heartbeat, leader_election
    await membership.fetch_members()
    peers = list(membership.members - {BROKER_ID})  # Exclude self
    replication.update_peers(peers)
    heartbeat.update_peers(peers)
    leader_election.peers = peers  # Update peers for leader election
    logging.info(f"Discovered peers: {peers}")


async def build_tree_and_start():
    """Build the spanning tree and start the server."""
    try:
        await discover_peers()  # Dynamically discover peers from membership
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


async def leader_announcement(request):
    """
    Endpoint for receiving leader announcements from other brokers.
    
    Expected JSON payload:
    {
        "leader_id": int
    }
    """
    try:
        data = await request.json()
        leader_id = data.get("leader_id")

        if leader_id:
            leader_election.leader = leader_id
            logging.info(f"Broker {BROKER_ID} acknowledged new leader {leader_id}")
            return web.json_response({"status": "success", "message": "Leader announced."})
        else:
            logging.warning(f"Invalid leader announcement received: {data}")
            return web.json_response({"status": "error", "message": "Invalid leader announcement."}, status=400)
    except Exception as e:
        logging.exception(f"Error in leader announcement route: {e}")
        return web.json_response({"status": "error", "message": str(e)}, status=500)


# Background task for heartbeat and leader election
async def start_background_tasks(app):
    """Start background tasks."""
    app["membership_task"] = asyncio.create_task(membership.start_membership_service())
    app["heartbeat_task"] = asyncio.create_task(heartbeat.start_heartbeat())
    app["leader_election_task"] = asyncio.create_task(
        leader_election.start_leader_election()
    )
    await replication.start_background_tasks(app)


async def cleanup_background_tasks(app):
    """Cancel background tasks and close database."""
    app["heartbeat_task"].cancel()
    app["leader_election_task"].cancel()  # Cancel leader election task
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
    app.router.add_post("/leader_announcement", leader_announcement)  # New route for leader announcements
    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)
    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(build_tree_and_start())
    except Exception as e:
        logging.exception(f"Failed to start broker: {e}")
