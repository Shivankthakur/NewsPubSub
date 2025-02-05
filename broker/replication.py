# File: replication.py

import asyncio
from util import logger_config
import logging
import aiohttp
import json

class DataReplication:
    def __init__(self, data_store, broker_id, port, config_file=None):
        """
        :param data_store: Local data store for the broker
        :param broker_id: ID of the current broker
        :param port: Local port for this broker
        :param config_file: Optional configuration file for the spanning tree
        """
        self.data_store = data_store
        self.broker_id = broker_id
        self.peers = []  # Initialize with an empty list; dynamic updates will populate it
        self.port = port  # Local port for this broker
        self.spanning_tree = {}  # Store the spanning tree
        self.config_file = config_file or "spanning_tree.json"  # Config file for static spanning tree
        self.failed_queue = asyncio.Queue()  # Queue for failed replication attempts

    async def build_spanning_tree(self):
        """Build a spanning tree from the peers and config file (if provided)."""
        if self.config_file:
            logging.info(f"Loading spanning tree from config file: {self.config_file}")
            await self.load_spanning_tree_from_file()
        else:
            logging.info(f"Building dynamic spanning tree using peers: {self.peers}")
            self.build_dynamic_spanning_tree()

    async def load_spanning_tree_from_file(self):
        """Load the spanning tree structure from a JSON config file."""
        try:
            with open(self.config_file, "r") as f:
                spanning_tree = json.load(f)
                # Ensure the spanning tree structure is correct
                if isinstance(spanning_tree, dict):
                    self.spanning_tree = spanning_tree
                    logging.info(f"Spanning tree loaded: {self.spanning_tree}")
                else:
                    logging.error("Invalid spanning tree format in config file.")
        except FileNotFoundError:
            logging.error(f"Config file {self.config_file} not found.")
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON from config file: {e}")

    def build_dynamic_spanning_tree(self):
        """Build a dynamic spanning tree using the membership list."""
        self.spanning_tree.clear()
        spanning_tree_nodes = self.peers.copy()
        spanning_tree_nodes.append(int(self.broker_id))  # include self id
        sorted_peers = sorted(spanning_tree_nodes)  # Sort to ensure deterministic order

        # Construct a simple BFS-based tree
        if sorted_peers:
            root = max(sorted_peers)  # Choose the highest ID as root
            self.spanning_tree[root] = [peer for peer in sorted_peers if peer != root]
            logging.info(f"Dynamic spanning tree updated: {self.spanning_tree}")
        else:
            logging.warning("No peers available to construct spanning tree.")

    async def replicate_message(self, topic, message, message_id):
        """Replicate the message to all peers using the spanning tree."""
        logging.debug(
            f"Attempting to replicate message '{message}' for topic '{topic}' with ID {message_id}"
        )

        # Start the multicast/broadcast process using the spanning tree
        for peer in self.peers:
            try:
                await self.send_to_peer(peer, topic, message, message_id)
            except Exception as e:
                logging.warning(f"Replication to {peer} failed. Adding to retry queue.")
                await self.failed_queue.put((peer, topic, message, message_id))

    async def retry_failed_replications(self):
        """Retry replication for failed messages."""
        while True:
            peer, topic, message, message_id = await self.failed_queue.get()
            try:
                await self.send_to_peer(peer, topic, message, message_id)
            except Exception:
                # If it fails again, re-add to the queue
                logging.warning(f"Retry failed for {peer}. Re-adding to queue.")
                await self.failed_queue.put((peer, topic, message, message_id))

    async def send_to_peer(self, peer, topic, message, message_id):
        """Send a message to a peer broker."""
        try:
            peer_port = 3000 + int(peer) - 1  # Assuming ports start from 3000
            url = f"http://broker-{peer}:{peer_port}/publish"
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    json={"topic": topic, "message": message, "message_id": message_id},
                ) as response:
                    if response.status != 200:
                        logging.error(
                            f"Failed to replicate to {peer} (HTTP {response.status})"
                        )
                    else:
                        logging.info(
                            f"Successfully replicated to {peer} (HTTP {response.status})"
                        )
        except Exception as e:
            logging.error(f"Unexpected error replicating to Broker {peer}: {e}")

    async def start_background_tasks(self, app):
        """Start background tasks for retrying failed replications."""
        app["replication_task"] = asyncio.create_task(self.retry_failed_replications())

    async def stop_background_tasks(self, app):
        """Stop background tasks on shutdown."""
        app["replication_task"].cancel()
        await app["replication_task"]

    def update_peers(self, peers):
        """Update the list of peers dynamically."""
        self.peers = [int(peer) for peer in peers if peer]
        logging.info(f"Updated peer list: {self.peers}")
        self.build_dynamic_spanning_tree()
