import asyncio
import logging
from util import logger_config
import aiohttp
import json

class DataReplication:
    def __init__(self, data_store, broker_id, peers, port, config_file=None):
        self.data_store = data_store
        self.broker_id = broker_id
        self.peers = peers
        self.port = port  # Local port for this broker
        self.spanning_tree = {}  # Store the spanning tree
        self.config_file = 'spanning_tree.json'  # Optional config file for static spanning tree

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
            with open(self.config_file, 'r') as f:
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
        """Build a dynamic spanning tree using peers (simplified for this example)."""
        for peer in self.peers:
            self.spanning_tree[peer] = []  # Initialize empty children list
            # In this case, each broker is connected to every other broker. 
            # You can replace this with your actual spanning tree logic.
        logging.info(f"Dynamic spanning tree built: {self.spanning_tree}")

    async def replicate_message(self, topic, message, message_id):
        """Replicate the message to all peers using the spanning tree."""
        logging.debug(f"Attempting to replicate message '{message}' for topic '{topic}' with ID {message_id}")
        
        # Start the multicast/broadcast process using the spanning tree
        await self.broadcast_message(self.broker_id, topic, message, message_id)

    async def broadcast_message(self, parent_broker, topic, message, message_id):
        """Send message to all child brokers in the spanning tree iteratively."""
        logging.debug(f"Starting broadcast process from Broker {parent_broker} for topic '{topic}' with message ID {message_id}")
        
        # Use a queue to process brokers iteratively
        queue = [parent_broker]
        
        while queue:
            current_broker = queue.pop(0)  # Get the first broker from the queue
            logging.debug(f"Broadcasting message from Broker {current_broker} (queue length: {len(queue)})")
            
            # In a real-world scenario, parent-broker would determine child-broker relations
            children = self.spanning_tree.get(str(current_broker), [])
            logging.debug(f"Broker {current_broker} has children: {children}")
            
            for child in children:
                logging.debug(f"Preparing to send message to child Broker {child}")
                await self.send_to_peer(child, topic, message, message_id)
                queue.append(child)  # Add child to the queue for further processing
                logging.debug(f"Broker {child} added to the queue")

    async def send_to_peer(self, peer, topic, message, message_id):
        """Send a message to a peer broker."""
        try:
            logging.debug(f"Preparing to send message '{message}' for topic '{topic}' with ID {message_id} to Broker {peer}")

            # Ensure peer is not empty and can be converted to integer
            if not peer:
                logging.warning("Invalid peer ID found, skipping replication.")
                return

            # Convert the peer from string to int and calculate the port
            peer_int = int(peer)  # Convert peer ID to integer
            peer_port = 3000 + peer_int - 1  # Assuming port starts from 3000 for broker 1, 3001 for broker 2, etc.
            url = f"http://127.0.0.1:{peer_port}/publish"
            logging.debug(f"Calculated URL for peer {peer}: {url}")

            async with aiohttp.ClientSession() as session:
                async with session.post(url, json={
                    "topic": topic,
                    "message": message,
                    "message_id": message_id  # Include message ID for deduplication
                }) as response:
                    if response.status != 200:
                        logging.error(f"Failed to replicate message to Broker {peer} (HTTP Status: {response.status})")
                    else:
                        logging.debug(f"Successfully replicated message to Broker {peer} (HTTP Status: {response.status})")
        except ValueError as e:
            logging.error(f"Error replicating to Broker {peer}: Invalid value encountered - {e}")
        except Exception as e:
            logging.error(f"Unexpected error replicating to Broker {peer}: {e}")