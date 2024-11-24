import asyncio
import logging
import aiohttp

class DataReplication:
    def __init__(self, data_store, broker_id, peers, port):
        self.data_store = data_store
        self.broker_id = broker_id
        self.peers = peers
        self.port = port  # Local port for this broker
        logging.basicConfig(level=logging.DEBUG)  # Set the logging level to DEBUG for more verbosity

    async def replicate_message(self, topic, message):
        """Replicate the message to all peers, ensuring no duplicate replication."""
        logging.debug(f"Attempting to replicate message '{message}' for topic '{topic}'")
        
        # Replicate the message to all peers except the current broker
        for peer in self.peers:
            if peer == self.broker_id:
                logging.debug(f"Skipping replication to self (Broker {self.broker_id})")
                continue  # Skip replicating to self
            
            # Send the message to the peer, regardless of whether it exists locally
            await self.send_to_peer(peer, topic, message)

    async def send_to_peer(self, peer, topic, message):
        """Send a message to a peer broker."""
        try:
            logging.debug(f"Preparing to send message '{message}' for topic '{topic}' to Broker {peer}")

            # Ensure peer is not empty and can be converted to integer
            if not peer:
                logging.warning("Invalid peer ID found, skipping replication.")
                return

            # Convert the peer from string to int and calculate the port
            peer_int = int(peer)  # Convert peer ID to integer
            peer_port = 3000 + peer_int-1  # Assuming port starts from 3000 for broker 1, 3001 for broker 2, etc.
            url = f"http://127.0.0.1:{peer_port}/publish"
            logging.debug(f"Calculated URL for peer {peer}: {url}")

            async with aiohttp.ClientSession() as session:
                async with session.post(url, json={
                    "topic": topic,
                    "message": message
                }) as response:
                    if response.status != 200:
                        logging.error(f"Failed to replicate message to Broker {peer} (HTTP Status: {response.status})")
                    else:
                        logging.debug(f"Successfully replicated message to Broker {peer} (HTTP Status: {response.status})")
        except ValueError as e:
            logging.error(f"Error replicating to Broker {peer}: Invalid value encountered - {e}")
        except Exception as e:
            logging.error(f"Unexpected error replicating to Broker {peer}: {e}")

