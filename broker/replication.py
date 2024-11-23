import asyncio
import logging
from aiohttp import ClientSession

class DataReplication:
    def __init__(self, data_store, broker_id, peers):
        self.data_store = data_store
        self.broker_id = broker_id
        self.peers = peers
    
    async def replicate_message(self, topic, message):
        """Replicate the message to all peer brokers."""
        async with ClientSession() as session:
            for peer in self.peers:
                try:
                    url = f"http://broker{peer}:8080/replicate"
                    payload = {"topic": topic, "message": message}
                    async with session.post(url, json=payload) as response:
                        if response.status == 200:
                            logging.info(f"Message replicated to Broker {peer}")
                except Exception as e:
                    logging.error(f"Failed to replicate to Broker {peer}: {e}")
