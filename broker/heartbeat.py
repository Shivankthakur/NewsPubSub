import asyncio
import logging
from aiohttp import ClientSession

class Heartbeat:
    def __init__(self, broker_id, peers):
        self.broker_id = broker_id
        self.peers = peers
        self.failed_peers = set()
        self.heartbeat_interval = 2  # seconds
    
    async def send_heartbeat(self):
        """Send heartbeat to all peers and check their status."""
        async with ClientSession() as session:
            while True:
                for peer in self.peers:
                    try:
                        url = f"http://broker{peer}:8080/heartbeat"
                        async with session.get(url, timeout=1) as response:
                            if response.status == 200:
                                self.failed_peers.discard(peer)
                    except Exception:
                        self.failed_peers.add(peer)
                        logging.warning(f"Peer {peer} failed.")
                
                await asyncio.sleep(self.heartbeat_interval)
    
    async def start_heartbeat(self, _):
        """Background task to continuously send heartbeats."""
        asyncio.create_task(self.send_heartbeat())
