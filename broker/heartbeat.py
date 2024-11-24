import asyncio
import logging
import random

class Heartbeat:
    def __init__(self, broker_id, peers):
        self.broker_id = int(broker_id)
        self.peers = [int(peer) for peer in peers if peer]  # Ensure only non-empty peers
        self.heartbeat_interval = 5  # seconds
        self.failed_peers = set()  # Keep track of failed peers

    async def start_heartbeat(self, *_):
        """Start heartbeat monitoring as a background task."""
        while True:
            await self.check_peers()
            await asyncio.sleep(self.heartbeat_interval)

    async def check_peers(self):
        """Check if peers are alive by pinging each one."""
        for peer in self.peers:
            if not await self.is_peer_alive(peer):
                if peer not in self.failed_peers:
                    logging.warning(f"Peer {peer} failed.")
                    self.failed_peers.add(peer)
            else:
                if peer in self.failed_peers:
                    logging.info(f"Peer {peer} is back online.")
                    self.failed_peers.discard(peer)
    
    async def is_peer_alive(self, broker_id):
        """Simulate heartbeat check (real implementation would ping broker)."""
        # Simulate a 20% chance of failure
        await asyncio.sleep(0.3)  # Simulating delay
        failure_chance = 0.0
        return random.random() > failure_chance  # 80% chance of success, 20% chance of failure