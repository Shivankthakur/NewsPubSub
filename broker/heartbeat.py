# heartbeat.py

import asyncio
import logging
import random

class Heartbeat:
    def __init__(self, broker_id, peers, failure_chance=0.0, heartbeat_interval=5):
        self.broker_id = int(broker_id)
        self.peers = [int(peer) for peer in peers if peer]
        self.heartbeat_interval = heartbeat_interval  # Default to 5 seconds
        self.failed_peers = set()
        self.failure_chance = failure_chance  # Configurable failure chance

    async def start_heartbeat(self, *_):
        """Start heartbeat monitoring as a background task."""
        while True:
            await self.check_peers()
            await asyncio.sleep(self.heartbeat_interval)

    async def check_peers(self):
        """Check if peers are alive by pinging each one."""
        for peer in self.peers:
            alive = await self.is_peer_alive(peer)
            if not alive:
                if peer not in self.failed_peers:
                    logging.warning(f"Peer {peer} failed.")
                    self.failed_peers.add(peer)
            else:
                if peer in self.failed_peers:
                    logging.info(f"Peer {peer} is back online.")
                    self.failed_peers.discard(peer)

    async def is_peer_alive(self, broker_id):
        """Simulate heartbeat check (real implementation would ping broker)."""
        await asyncio.sleep(0.3)  # Simulating delay
        return random.random() > self.failure_chance  # Simulate failure based on configurable chance