# heartbeat.py

import asyncio
import logging
from aiohttp import ClientSession, ClientTimeout

class Heartbeat:
    def __init__(self, broker_id, peers, failure_timeout=2, heartbeat_interval=5):
        """
        :param broker_id: ID of the current broker
        :param peers: List of peer broker IDs
        :param failure_timeout: Timeout in seconds to consider a peer failed
        :param heartbeat_interval: Interval in seconds to send heartbeats
        """
        self.broker_id = int(broker_id)
        self.peers = [int(peer) for peer in peers if peer]
        self.failed_peers = set()
        self.failure_timeout = failure_timeout
        self.heartbeat_interval = heartbeat_interval

    async def start_heartbeat(self, *_):
        """Start heartbeat monitoring as a background task."""
        while True:
            logging.debug("Heartbeat is working...")
            await self.check_peers()
            await self.log_peer_status()  # Log online and failed peers
            await asyncio.sleep(self.heartbeat_interval)

    async def check_peers(self):
        """Check if peers are alive by pinging each one."""
        for peer in self.peers:
            is_alive = await self.is_peer_alive(peer)
            if not is_alive:
                if peer not in self.failed_peers:
                    logging.warning(f"Peer {peer} has failed.")
                    self.failed_peers.add(peer)
            else:
                if peer in self.failed_peers:
                    logging.info(f"Peer {peer} is back online.")
                    self.failed_peers.discard(peer)

    async def is_peer_alive(self, broker_id):
        """Check if a peer is alive by sending an HTTP request."""
        peer_port = 3000 + int(broker_id) - 1  # Map broker ID to port
        url = f"http://127.0.0.1:{peer_port}/heartbeat"  # Updated endpoint
        timeout = ClientTimeout(total=self.failure_timeout)

        try:
            async with ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        logging.info(f"Peer {broker_id} is alive.")
                        return True
                    else:
                        logging.warning(f"Peer {broker_id} returned HTTP {response.status}.")
                        return False
        except Exception as e:
            logging.error(f"Peer {broker_id} heartbeat check failed: {e}")
            return False

    def update_peers(self, peers):
        """Update the list of peers dynamically."""
        self.peers = [int(peer) for peer in peers if peer]
        logging.info(f"Updated peer list: {self.peers}")

    async def log_peer_status(self):
        """Log the current status of online and failed peers."""
        online_peers = [peer for peer in self.peers if peer not in self.failed_peers]
        logging.info(f"Online Brokers: {online_peers}")
        logging.info(f"Failed Brokers: {list(self.failed_peers)}")
