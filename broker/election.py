# election.py

import asyncio
import logging
from util import logger_config
import random

class LeaderElection:
    def __init__(self, broker_id, peers, failure_chance=0, election_timeout=10):
        self.broker_id = int(broker_id)
        self.peers = [int(peer) for peer in peers if peer]
        self.leader = None
        self.failure_chance = failure_chance  # Configurable failure chance for peer health check
        self.election_timeout = election_timeout  # Timeout for waiting for higher ID brokers (seconds)

    async def start_leader_election(self, *_):
        """Initiate leader election upon startup."""
        if self.leader is None or self.leader not in self.peers:
            await self.elect_leader()
    
    async def elect_leader(self):
        """Bully Election Algorithm to elect a leader."""
        # Identify brokers with higher IDs
        higher_ids = [peer for peer in self.peers if peer > self.broker_id]
        
        if not higher_ids:
            # No higher ID brokers, so this broker becomes the leader
            self.leader = self.broker_id
            logging.info(f"Broker {self.broker_id} elected as leader.")
        else:
            # Ask higher ID brokers if they are alive (simulated)
            for peer in higher_ids:
                if await self.is_alive(peer):
                    # If any higher ID broker is alive, it should take leadership
                    logging.info(f"Broker {peer} is alive, skipping leader election.")
                    return
        
            # If no higher IDs are alive, this broker becomes the leader
            self.leader = self.broker_id
            logging.info(f"Broker {self.broker_id} elected as leader.")
    
    async def is_alive(self, broker_id):
        """Simulate the check if a higher ID broker is alive (to be replaced by actual health check)."""
        await asyncio.sleep(0.5)  # Simulating network delay or communication with peer
        return random.random() > self.failure_chance  # Simulate failure based on configurable chance
