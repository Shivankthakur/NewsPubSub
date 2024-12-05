# File: election.py

import asyncio
import logging
import random
from util import logger_config

logger_config.setup_logger()

class LeaderElection:
    """
    Implements the Bully Algorithm for leader election in a distributed system.
    """

    def __init__(self, broker_id, peers, failure_chance=0, election_timeout=10):
        """
        :param broker_id: ID of the current broker.
        :param peers: List of peer broker IDs.
        :param failure_chance: Simulated failure chance for peer health checks (0 to 1).
        :param election_timeout: Timeout in seconds for waiting for higher ID brokers.
        """
        self.broker_id = int(broker_id)
        self.peers = [int(peer) for peer in peers if peer]
        self.leader = None
        self.failure_chance = failure_chance
        self.election_timeout = election_timeout

    async def start_leader_election(self, *_):
        """
        Starts the leader election process if no leader exists or the current leader is invalid.
        """
        if self.leader is None or self.leader not in self.peers:
            logging.info("Starting leader election process...")
            await self.elect_leader()

    async def elect_leader(self):
        """
        Implements the Bully Election Algorithm to elect a leader.
        The broker with the highest ID that is alive becomes the leader.
        """
        higher_ids = [peer for peer in self.peers if peer > self.broker_id]
        logging.debug(f"Broker {self.broker_id}: Peers with higher IDs: {higher_ids}")

        if not higher_ids:
            # No higher ID brokers; this broker becomes the leader.
            self.leader = self.broker_id
            logging.info(f"Broker {self.broker_id} elected as leader (no higher ID peers).")
        else:
            # Check if higher ID brokers are alive.
            for peer in higher_ids:
                if await self.is_alive(peer):
                    logging.info(f"Broker {peer} is alive. Broker {self.broker_id} defers election.")
                    return

            # If no higher ID peers are alive, this broker becomes the leader.
            self.leader = self.broker_id
            logging.info(f"Broker {self.broker_id} elected as leader (no response from higher ID peers).")

    async def is_alive(self, broker_id):
        """
        Simulates a health check for a higher ID broker.
        This can be replaced by an actual implementation (e.g., heartbeat or HTTP ping).
        
        :param broker_id: The ID of the broker to check.
        :return: True if the broker is alive, False otherwise.
        """
        logging.debug(f"Broker {self.broker_id}: Checking if Broker {broker_id} is alive...")
        await asyncio.sleep(0.5)  # Simulate network delay or communication latency
        is_alive = random.random() > self.failure_chance
        logging.info(
            f"Broker {self.broker_id}: Broker {broker_id} is {'alive' if is_alive else 'not alive'}."
        )
        return is_alive
