import asyncio
import logging
from util import logger_config
from membership import Membership
import aiohttp

logger_config.setup_logger()


class LeaderElection:
    """
    Implements the Bully Algorithm for leader election in a distributed system.
    """

    def __init__(self, broker_id, peers, membership_service, election_timeout=10):
        """
        :param broker_id: ID of the current broker.
        :param peers: List of peer broker IDs.
        :param membership_service: Instance of the Membership class to track brokers.
        :param election_timeout: Timeout in seconds for waiting for higher ID brokers.
        """
        self.broker_id = int(broker_id)
        self.membership_service = membership_service  # Membership instance
        self.peers = [int(peer) for peer in peers if peer]  # Initial peer list
        self.leader = None
        self.election_timeout = election_timeout

    async def update_peers(self):
        """
        Updates the list of peers using the Membership service.
        Excludes the current broker's ID from the peer list.
        """
        await self.membership_service.fetch_members()  # Update membership
        self.peers = [peer for peer in self.membership_service.members if peer != self.broker_id]
        logging.info(f"Broker {self.broker_id}: Updated peer list: {self.peers}")

    async def start_leader_election(self, *_):
        """
        Starts the leader election process if no leader exists or the current leader is invalid.
        """
        await self.update_peers()  # Ensure peers are up-to-date
        if self.leader is None or self.leader not in self.peers:
            logging.info("Starting leader election process...")
            await self.elect_leader()

    async def elect_leader(self):
        """
        Implements the Bully Election Algorithm to elect a leader.
        The broker with the highest ID that is alive becomes the leader.
        """
        await self.update_peers()  # Refresh peers
        higher_ids = [peer for peer in self.peers if peer > self.broker_id]
        logging.debug(f"Broker {self.broker_id}: Peers with higher IDs: {higher_ids}")

        if not higher_ids:
            # No higher ID brokers; this broker becomes the leader.
            self.leader = self.broker_id
            logging.info(f"Broker {self.broker_id} elected as leader (no higher ID peers).")
            await self.announce_leader()
        else:
            # Check if higher ID brokers are alive.
            for peer in higher_ids:
                if await self.is_alive(peer):
                    logging.info(f"Broker {peer} is alive. Broker {self.broker_id} defers election.")
                    return

            # If no higher ID peers are alive, this broker becomes the leader.
            self.leader = self.broker_id
            logging.info(f"Broker {self.broker_id} elected as leader (no response from higher ID peers).")
            await self.announce_leader()

    async def is_alive(self, broker_id):
        """
        Checks if a broker is alive using the Membership service.
        
        :param broker_id: The ID of the broker to check.
        :return: True if the broker is alive, False otherwise.
        """
        logging.debug(f"Broker {self.broker_id}: Checking if Broker {broker_id} is alive...")
        await self.membership_service.fetch_members()  # Update the membership list
        is_alive = broker_id in self.membership_service.members
        logging.info(
            f"Broker {self.broker_id}: Broker {broker_id} is {'alive' if is_alive else 'not alive'}."
        )
        return is_alive

    async def announce_leader(self):
        """
        Notifies all peers about the newly elected leader.
        """
        logging.info(f"Broker {self.broker_id}: Announcing leader {self.leader} to peers.")
        for peer in self.peers:
            try:
                await self.send_leader_announcement(peer)
                logging.info(f"Broker {self.broker_id}: Successfully announced leader to Broker {peer}.")
            except Exception as e:
                logging.error(f"Broker {self.broker_id}: Failed to announce leader to Broker {peer}. Error: {e}")

    async def send_leader_announcement(self, peer):
        """
        Sends the leader announcement to a peer broker.
        
        :param peer: The peer broker ID.
        """
        peer_port = 3000 + int(peer) - 1  # Map broker ID to port
        url = f"http://broker-{peer}:{peer_port}/leader_announcement"
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json={"leader_id": self.leader}) as response:
                if response.status == 200:
                    logging.debug(f"Broker {self.broker_id}: Announcement sent to Broker {peer}.")
                else:
                    raise Exception(f"Failed to send leader announcement to Broker {peer}.")

