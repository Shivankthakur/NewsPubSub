import asyncio
import logging

class LeaderElection:
    def __init__(self, broker_id, peers):
        self.broker_id = int(broker_id)
        self.peers = [int(peer) for peer in peers]
        self.leader = None
        self.election_timeout = 5  # seconds

    async def start_leader_election(self, _):
        """Initiate leader election upon startup."""
        if self.leader is None or self.leader not in self.peers:
            await self.elect_leader()
    
    async def elect_leader(self):
        """Simple Bully Election Algorithm."""
        higher_ids = [peer for peer in self.peers if peer > self.broker_id]
        if not higher_ids:
            # This broker becomes the leader
            self.leader = self.broker_id
            logging.info(f"Broker {self.broker_id} elected as leader.")
        else:
            # Ask higher IDs if they are alive
            # Simulated election message to higher ID brokers
            for peer in higher_ids:
                if await self.is_alive(peer):
                    return
        
            # If no higher IDs are alive, become the leader
            self.leader = self.broker_id
            logging.info(f"Broker {self.broker_id} elected as leader.")
    
    async def is_alive(self, broker_id):
        """Check if a higher ID broker is alive (simulated check)."""
        # In a real scenario, this would send an actual message
        # Here, it's simulated with a timeout.
        await asyncio.sleep(0.5)
        return False
