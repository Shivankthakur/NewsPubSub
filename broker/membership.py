# File: membership.py

import asyncio
import logging
import aiohttp
from util import logger_config

logger_config.setup_logger()


class Membership:
    def __init__(self, broker_id, registry_url=None, update_interval=10, on_membership_change=None):
        """
        :param broker_id: ID of the current broker.
        :param registry_url: URL of the centralized registry service.
        :param update_interval: Interval in seconds to update the membership list.
        :param on_membership_change: Callback function for handling membership changes.
        """
        self.broker_id = broker_id
        self.registry_url = registry_url
        self.update_interval = update_interval
        self.members = set()  # Current membership list
        self.on_membership_change = on_membership_change  # Callback for membership updates

    async def register_broker(self):
        """Register the broker with the centralized registry."""
        if not self.registry_url:
            logging.warning("No registry URL provided; running standalone.")
            return

        try:
            logging.info(f"Registering Broker {self.broker_id} with registry at {self.registry_url}.")
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.registry_url}/register",
                    json={"broker_id": self.broker_id},
                ) as response:
                    if response.status == 200:
                        logging.info("Registration successful.")
                    else:
                        logging.warning(f"Failed to register with registry (HTTP {response.status}).")
        except Exception as e:
            logging.exception(f"Error registering with registry: {e}")

    async def fetch_members(self):
        """Fetch the latest membership list from the registry."""
        if not self.registry_url:
            logging.warning("No registry URL provided; using local peers.")
            return

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.registry_url}/members") as response:
                    if response.status == 200:
                        try:
                            members = await response.json()  # Expecting a JSON list of broker IDs
                            new_members = set(members)

                            # Update membership only if it has changed
                            if new_members != self.members:
                                self.members = new_members
                                logging.info(f"Membership updated: {self.members}")

                                # Trigger the callback if provided
                                if self.on_membership_change:
                                    await self.on_membership_change(self.members)
                            else:
                                logging.debug("No membership changes detected.")
                        except ValueError as e:
                            logging.error(f"Malformed JSON response from registry: {e}")
                    else:
                        logging.warning(f"Failed to fetch members (HTTP {response.status}).")
        except Exception as e:
            logging.exception(f"Error fetching members from registry: {e}")

    async def start_membership_service(self):
        """Start periodic membership updates."""
        # Register the broker with the registry
        await self.register_broker()

        # Start a periodic task to fetch the latest membership list
        while True:
            await self.fetch_members()
            await asyncio.sleep(self.update_interval)
