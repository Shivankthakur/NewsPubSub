import aiohttp
import asyncio
import argparse

BROKER_ADDRESSES = [
    "http://127.0.0.1:3000",
    "http://127.0.0.1:3001",
    "http://127.0.0.1:3002"
]

async def publish_message(topic, message):
    """Publish a message to a topic by sending it to one of the brokers."""
    async with aiohttp.ClientSession() as session:
        # Choose a random broker to publish to
        broker_url = BROKER_ADDRESSES[0]  # Or implement a load-balancing strategy
        url = f"{broker_url}/publish"
        data = {"topic": topic, "message": message}
        
        async with session.post(url, json=data) as response:
            print(f"Response from broker: {await response.text()}")

async def subscribe_topic(topic):
    """Subscribe to a topic by polling one of the brokers."""
    async with aiohttp.ClientSession() as session:
        # Choose a random broker to get data from
        broker_url = BROKER_ADDRESSES[0]  # Or implement a load-balancing strategy
        url = f"{broker_url}/data/{topic}"

        while True:
            async with session.get(url) as response:
                messages = await response.json()
                print(f"Messages for topic '{topic}': {messages['messages']}")
            
            # Poll every few seconds
            await asyncio.sleep(5)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Client Interface for Pub-Sub System")
    parser.add_argument('--mode', choices=['publish', 'subscribe'], required=True, help="Mode: publish or subscribe")
    parser.add_argument('--topic', type=str, required=True, help="Topic name")
    parser.add_argument('--message', type=str, required=False, help="Message to publish (if in publish mode)")

    args = parser.parse_args()

    if args.mode == 'publish':
        asyncio.run(publish_message(args.topic, args.message))
    elif args.mode == 'subscribe':
        asyncio.run(subscribe_topic(args.topic))
