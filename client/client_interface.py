import aiohttp
import asyncio
import argparse

BROKER_ADDRESSES = [
    "http://127.0.0.1:3000",
    "http://127.0.0.1:3001",
    "http://127.0.0.1:3002",
    "http://127.0.0.1:3003",
    "http://127.0.0.1:3004",
    "http://127.0.0.1:3005",
    "http://127.0.0.1:3006",
    "http://127.0.0.1:3007",
    "http://127.0.0.1:3008",
    "http://127.0.0.1:3009",
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


async def subscribe_topic_adaptive(
    topic, min_interval=1, max_interval=10, default_interval=5
):
    """
    Subscribe to a topic using an adaptive polling mechanism.
    The polling interval adjusts dynamically based on message frequency.
    """
    async with aiohttp.ClientSession() as session:
        broker_url = BROKER_ADDRESSES[0]  # Choose a broker (can be randomized)
        url = f"{broker_url}/data/{topic}"

        current_interval = default_interval
        print(f"Subscribed to topic '{topic}'. Starting adaptive polling...\n")

        last_message = None  # Track the last message to detect new messages

        while True:
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        try:
                            raw_response = (
                                await response.text()
                            )  # Debugging: Log the raw response
                            messages = await response.json()
                            if (
                                isinstance(messages, dict)
                                and "messages" in messages
                                and isinstance(messages["messages"], list)
                            ):
                                latest_message = messages["messages"][
                                    -1
                                ]  # Get the latest message
                                if latest_message != last_message:
                                    print(
                                        f"New messages for topic '{topic}': {messages['messages']}"
                                    )
                                    last_message = latest_message
                                    current_interval = max(
                                        min_interval, current_interval // 2
                                    )
                                else:
                                    print(f"No new messages for topic '{topic}'")
                                    current_interval = min(
                                        max_interval, current_interval + 1
                                    )
                            else:
                                print(f"Unexpected response structure: {messages}")
                                current_interval = min(
                                    max_interval, current_interval + 1
                                )
                        except aiohttp.ContentTypeError:
                            raw_response = await response.text()
                            print(
                                f"Error: Response is not valid JSON. Raw response: {raw_response}"
                            )
                            current_interval = min(max_interval, current_interval + 1)
                    elif response.status == 204:
                        print(f"No new messages for topic '{topic}' (204 No Content)")
                        current_interval = min(max_interval, current_interval + 1)
                    else:
                        print(
                            f"Error: Received unexpected status code {response.status}"
                        )

            except aiohttp.ClientError as e:
                print(f"Connection error while polling for topic '{topic}': {e}")
                current_interval = min(max_interval, current_interval + 1)
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
                current_interval = min(max_interval, current_interval + 1)

            print(f"Polling again in {current_interval} seconds...\n")
            await asyncio.sleep(current_interval)


async def fetch_messages(topic):
    """Fetch messages for a topic from all brokers."""
    async with aiohttp.ClientSession() as session:
        for broker_url in BROKER_ADDRESSES:
            url = f"{broker_url}/data/{topic}"
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        messages = await response.json()
                        print(f"Messages from {broker_url}: {messages}")
                    else:
                        print(f"Failed to fetch from {broker_url}: {response.status}")
            except Exception as e:
                print(f"Error fetching messages from {broker_url}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Client Interface for Pub-Sub System")
    parser.add_argument(
        "--mode",
        choices=["publish", "subscribe", "fetch"],
        required=True,
        help="Mode: publish, subscribe, or fetch",
    )
    parser.add_argument("--topic", type=str, required=True, help="Topic name")
    parser.add_argument(
        "--message",
        type=str,
        required=False,
        help="Message to publish (if in publish mode)",
    )

    args = parser.parse_args()

    if args.mode == "publish":
        if not args.message:
            print("Error: You must provide a message to publish in 'publish' mode.")
        else:
            asyncio.run(publish_message(args.topic, args.message))
    elif args.mode == "subscribe":
        asyncio.run(
            subscribe_topic_adaptive(
                args.topic, min_interval=1, max_interval=10, default_interval=5
            )
        )
    elif args.mode == "fetch":
        asyncio.run(fetch_messages(args.topic))
