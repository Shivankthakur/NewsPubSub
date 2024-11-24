import json
import os

class DataStore:
    """A simple local data store for storing and retrieving messages, with persistence."""
    
    def __init__(self, filename='data_store.json', dedup_filename='dedup_store.json'):
        self.filename = filename
        self.dedup_filename = dedup_filename
        self.store = self.load_data()  # Load existing data from file, if any
        self.dedup_store = self.load_dedup_data()  # Load the deduplication store
    
    def load_data(self):
        """Load data from the file if it exists."""
        if os.path.exists(self.filename):
            with open(self.filename, 'r') as f:
                try:
                    return json.load(f)
                except json.JSONDecodeError:
                    return {}
        else:
            return {}
    
    def load_dedup_data(self):
        """Load deduplication store if it exists."""
        if os.path.exists(self.dedup_filename):
            with open(self.dedup_filename, 'r') as f:
                try:
                    return set(json.load(f))  # Store as set for faster lookup
                except json.JSONDecodeError:
                    return set()
        else:
            return set()

    def save_data(self):
        """Save the current data to the file."""
        with open(self.filename, 'w') as f:
            json.dump(self.store, f, indent=4)

    def save_dedup_data(self):
        """Save the deduplication data to the file."""
        with open(self.dedup_filename, 'w') as f:
            json.dump(list(self.dedup_store), f, indent=4)

    def store_message(self, topic, message, message_id):
        """Store a message under a specific topic, with deduplication."""
        if message_id not in self.dedup_store:
            if topic not in self.store:
                self.store[topic] = []
            self.store[topic].append(message)
            self.dedup_store.add(message_id)  # Add to deduplication store
            self.save_data()  # Persist data to file after storing a message
            self.save_dedup_data()  # Persist deduplication data
            return True  # Message was stored
        else:
            return False  # Message is a duplicate

    def get_messages(self, topic):
        """Retrieve all messages for a specific topic."""
        return self.store.get(topic, [])
