# data_store.py

import json
import os

class DataStore:
    """A simple local data store for storing and retrieving messages, with persistence."""
    
    def __init__(self, filename='data_store.json'):
        self.filename = filename
        self.store = self.load_data()  # Load existing data from file, if any
    
    def load_data(self):
        """Load data from the file if it exists."""
        if os.path.exists(self.filename):
            with open(self.filename, 'r') as f:
                try:
                    return json.load(f)
                except json.JSONDecodeError:
                    # If file is empty or corrupt, return an empty store
                    return {}
        else:
            return {}  # If file doesn't exist, return an empty dictionary
    
    def save_data(self):
        """Save the current data to the file."""
        with open(self.filename, 'w') as f:
            json.dump(self.store, f, indent=4)
    
    def store_message(self, topic, message):
        """Store a message under a specific topic."""
        if topic not in self.store:
            self.store[topic] = []
        self.store[topic].append(message)
        self.save_data()  # Persist data to file after storing a message
    
    def get_messages(self, topic):
        """Retrieve all messages for a specific topic."""
        return self.store.get(topic, [])