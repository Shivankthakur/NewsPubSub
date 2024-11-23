class DataStore:
    def __init__(self):
        # Use a dictionary to store topics and their messages
        self.data = {}
    
    def store_message(self, topic, message):
        """Store a message under a topic."""
        if topic not in self.data:
            self.data[topic] = []
        self.data[topic].append(message)
    
    def get_messages(self, topic):
        """Retrieve all messages for a given topic."""
        return self.data.get(topic, [])
