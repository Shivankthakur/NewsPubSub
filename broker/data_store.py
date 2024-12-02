import sqlite3
import os
import time


class DataStore:
    def __init__(self, db_dir="databases", max_size=100, ttl=3600):
        """Initialize the SQLite database connection and message buffer."""
        self.db_dir = db_dir
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)  # Create directory for databases
        self.create_tables()
        self.message_buffer = {}  # topic -> message buffer
        self.max_size = max_size  # Max size for each topic's buffer
        self.ttl = ttl  # Time-to-live for messages in seconds

    def get_db_file(self, topic):
        """Return the appropriate database file path based on topic."""
        return os.path.join(self.db_dir, f"data_store_{topic}.db")

    def create_tables(self):
        """Create tables for storing topics and messages."""
        for db_file in os.listdir(self.db_dir):
            if db_file.endswith(".db"):
                conn = sqlite3.connect(os.path.join(self.db_dir, db_file))
                with conn:
                    conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS messages (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            message_id TEXT UNIQUE NOT NULL,
                            topic TEXT NOT NULL,
                            message TEXT NOT NULL,
                            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                        )
                    """
                    )
                conn.close()

    def create_topic(self, topic):
        """Create a new topic and initialize a database for it."""
        db_file = self.get_db_file(topic)
        if not os.path.exists(db_file):
            conn = sqlite3.connect(db_file)
            with conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS messages (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        message_id TEXT UNIQUE NOT NULL,
                        topic TEXT NOT NULL,
                        message TEXT NOT NULL,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """
                )
            conn.close()
            print(f"Topic '{topic}' created.")
        else:
            print(f"Topic '{topic}' already exists.")

    def delete_topic(self, topic):
        """Delete a topic and all associated messages."""
        db_file = self.get_db_file(topic)
        if os.path.exists(db_file):
            os.remove(db_file)
            print(f"Topic '{topic}' deleted.")
        else:
            print(f"Topic '{topic}' does not exist.")

    def store_message(self, topic, message, message_id):
        """
        Insert a message into the database under the given topic.
        For scalability, messages are routed to different databases based on topic.
        """
        db_file = self.get_db_file(topic)  # Use topic-based database sharding
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        try:
            cursor.execute(
                "INSERT INTO messages (message_id, topic, message) VALUES (?, ?, ?)",
                (message_id, topic, message),
            )
            conn.commit()
            self.add_to_buffer(topic, message_id, message)
            return True
        except sqlite3.IntegrityError:
            # Duplicate message_id (message already exists)
            print(f"Duplicate message detected: {message_id}")
            return False
        finally:
            conn.close()

    def get_messages(self, topic, batch_size=5, start_offset=0):
        """Retrieve a batch of messages for the specified topic."""
        db_file = self.get_db_file(topic)
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT message FROM messages WHERE topic = ? ORDER BY timestamp ASC LIMIT ? OFFSET ?",
            (topic, batch_size, start_offset),
        )
        messages = [row[0] for row in cursor.fetchall()]
        conn.close()
        return messages

    def add_to_buffer(self, topic, message_id, message):
        """Add a message to the buffer with TTL (time-to-live) and size limits."""
        if topic not in self.message_buffer:
            self.message_buffer[topic] = []

        # Apply retention policy (buffer size and TTL)
        if len(self.message_buffer[topic]) >= self.max_size:
            self.message_buffer[topic].pop(0)  # Remove oldest message

        self.message_buffer[topic].append(
            {
                "message_id": message_id,
                "message": message,
                "timestamp": time.time(),  # Store the timestamp for TTL
            }
        )

    def apply_retention(self):
        """Remove expired messages based on TTL."""
        current_time = time.time()
        for topic in self.message_buffer:
            self.message_buffer[topic] = [
                msg
                for msg in self.message_buffer[topic]
                if current_time - msg["timestamp"] < self.ttl
            ]

    def close(self):
        """Close the database connection."""
        pass
