import sqlite3


class DataStore:
    def __init__(self, db_file="data_store.db"):
        """Initialize the SQLite database connection."""
        self.conn = sqlite3.connect(db_file)
        self.create_tables()

    def create_tables(self):
        """Create tables for storing topics and messages."""
        with self.conn:
            self.conn.execute(
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

    def store_message(self, topic, message, message_id):
        """
        Insert a message into the database under the given topic.

        Args:
            topic (str): The topic to which the message belongs.
            message (str): The message content.
            message_id (str): Unique identifier for the message.
        """
        try:
            with self.conn:
                self.conn.execute(
                    "INSERT INTO messages (message_id, topic, message) VALUES (?, ?, ?)",
                    (message_id, topic, message),
                )
        except sqlite3.IntegrityError:
            # Duplicate message_id (message already exists)
            print(f"Duplicate message detected: {message_id}")
            return False
        return True

    def get_messages(self, topic):
        """
        Retrieve all messages for the specified topic.

        Args:
            topic (str): The topic to fetch messages for.

        Returns:
            list: A list of messages for the topic.
        """
        with self.conn:
            cursor = self.conn.execute(
                "SELECT message FROM messages WHERE topic = ? ORDER BY timestamp ASC",
                (topic,),
            )
            return [row[0] for row in cursor.fetchall()]

    def close(self):
        """Close the database connection."""
        self.conn.close()
