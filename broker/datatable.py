import sqlite3


class DataStore:
    """
    A lightweight class to manage message storage and retrieval for a single SQLite database.
    """

    def __init__(self, db_file="data_store.db"):
        """
        Initialize the SQLite database connection.

        :param db_file: File path for the SQLite database.
        """
        self.db_file = db_file
        self.conn = sqlite3.connect(db_file, check_same_thread=False)  # Enable multi-threaded access
        self.create_tables()

    def create_tables(self):
        """
        Create a table for storing messages if it does not exist.
        """
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
        Insert a message into the database under the specified topic.

        :param topic: Topic to which the message belongs.
        :param message: The content of the message.
        :param message_id: Unique identifier for the message.
        :return: True if the message was successfully stored, False otherwise.
        """
        try:
            with self.conn:
                self.conn.execute(
                    "INSERT INTO messages (message_id, topic, message) VALUES (?, ?, ?)",
                    (message_id, topic, message),
                )
            return True
        except sqlite3.IntegrityError:
            print(f"Duplicate message detected: {message_id}")
            return False

    def get_messages(self, topic, batch_size=5, start_offset=0):
        """
        Retrieve messages for a specific topic with pagination support.

        :param topic: Topic to fetch messages for.
        :param batch_size: Number of messages to retrieve in a single batch.
        :param start_offset: Offset for pagination (default is 0).
        :return: A list of messages for the topic.
        """
        with self.conn:
            cursor = self.conn.execute(
                """
                SELECT message
                FROM messages
                WHERE topic = ?
                ORDER BY timestamp ASC
                LIMIT ? OFFSET ?
                """,
                (topic, batch_size, start_offset),
            )
            return [row[0] for row in cursor.fetchall()]

    def delete_topic(self, topic):
        """
        Delete all messages under the specified topic.

        :param topic: The topic to delete.
        """
        with self.conn:
            self.conn.execute(
                "DELETE FROM messages WHERE topic = ?", (topic,)
            )
        print(f"All messages under topic '{topic}' have been deleted.")

    def close(self):
        """
        Close the database connection.
        """
        self.conn.close()


# # Example usage:
# if __name__ == "__main__":
#     dt = DataTable()
#     dt.store_message("news", "Breaking News: Sample message!", "msg-001")
#     dt.store_message("news", "Another News: Sample message 2!", "msg-002")
#     print("Messages for 'news':", dt.get_messages("news"))
#     dt.delete_topic("news")
#     dt.close()
