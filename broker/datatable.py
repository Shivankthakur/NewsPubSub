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
        self.conn = sqlite3.connect(
            db_file, check_same_thread=False
        )  # Enable multi-threaded access
        self.conn.row_factory = sqlite3.Row

    def create_topic_table(self, topic):
        """
        Create a table dynamically for the specified topic if it does not exist.

        :param topic: The topic name for the new table.
        """
        table_name = self._sanitize_table_name(topic)
        with self.conn:
            self.conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id TEXT UNIQUE NOT NULL,
                    message TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        print(f"Table '{table_name}' created or already exists.")

    def store_message(self, topic, message, message_id):
        """
        Insert a message into the database under the specified topic (table).

        :param topic: Topic to which the message belongs (table name).
        :param message: The content of the message.
        :param message_id: Unique identifier for the message.
        :return: True if the message was successfully stored, False otherwise.
        """
        table_name = self._sanitize_table_name(topic)
        self.create_topic_table(topic)  # Ensure the table exists
        try:
            with self.conn:
                self.conn.execute(
                    f"INSERT INTO {table_name} (message_id, message) VALUES (?, ?)",
                    (message_id, message),
                )
            print(f"Message stored successfully: {topic} -> {message}")
            return True
        except sqlite3.IntegrityError:
            print(f"Duplicate message detected: {message_id}")
            return False

    def get_messages(self, topic, batch_size=5, start_offset=0):
        """
        Retrieve messages for a specific topic (table) with pagination support.

        :param topic: Topic (table name) to fetch messages for.
        :param batch_size: Number of messages to retrieve in a single batch.
        :param start_offset: Offset for pagination (default is 0).
        :return: A list of messages for the topic.
        """
        table_name = self._sanitize_table_name(topic)
        try:
            with self.conn:
                cursor = self.conn.execute(
                    f"""
                    SELECT message
                    FROM {table_name}
                    ORDER BY timestamp ASC
                    LIMIT ? OFFSET ?
                    """,
                    (batch_size, start_offset),
                )
                messages = [row["message"] for row in cursor.fetchall()]
                print(f"Fetched messages for topic '{topic}': {messages}")
                return messages
        except sqlite3.OperationalError:
            print(f"Topic '{topic}' does not exist.")
            return []

    def delete_topic(self, topic):
        """
        Drop the table for the specified topic.

        :param topic: The topic (table name) to delete.
        """
        table_name = self._sanitize_table_name(topic)
        with self.conn:
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        print(f"Table for topic '{topic}' has been deleted.")

    def close(self):
        """
        Close the database connection.
        """
        self.conn.close()

    @staticmethod
    def _sanitize_table_name(topic):
        """
        Sanitize the topic name to ensure it can be safely used as a table name.
        """
        return topic.replace(" ", "_").replace("-", "_").lower()


# # Example usage:
# if __name__ == "__main__":
#     dt = DataTable()
#     dt.store_message("news", "Breaking News: Sample message!", "msg-001")
#     dt.store_message("news", "Another News: Sample message 2!", "msg-002")
#     print("Messages for 'news':", dt.get_messages("news"))
#     dt.delete_topic("news")
#     dt.close()
