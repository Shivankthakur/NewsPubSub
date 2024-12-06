# import sqlite3
# import os
# import time


# class DataStore:
#     """
#     A class to handle data storage and message buffering for a distributed system.
#     Supports topic-based message sharding with SQLite and in-memory buffering.
#     """

#     def __init__(self, db_dir="databases", max_size=100, ttl=3600):
#         """
#         Initialize the DataStore instance.

#         :param db_dir: Directory to store SQLite database files.
#         :param max_size: Maximum size of the in-memory message buffer per topic.
#         :param ttl: Time-to-live for messages in the buffer (in seconds).
#         """
#         self.db_dir = db_dir
#         self.max_size = max_size
#         self.ttl = ttl
#         self.message_buffer = {}  # In-memory buffer for topics: {topic: [messages]}
        
#         if not os.path.exists(db_dir):
#             os.makedirs(db_dir)  # Create the database directory if it doesn't exist

#         self.create_tables()

#     def get_db_file(self, topic):
#         """
#         Get the file path for a topic's SQLite database.

#         :param topic: The topic for which to retrieve the database file path.
#         :return: Full path to the topic's database file.
#         """
#         return os.path.join(self.db_dir, f"data_store_{topic}.db")

#     def create_tables(self):
#         """
#         Create the required tables for all existing topic databases in the directory.
#         """
#         for db_file in os.listdir(self.db_dir):
#             if db_file.endswith(".db"):
#                 conn = sqlite3.connect(os.path.join(self.db_dir, db_file))
#                 with conn:
#                     conn.execute(
#                         """
#                         CREATE TABLE IF NOT EXISTS messages (
#                             id INTEGER PRIMARY KEY AUTOINCREMENT,
#                             message_id TEXT UNIQUE NOT NULL,
#                             topic TEXT NOT NULL,
#                             message TEXT NOT NULL,
#                             timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
#                         )
#                         """
#                     )
#                 conn.close()

#     def create_topic(self, topic):
#         """
#         Create a new topic database if it does not exist.

#         :param topic: Name of the topic to create.
#         """
#         db_file = self.get_db_file(topic)
#         if not os.path.exists(db_file):
#             conn = sqlite3.connect(db_file)
#             with conn:
#                 conn.execute(
#                     """
#                     CREATE TABLE IF NOT EXISTS messages (
#                         id INTEGER PRIMARY KEY AUTOINCREMENT,
#                         message_id TEXT UNIQUE NOT NULL,
#                         topic TEXT NOT NULL,
#                         message TEXT NOT NULL,
#                         timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
#                     )
#                     """
#                 )
#             conn.close()
#             print(f"Topic '{topic}' created.")
#         else:
#             print(f"Topic '{topic}' already exists.")

#     def delete_topic(self, topic):
#         """
#         Delete a topic and its associated database.

#         :param topic: Name of the topic to delete.
#         """
#         db_file = self.get_db_file(topic)
#         if os.path.exists(db_file):
#             os.remove(db_file)
#             print(f"Topic '{topic}' deleted.")
#         else:
#             print(f"Topic '{topic}' does not exist.")

#     def store_message(self, topic, message, message_id):
#         """
#         Store a message in the SQLite database for a specific topic.

#         :param topic: Topic under which the message should be stored.
#         :param message: The message content to store.
#         :param message_id: Unique identifier for the message.
#         :return: True if the message was successfully stored, False if it's a duplicate.
#         """
#         db_file = self.get_db_file(topic)
#         conn = sqlite3.connect(db_file)
#         cursor = conn.cursor()

#         try:
#             cursor.execute(
#                 "INSERT INTO messages (message_id, topic, message) VALUES (?, ?, ?)",
#                 (message_id, topic, message),
#             )
#             conn.commit()
#             self.add_to_buffer(topic, message_id, message)
#             return True
#         except sqlite3.IntegrityError:
#             print(f"Duplicate message detected: {message_id}")
#             return False
#         finally:
#             conn.close()

#     def get_messages(self, topic, batch_size=5, start_offset=0):
#         """
#         Retrieve a batch of messages for a specific topic.

#         :param topic: Topic for which messages should be retrieved.
#         :param batch_size: Number of messages to retrieve.
#         :param start_offset: Offset for pagination.
#         :return: List of messages.
#         """
#         db_file = self.get_db_file(topic)
#         conn = sqlite3.connect(db_file)
#         cursor = conn.cursor()
#         cursor.execute(
#             """
#             SELECT message
#             FROM messages
#             WHERE topic = ?
#             ORDER BY timestamp ASC
#             LIMIT ? OFFSET ?
#             """,
#             (topic, batch_size, start_offset),
#         )
#         messages = [row[0] for row in cursor.fetchall()]
#         conn.close()
#         return messages

#     def add_to_buffer(self, topic, message_id, message):
#         """
#         Add a message to the in-memory buffer for a topic.

#         :param topic: Topic under which the message is buffered.
#         :param message_id: Unique identifier for the message.
#         :param message: The message content.
#         """
#         if topic not in self.message_buffer:
#             self.message_buffer[topic] = []

#         # Enforce buffer size limit
#         if len(self.message_buffer[topic]) >= self.max_size:
#             self.message_buffer[topic].pop(0)

#         # Add the message to the buffer
#         self.message_buffer[topic].append(
#             {
#                 "message_id": message_id,
#                 "message": message,
#                 "timestamp": time.time(),
#             }
#         )

#     def apply_retention(self):
#         """
#         Remove expired messages from the buffer based on TTL.
#         """
#         current_time = time.time()
#         for topic in self.message_buffer:
#             self.message_buffer[topic] = [
#                 msg
#                 for msg in self.message_buffer[topic]
#                 if current_time - msg["timestamp"] < self.ttl
#             ]

#     def close(self):
#         """
#         Placeholder for cleanup operations (if needed).
#         """
#         pass
