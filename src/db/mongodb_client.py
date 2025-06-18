"""MongoDB connection and utilities."""

from pymongo import MongoClient
from pymongo.database import Database

from src.config import MONGO_CONFIG


class MongoDBClient:
    def __init__(self):
        self.client = MongoClient(MONGO_CONFIG["uri"])
        self.db: Database = self.client[MONGO_CONFIG["database"]]

    def get_collection(self, name: str):
        """Get a MongoDB collection."""
        return self.db[name]

    def create_indexes(self):
        """Create necessary indexes."""
        # TODO: Add indexes as needed


# Singleton instance
mongo_client = MongoDBClient()
