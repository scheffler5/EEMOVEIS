import os
from functools import lru_cache

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase


@lru_cache(maxsize=1)
def get_client() -> AsyncIOMotorClient:
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    return AsyncIOMotorClient(mongo_uri)


def get_database() -> AsyncIOMotorDatabase:
    database_name = os.getenv("MONGO_DATABASE", "eemoveis")
    return get_client()[database_name]
