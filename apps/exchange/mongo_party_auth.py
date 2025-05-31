# apps/exchange/mongo_party_auth.py

from __future__ import annotations
import logging
from typing import Dict

import bcrypt
from motor.motor_asyncio import AsyncIOMotorClient
from apps.exchange.settings import get_settings

SET = get_settings()
log = logging.getLogger("PartyAuth")


class MongoPartyAuth:
    """
    Verifies party credentials against a MongoDB collection.
    Passwords must be stored as bcrypt hashes in the "password" field.
    """

    _cache: Dict[int, str] | None = None  # Maps party_id -> bcrypt hash (UTF-8 string)

    @classmethod
    async def _load(cls) -> None:
        """
        Populate the in-memory cache by loading all party_id -> password_hash
        entries from the "parties" collection. Assumes each document has:
            {
                "party_id": <int>,
                "password": <bcrypt_hash_string>
                // ... other fields ...
            }
        """
        if cls._cache is not None:
            return  # Already loaded

        uri = f"mongodb://{SET.mongo_host}:{SET.mongo_port}/{SET.mongo_db}"
        client = AsyncIOMotorClient(uri)
        collection = client[SET.mongo_db]["parties"]

        temp_cache: Dict[int, str] = {}
        async for doc in collection.find({}, {"_id": 0, "party_id": 1, "password": 1}):
            pid = int(doc["party_id"])
            pwd_hash = doc.get("password", "")
            if isinstance(pwd_hash, bytes):
                # Sometimes you might store raw bytes; convert to UTF-8 string
                try:
                    pwd_hash = pwd_hash.decode("utf-8")
                except Exception:
                    log.warning("Could not decode password hash for party %d", pid)
                    pwd_hash = ""
            temp_cache[pid] = pwd_hash

        cls._cache = temp_cache
        log.info("Loaded %d parties into cache", len(cls._cache))

    @classmethod
    async def verify(cls, party_id: int, password: str) -> bool:
        """
        Verify that the provided plain-text password matches the stored bcrypt hash.
        Returns True if match, False otherwise.
        """
        # Ensure cache is loaded
        await cls._load()

        # 1) Check in‐memory cache first
        stored_hash = cls._cache.get(party_id)
        if stored_hash:
            try:
                # bcrypt.checkpw expects both arguments as bytes
                if isinstance(stored_hash, str):
                    stored_hash_bytes = stored_hash.encode("utf-8")
                else:
                    stored_hash_bytes = stored_hash
                return bcrypt.checkpw(password.encode("utf-8"), stored_hash_bytes)
            except (ValueError, bcrypt.errors.InvalidHash) as e:
                log.error("Invalid bcrypt hash for party_id %d: %s", party_id, e)
                return False

        # 2) If not cached, query MongoDB directly
        log.info("Party %d not in cache; querying MongoDB", party_id)
        uri = f"mongodb://{SET.mongo_host}:{SET.mongo_port}/{SET.mongo_db}"
        client = AsyncIOMotorClient(uri)
        doc = await client[SET.mongo_db]["parties"].find_one(
            {"party_id": party_id},
            {"_id": 0, "password": 1}
        )

        if doc and "password" in doc:
            pwd_hash = doc["password"]
            if isinstance(pwd_hash, bytes):
                try:
                    pwd_hash_bytes = pwd_hash
                    pwd_hash = pwd_hash.decode("utf-8")
                except Exception:
                    log.warning("Could not decode fetched password hash for party %d", party_id)
                    return False
            else:
                pwd_hash_bytes = pwd_hash.encode("utf-8")

            # Update cache for future lookups
            if cls._cache is not None:
                cls._cache[party_id] = pwd_hash

            try:
                return bcrypt.checkpw(password.encode("utf-8"), pwd_hash_bytes)
            except (ValueError, bcrypt.errors.InvalidHash) as e:
                log.error("Invalid bcrypt hash for party_id %d: %s", party_id, e)
                return False

        log.warning("Party %d not found in MongoDB", party_id)
        return False
