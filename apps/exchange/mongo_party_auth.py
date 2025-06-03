# apps/exchange/mongo_party_auth.py

import logging
from fastapi import Body, Request, HTTPException, status
import bcrypt
from motor.motor_asyncio import AsyncIOMotorClient
from apps.exchange.settings import get_settings

SET = get_settings()
log = logging.getLogger("PartyAuth")


def _build_auth_uri(db_name: str) -> str:
    if SET.mongo_user and SET.mongo_pass:
        user = SET.mongo_user
        pw   = SET.mongo_pass
        host = SET.mongo_host
        port = SET.mongo_port
        # authSource=admin ensures we authenticate against the admin DB
        return f"mongodb://{user}:{pw}@{host}:{port}/{db_name}?authSource=admin"
    else:
        return f"mongodb://{SET.mongo_host}:{SET.mongo_port}/{db_name}"


class MongoPartyAuth:
    _cache: dict[str, str] | None = None

    @classmethod
    async def _load(cls) -> None:
        if cls._cache is not None:
            return

        uri = _build_auth_uri(SET.mongo_db)
        client = AsyncIOMotorClient(uri)
        collection = client[SET.mongo_db]["parties"]

        temp: dict[str, str] = {}
        async for doc in collection.find({}, {"_id": 0, "party_id": 1, "password": 1}):
            pid = str(doc["party_id"])
            pwd_hash = doc.get("password", "")
            if isinstance(pwd_hash, bytes):
                try:
                    pwd_hash = pwd_hash.decode("utf-8")
                except Exception:
                    log.warning("Could not decode password hash for party %d", pid)
                    pwd_hash = ""
            temp[pid] = pwd_hash

        cls._cache = temp
        log.info("Loaded %d parties into cache", len(cls._cache))
        client.close()

    @classmethod
    async def verify(cls, party_id: str, password: str) -> bool:
        await cls._load()

        stored_hash = cls._cache.get(party_id)
        if stored_hash:
            try:
                stored_bytes = stored_hash.encode("utf-8")
                return bcrypt.checkpw(password.encode("utf-8"), stored_bytes)
            except Exception as e:
                log.error("Invalid bcrypt hash for party_id %d: %s", party_id, e)
                return False

        log.info("Party %d not in cache; querying MongoDB", party_id)
        uri = _build_auth_uri(SET.mongo_db)
        client = AsyncIOMotorClient(uri)
        collection = client[SET.mongo_db]["parties"]

        doc = await collection.find_one(
            {"party_id": party_id},
            {"_id": 0, "password": 1}
        )

        if doc and "password" in doc:
            pwd_hash = doc["password"]
            if isinstance(pwd_hash, bytes):
                try:
                    pwd_hash = pwd_hash.decode("utf-8")
                except Exception:
                    log.warning("Could not decode fetched password hash for party %d", party_id)
                    client.close()
                    return False
            else:
                pwd_hash = pwd_hash

            if cls._cache is not None:
                cls._cache[party_id] = pwd_hash

            try:
                result = bcrypt.checkpw(password.encode("utf-8"), pwd_hash.encode("utf-8"))
                client.close()
                return result
            except Exception as e:
                log.error("Invalid bcrypt hash for party_id %d: %s", party_id, e)
                client.close()
                return False

        log.warning("Party %d not found in MongoDB", party_id)
        client.close()
        return False

    @classmethod
    async def get(cls, party_id: str) -> dict | None:
        uri = _build_auth_uri(SET.mongo_db)
        client = AsyncIOMotorClient(uri)
        collection = client[SET.mongo_db]["parties"]

        doc = await collection.find_one(
            {"party_id": party_id},
            {"_id": 0}
        )
        client.close()
        return doc


class Auth:
    def __init__(self, require_admin: bool = False):
        self.require_admin = require_admin

    async def __call__(self, request: Request, payload: dict = Body(...)) -> dict:
        raw = payload.get("party_id")
        try:
            pid = str(raw) if raw is not None else 0
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="party_id must be an integer"
            )
        pwd = payload.get("password", "")

        if not await MongoPartyAuth.verify(pid, pwd):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="invalid party_id / password"
            )

        party_doc = await MongoPartyAuth.get(pid)
        if not party_doc:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="party not found"
            )

        if self.require_admin and not party_doc.get("is_admin", False):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="admin privileges required"
            )

        request.state.party = party_doc
        return payload
