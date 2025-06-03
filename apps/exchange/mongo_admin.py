# apps/exchange/mongo_admin.py
from __future__ import annotations
import logging
from typing import Any, Dict, List, Optional

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import OperationFailure
from apps.exchange.settings import get_settings

SET = get_settings()
log = logging.getLogger("MongoAdmin")


def _admin_uri() -> str:
    if SET.mongo_user and SET.mongo_pass:
        return f"mongodb://{SET.mongo_user}:{SET.mongo_pass}@{SET.mongo_host}:{SET.mongo_port}/admin"
    else:
        return f"mongodb://{SET.mongo_host}:{SET.mongo_port}/admin"


class MongoAdmin:
    def __init__(self, admin_uri: Optional[str] = None) -> None:
        uri = admin_uri if admin_uri is not None else _admin_uri()
        self._client = AsyncIOMotorClient(uri)
        self._admin_db = self._client.get_database("admin")
        log.info("MongoAdmin connected to %s", uri)

    async def create_user(
        self,
        username: str,
        password: str,
        roles: List[Dict[str, Any]],
        *,
        database: str = "admin"
    ) -> None:
        try:
            await self._admin_db.command(
                "createUser",
                username,
                pwd=password,
                roles=roles,
                writeConcern={"w": "majority"}
            )
            log.info("Created user '%s' on db '%s' with roles %s", username, database, roles)
        except OperationFailure as e:
            log.error("Failed to create user %s: %s", username, e)
            raise

    async def drop_user(self, username: str, *, database: str = "admin") -> None:
        try:
            await self._client.get_database(database).command("dropUser", username)
            log.info("Dropped user '%s' from db '%s'", username, database)
        except OperationFailure as e:
            log.error("Failed to drop user %s: %s", username, e)
            raise

    async def list_users(self, *, database: str = "admin") -> List[Dict[str, Any]]:
        try:
            result = await self._client.get_database(database).command("usersInfo")
            # result is like { "users": [ {userDoc}, ... ], "ok": 1 }
            users = result.get("users", [])
            log.info("Listing %d users in db '%s'", len(users), database)
            return users
        except OperationFailure as e:
            log.error("Failed to list users on db %s: %s", database, e)
            raise

    async def create_database(self, db_name: str) -> None:
        try:
            db = self._client.get_database(db_name)
            # Insert into a dummy, ephemeral collection named "__init__"
            await db.get_collection("__init__").insert_one({"_created": True})
            log.info("Ensured database '%s' exists (wrote dummy document)", db_name)
        except OperationFailure as e:
            log.error("Failed to create database %s: %s", db_name, e)
            raise

    async def list_databases(self) -> List[str]:
        names = await self._client.list_database_names()
        log.info("Databases on server: %s", names)
        return names

    def get_client_for_db(self, db_name: str) -> AsyncIOMotorClient:
        if SET.mongo_user and SET.mongo_pass:
            # Build a URI that authenticates against the admin DB
            auth = f"{SET.mongo_user}:{SET.mongo_pass}@"
            uri = (
                f"mongodb://{auth}"
                f"{SET.mongo_host}:{SET.mongo_port}/{db_name}"
                f"?authSource=admin"
            )
        else:
            uri = f"mongodb://{SET.mongo_host}:{SET.mongo_port}/{db_name}"

        log.info("Opened new client for DB '%s' via URI %s", db_name, uri)
        return AsyncIOMotorClient(uri)

    async def drop_database(self, db_name: str) -> None:
        try:
            await self._client.drop_database(db_name)
            log.info("Dropped database '%s'", db_name)
        except OperationFailure as e:
            log.error("Failed to drop database %s: %s", db_name, e)
            raise
