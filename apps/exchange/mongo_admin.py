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
    """
    Build a URI that connects as the “root” or “admin” user.
    You must have created an admin user in Mongo with the correct role.
    """
    if SET.mongo_user and SET.mongo_pass:
        return f"mongodb://{SET.mongo_user}:{SET.mongo_pass}@{SET.mongo_host}:{SET.mongo_port}/admin"
    else:
        # if no auth is configured, assume localhost without credentials
        return f"mongodb://{SET.mongo_host}:{SET.mongo_port}/admin"


class MongoAdmin:
    """
    A utility class for admin‐level operations on MongoDB:
      • create_user / drop_user / list_users
      • create_database (by writing an empty document)
      • get_client_for_db to open a new AsyncIOMotorClient pointed at a given DB

    All methods are async, so you can call them from an async context (e.g. startup scripts).
    """

    def __init__(self, admin_uri: Optional[str] = None) -> None:
        """
        Connect to the “admin” database using either:
          • the provided URI string, or
          • the URI built from settings (mongo_user / mongo_pass).
        """
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
        """
        Create a new user in the specified database (defaults to 'admin').
        `roles` is a list of { "role": "<roleName>", "db": "<dbName>" } dicts.
        Example:
            roles=[{"role":"readWrite", "db":"exchange"}, {"role":"dbAdmin", "db":"exchange"}]
        """
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
        """
        Drop an existing user from the specified database.
        """
        try:
            await self._client.get_database(database).command("dropUser", username)
            log.info("Dropped user '%s' from db '%s'", username, database)
        except OperationFailure as e:
            log.error("Failed to drop user %s: %s", username, e)
            raise

    async def list_users(self, *, database: str = "admin") -> List[Dict[str, Any]]:
        """
        Return a list of all users in the specified database (default: 'admin').
        """
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
        """
        “Create” a database by inserting an empty document into a dummy collection.
        MongoDB lazily creates databases; writing at least one doc forces creation.
        """
        try:
            db = self._client.get_database(db_name)
            # Insert into a dummy, ephemeral collection named "__init__"
            await db.get_collection("__init__").insert_one({"_created": True})
            log.info("Ensured database '%s' exists (wrote dummy document)", db_name)
        except OperationFailure as e:
            log.error("Failed to create database %s: %s", db_name, e)
            raise

    async def list_databases(self) -> List[str]:
        """
        Return a list of all database names on this Mongo instance.
        """
        names = await self._client.list_database_names()
        log.info("Databases on server: %s", names)
        return names

    def get_client_for_db(self, db_name: str) -> AsyncIOMotorClient:
        """
        Return a new AsyncIOMotorClient pointed at `db_name`, using the same
        MONGO_USER/MONGO_PASS as admin, with authSource=admin.
        """
        if SET.mongo_user and SET.mongo_pass:
            # Build a URI that authenticates against the admin DB
            auth = f"{SET.mongo_user}:{SET.mongo_pass}@"
            uri = (
                f"mongodb://{auth}"
                f"{SET.mongo_host}:{SET.mongo_port}/{db_name}"
                f"?authSource=admin"
            )
        else:
            # No auth: connect directly
            uri = f"mongodb://{SET.mongo_host}:{SET.mongo_port}/{db_name}"

        log.info("Opened new client for DB '%s' via URI %s", db_name, uri)
        return AsyncIOMotorClient(uri)

    async def drop_database(self, db_name: str) -> None:
        """
        Drop (delete) the given database entirely. **USE CAUTION**.
        """
        try:
            await self._client.drop_database(db_name)
            log.info("Dropped database '%s'", db_name)
        except OperationFailure as e:
            log.error("Failed to drop database %s: %s", db_name, e)
            raise
