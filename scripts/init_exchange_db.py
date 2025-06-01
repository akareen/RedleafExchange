#!/usr/bin/env python3
import sys
import pathlib

# ─────────────────────────────────────────────────────────────────────────
# Ensure the project root is on Python’s import path so that `apps/...` works.
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
# ─────────────────────────────────────────────────────────────────────────

import asyncio
import logging
from typing import List

from pymongo.errors import OperationFailure

from apps.exchange.mongo_admin import MongoAdmin
from apps.exchange.settings import get_settings

LOG = logging.getLogger("InitExchangeDB")
SET = get_settings()


async def ensure_collections_for_instruments(
    instrument_ids: List[int],
    admin: MongoAdmin,
) -> None:
    """
    For each instrument_id in instrument_ids,
      1) Issue createIndexes via the authenticated admin client against `exchange`:
         – orders_<instr>      (unique index on 'order_id')
         – trades_<instr>      (index on 'timestamp')
         – live_orders_<instr> (unique index on 'order_id')
    """
    # Use the already-authenticated client inside `admin` to act on the "exchange" DB
    admin_client = admin._client
    db = admin_client.get_database(SET.mongo_db)

    for instr in instrument_ids:
        LOG.info("Ensuring collections for instrument %d …", instr)

        # orders_<instr> → unique index on order_id
        try:
            await db.command({
                "createIndexes": f"orders_{instr}",
                "indexes": [
                    {"key": {"order_id": 1}, "name": "pk_order_id", "unique": True}
                ],
            })
            LOG.info("  → orders_%d OK (unique index on order_id)", instr)
        except OperationFailure as e:
            LOG.error("  ✗ failed to create index on orders_%d: %s", instr, e)

        # trades_<instr> → index on timestamp
        try:
            await db.command({
                "createIndexes": f"trades_{instr}",
                "indexes": [
                    {"key": {"timestamp": 1}, "name": "idx_timestamp"}
                ],
            })
            LOG.info("  → trades_%d OK (index on timestamp)", instr)
        except OperationFailure as e:
            LOG.error("  ✗ failed to create index on trades_%d: %s", instr, e)

        # live_orders_<instr> → unique index on order_id
        try:
            await db.command({
                "createIndexes": f"live_orders_{instr}",
                "indexes": [
                    {"key": {"order_id": 1}, "name": "pk_live_order_id", "unique": True}
                ],
            })
            LOG.info("  → live_orders_%d OK (unique index on order_id)", instr)
        except OperationFailure as e:
            LOG.error("  ✗ failed to create index on live_orders_%d: %s", instr, e)

    LOG.info("All requested instrument collections have been ensured.")


async def main():
    # 1) Which instrument IDs to bootstrap (modify as needed)
    to_create = [1, 2, 3, 4, 5]

    # 2) Instantiate MongoAdmin (reads MONGO_USER, MONGO_PASS, etc.)
    admin = MongoAdmin()
    LOG.info("Connected as MongoAdmin to create exchange collections.")

    # 3) Create the collections + indexes on the “exchange” database
    await ensure_collections_for_instruments(to_create, admin)

    # 4) Clean up and exit
    admin._client.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z"
    )
    asyncio.run(main())
