#!/usr/bin/env python3
import csv
import bcrypt
import os
from pymongo import MongoClient
from apps.exchange.settings import get_settings

def main():
    settings = get_settings()
    if settings.mongo_user:
        uri = (
            f"mongodb://{settings.mongo_user}:{settings.mongo_pass}"
            f"@{settings.mongo_host}:{settings.mongo_port}/{settings.mongo_db}?authSource=admin"
        )
    else:
        uri = f"mongodb://{settings.mongo_host}:{settings.mongo_port}/{settings.mongo_db}"

    client = MongoClient(uri)
    db = client[settings.mongo_db]
    parties_coll = db["parties"]

    csv_path = "scripts/parties.csv"
    with open(csv_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        to_insert = []

        for row in reader:
            try:
                party_id = str(row["party_id"])
            except (ValueError, KeyError):
                print(f" Skipping row with invalid or missing party_id: {row}")
                continue

            # Read party_name (free text)
            party_name = row.get("party_name", "").strip()
            if not party_name:
                print(f" Skipping row with empty party_name: {row}")
                continue

            plaintext = row.get("password", "").strip()
            if not plaintext:
                print(f" Skipping row with empty password: {row}")
                continue

            is_admin_str = row.get("is_admin", "").strip().lower()
            is_admin = is_admin_str in ("1", "true", "yes")

            hashed_bytes = bcrypt.hashpw(plaintext.encode("utf-8"), bcrypt.gensalt())
            password_hash = hashed_bytes.decode("utf-8")

            doc = {
                "party_id": party_id,
                "party_name": party_name,
                "password": password_hash,
                "is_admin": is_admin,
            }
            to_insert.append(doc)

        if not to_insert:
            print(" No valid rows found in CSV; nothing to insert.")
            return
        try:
            result = parties_coll.insert_many(to_insert)
            print(f" Inserted {len(result.inserted_ids)} party documents into '{settings.mongo_db}.parties'.")
        except Exception as e:
            print(" Error inserting documents:", str(e))
    client.close()

if __name__ == "__main__":
    main()
