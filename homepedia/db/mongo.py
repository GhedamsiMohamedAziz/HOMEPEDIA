"""MongoDB access for raw documents, reviews and NLP outputs (section 8)."""

from __future__ import annotations

from typing import Any

from pymongo import MongoClient
from pymongo.database import Database

from homepedia.settings import Settings, get_settings

REQUIRED_COLLECTIONS = ("raw_documents", "reviews", "nlp_outputs")


def get_client(settings: Settings | None = None) -> MongoClient[dict[str, Any]]:
    s = settings or get_settings()
    return MongoClient(s.mongo_uri, serverSelectionTimeoutMS=5000)


def get_database(settings: Settings | None = None) -> Database[dict[str, Any]]:
    s = settings or get_settings()
    return get_client(s)[s.mongo_db]


def missing_collections(settings: Settings | None = None) -> list[str]:
    existing = set(get_database(settings).list_collection_names())
    return [c for c in REQUIRED_COLLECTIONS if c not in existing]
