"""SILVER texts → MongoDB `reviews` (section 8 document shape). `python -m database.load_mongo`."""

from __future__ import annotations

import hashlib
import sys
import time

import pyarrow.parquet as pq
from pymongo import UpdateOne

from homepedia.db.mongo import get_database
from homepedia.ledger import record_run
from homepedia.logging import configure_logging, get_logger
from homepedia.settings import Settings, get_settings

log = get_logger("load_mongo")
SOURCE = "grand_debat"


def documents(settings: Settings) -> list[dict[str, object]]:
    rows = pq.read_table(settings.data_lake_root / "silver" / "meeting_reports").to_pylist()
    docs = []
    for r in rows:
        key = f"{r['created_at']}|{r['title']}|{r['postal_code']}"
        docs.append(
            {
                "_id": f"{SOURCE}:{hashlib.sha1(key.encode()).hexdigest()[:16]}",  # noqa: S324 — id, not security
                "source": SOURCE,
                "territory": {
                    "commune_code": r["commune_code"],
                    "department_code": r["department_code"],
                    "region_code": r["region_code"],
                },
                "text": r["text"],
                "language": "fr",
                "title": r["title"],
                "themes": r["themes"] or [],
                "created_at": r["created_at"],
                "postal_code": r["postal_code"],
                "town": r["town"],
            }
        )
    return docs


def main() -> int:
    settings = get_settings()
    configure_logging(settings.log_level)
    t0 = time.perf_counter()
    with record_run(settings, "load.reviews", settings.pipeline_version, "silver") as st:
        docs = documents(settings)
        st.rows_received = len(docs)
        if docs:
            res = get_database(settings).reviews.bulk_write(
                [UpdateOne({"_id": d["_id"]}, {"$set": d}, upsert=True) for d in docs], ordered=False
            )
            st.rows_processed = res.upserted_count + res.matched_count
    log.info(
        "loaded",
        extra={
            "extra": {
                "collection": "reviews",
                "rows": st.rows_processed,
                "s": round(time.perf_counter() - t0, 1),
            }
        },
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
