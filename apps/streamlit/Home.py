"""HOMEPEDIA — application shell. Phase 1 shows platform status; pages come in Phase 7."""

from __future__ import annotations

import streamlit as st

from homepedia.healthcheck import run_checks
from homepedia.settings import get_settings

st.set_page_config(page_title="HOMEPEDIA", page_icon="🏠", layout="wide")
st.title("HOMEPEDIA")
st.caption("French housing market — DataOps · Big Data · Geospatial · AI")

st.subheader("Platform status")
settings = get_settings()
for result in run_checks(settings):
    (st.success if result.ok else st.error)(f"**{result.name}** — {result.detail}")

st.subheader("Configuration")
st.table(
    {
        "Spark master": [settings.spark_master_url],
        "PostgreSQL": [f"{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}"],
        "MongoDB": [f"{settings.mongo_host}:{settings.mongo_port}/{settings.mongo_db}"],
        "Data lake": [str(settings.data_lake_root)],
    }
)
