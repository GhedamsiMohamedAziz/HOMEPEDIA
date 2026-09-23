"""Typed runtime configuration. Single source of truth for connection strings."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    postgres_host: str = "localhost"
    postgres_port: int = Field(default=5432, ge=1, le=65535)
    postgres_db: str = "homepedia"
    postgres_user: str = "homepedia"
    postgres_password: SecretStr = SecretStr("change-me")

    mongo_host: str = "localhost"
    mongo_port: int = Field(default=27017, ge=1, le=65535)
    mongo_db: str = "homepedia"
    mongo_user: str = "homepedia"
    mongo_password: SecretStr = SecretStr("change-me")

    spark_master_url: str = "local[*]"
    spark_app_name: str = "homepedia"

    data_lake_root: Path = Path("./data")
    log_level: str = "INFO"

    # ingestion scope (section 30: build on a few departments first, widen later)
    departments: list[str] = ["33", "31", "44"]
    dvf_years: list[int] = [2023, 2024]
    dpe_since: str = "2024-01-01"
    http_timeout: int = Field(default=120, ge=1)
    pipeline_version: str = "0.1.0"

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password.get_secret_value()}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def mongo_uri(self) -> str:
        return (
            f"mongodb://{self.mongo_user}:{self.mongo_password.get_secret_value()}"
            f"@{self.mongo_host}:{self.mongo_port}/{self.mongo_db}?authSource=admin"
        )

    def lake_path(self, layer: str) -> Path:
        """RAW / BRONZE / SILVER / GOLD layer directory (section 9 of the plan)."""
        if layer not in {"raw", "bronze", "silver", "gold"}:
            raise ValueError(f"unknown data lake layer: {layer!r}")
        return self.data_lake_root / layer


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
