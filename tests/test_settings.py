from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import SecretStr

from homepedia.settings import Settings


def test_dsn_and_uri_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_HOST", "db")
    monkeypatch.setenv("POSTGRES_PASSWORD", "s3cret")
    monkeypatch.setenv("MONGO_PORT", "27018")
    s = Settings(_env_file=None)  # type: ignore[call-arg]
    assert s.postgres_dsn == "postgresql://homepedia:s3cret@db:5432/homepedia"
    assert s.mongo_uri.endswith("@localhost:27018/homepedia?authSource=admin")


def test_secret_not_leaked_in_repr() -> None:
    s = Settings(_env_file=None, postgres_password=SecretStr("topsecret"))  # type: ignore[call-arg]
    assert "topsecret" not in repr(s)


def test_invalid_port_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_PORT", "70000")
    with pytest.raises(ValueError, match="postgres_port"):
        Settings(_env_file=None)  # type: ignore[call-arg]


def test_lake_path_layers(settings: Settings) -> None:
    assert settings.lake_path("gold") == Path("data/gold")
    with pytest.raises(ValueError, match="unknown data lake layer"):
        settings.lake_path("platinum")
