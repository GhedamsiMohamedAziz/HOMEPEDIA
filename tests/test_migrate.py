from __future__ import annotations

from pathlib import Path

import pytest

from database.migrate import MIGRATIONS_DIR, list_migrations


def test_repo_migrations_are_well_formed() -> None:
    versions = [v for v, _ in list_migrations(MIGRATIONS_DIR)]
    assert versions == sorted(versions)
    assert versions[0] == 1


def test_bad_filename_rejected(tmp_path: Path) -> None:
    (tmp_path / "init.sql").write_text("select 1")
    with pytest.raises(ValueError, match="bad migration filename"):
        list_migrations(tmp_path)


def test_duplicate_version_rejected(tmp_path: Path) -> None:
    (tmp_path / "0001_a.sql").write_text("select 1")
    (tmp_path / "0001_b.sql").write_text("select 1")
    with pytest.raises(ValueError, match="duplicate"):
        list_migrations(tmp_path)
