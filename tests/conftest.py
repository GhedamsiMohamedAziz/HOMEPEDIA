from __future__ import annotations

import pytest

from homepedia.settings import Settings


@pytest.fixture
def settings() -> Settings:
    # explicit values: tests must not depend on the developer's .env
    return Settings(_env_file=None)  # type: ignore[call-arg]
