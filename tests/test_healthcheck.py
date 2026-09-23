from __future__ import annotations

from homepedia.healthcheck import run_checks
from homepedia.settings import Settings


def test_failing_check_is_reported_not_raised(settings: Settings) -> None:
    def boom(_: Settings) -> str:
        raise ConnectionError("down")

    results = run_checks(settings, {"ok": lambda _: "fine", "bad": boom})
    assert [(r.name, r.ok) for r in results] == [("ok", True), ("bad", False)]
    assert results[1].detail == "ConnectionError: down"
