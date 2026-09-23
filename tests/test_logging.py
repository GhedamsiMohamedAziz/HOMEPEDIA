from __future__ import annotations

import json
import logging

from homepedia.logging import JsonFormatter


def test_json_formatter_includes_extra() -> None:
    record = logging.LogRecord("x", logging.INFO, __file__, 1, "hello %s", ("w",), None)
    record.extra = {"rows_received": 3}
    out = json.loads(JsonFormatter().format(record))
    assert out["msg"] == "hello w"
    assert out["rows_received"] == 3
    assert out["level"] == "INFO"
