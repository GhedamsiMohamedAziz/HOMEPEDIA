"""`python -m ingestion.pipelines.run [source ...|all]` — run connectors, exit 1 if any fails."""

from __future__ import annotations

import argparse
import sys

from homepedia.logging import configure_logging, get_logger
from homepedia.settings import get_settings
from ingestion.pipelines.registry import CONNECTORS

log = get_logger("ingestion.run")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sources", nargs="*", default=["all"], help=f"one of: all, {', '.join(CONNECTORS)}")
    args = parser.parse_args(argv)
    names = list(CONNECTORS) if args.sources == ["all"] else args.sources
    unknown = [n for n in names if n not in CONNECTORS]
    if unknown:
        parser.error(f"unknown source(s): {unknown}")

    settings = get_settings()
    configure_logging(settings.log_level)
    failed = 0
    for name in names:
        try:
            result = CONNECTORS[name](settings).run()
            log.info(
                "source ok",
                extra={"extra": {"source": name, "duration_s": result.duration_s, **result.tables}},
            )
        except Exception as exc:  # one failing source must not hide the others; the ledger has the details
            failed += 1
            log.error(
                "source failed", extra={"extra": {"source": name, "error": f"{type(exc).__name__}: {exc}"}}
            )
            log.debug("traceback", exc_info=exc)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
