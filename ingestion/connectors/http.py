"""Minimal HTTP helpers: streamed download with sha256, JSON GET, bounded retries. Stdlib only."""

from __future__ import annotations

import hashlib
import json
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

USER_AGENT = "homepedia-ingestion/0.1 (+https://github.com/homepedia)"
RETRY_STATUS = {429, 500, 502, 503, 504}


def _open(url: str, timeout: int, retries: int = 3) -> Any:
    if not url.startswith("https://"):
        raise ValueError(f"refusing non-https url: {url}")
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "*/*"})  # noqa: S310
    for attempt in range(retries + 1):
        try:
            return urllib.request.urlopen(req, timeout=timeout)  # noqa: S310 — https enforced above
        except urllib.error.HTTPError as exc:
            if exc.code not in RETRY_STATUS or attempt == retries:
                raise
        except (urllib.error.URLError, TimeoutError):
            if attempt == retries:
                raise
        time.sleep(2**attempt)
    raise AssertionError("unreachable")


def download(url: str, dest: Path, timeout: int, retries: int = 3) -> str:
    """Stream `url` to `dest` (atomic via .part), return the sha256 hex digest."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_suffix(dest.suffix + ".part")
    for attempt in range(retries + 1):
        digest = hashlib.sha256()
        try:
            with _open(url, timeout) as resp, part.open("wb") as fh:
                while chunk := resp.read(1 << 20):
                    fh.write(chunk)
                    digest.update(chunk)
        except (TimeoutError, ConnectionError, urllib.error.URLError):
            if attempt == retries:  # slow public servers stall mid-stream; restart the transfer
                raise
            time.sleep(2**attempt)
            continue
        part.replace(dest)
        return digest.hexdigest()
    raise AssertionError("unreachable")


def get_json(url: str, timeout: int) -> Any:
    with _open(url, timeout) as resp:
        return json.load(resp)
