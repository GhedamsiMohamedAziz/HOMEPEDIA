"""Territory resolution without an INSEE code: point-in-polygon (stations) and postal code + town (texts)."""

from __future__ import annotations

import json
import re
import unicodedata
from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from shapely import STRtree
from shapely.geometry import Point, shape

_ABBREV = {"ST": "SAINT", "STE": "SAINTE", "S": "SUR"}


def normalize_name(name: str | None) -> str:
    """'St-Lys' -> 'SAINT LYS', 'L'Isle-Jourdain' -> 'L ISLE JOURDAIN' (accent/case/punctuation-free)."""
    if not name:
        return ""
    s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    tokens = re.split(r"[^A-Z0-9]+", s.upper())
    return " ".join(_ABBREV.get(t, t) for t in tokens if t)


@dataclass
class CommuneIndex:
    """Built once on the driver from the geo reference; ~35k communes."""

    codes: list[str]
    names: list[str]
    tree: STRtree | None
    by_postal: dict[str, list[int]]

    @classmethod
    def build(
        cls, rows: Iterable[tuple[str, str, Sequence[str] | None, str | None]], *, geometry: bool = True
    ) -> CommuneIndex:
        codes: list[str] = []
        names: list[str] = []
        geoms: list[object] = []
        by_postal: dict[str, list[int]] = {}
        for i, (code, name, postal_codes, geojson) in enumerate(rows):
            codes.append(code)
            names.append(normalize_name(name))
            for pc in postal_codes or ():
                by_postal.setdefault(pc, []).append(i)
            if geometry:
                geoms.append(shape(json.loads(geojson)) if geojson else Point())
        tree = STRtree(geoms) if geometry else None
        return cls(codes, names, tree, by_postal)

    def resolve_point(self, longitude: float, latitude: float) -> str | None:
        if self.tree is None:
            raise RuntimeError("index built without geometry")
        hits = self.tree.query(Point(longitude, latitude), predicate="intersects")
        return self.codes[int(hits[0])] if len(hits) else None

    def resolve_postal(self, postal_code: str | None, town: str | None) -> str | None:
        """Exact town-name match within the postal code; a postal code with a single commune also resolves."""
        cands = self.by_postal.get(postal_code or "", [])
        if not cands:
            return None
        if len(cands) == 1:
            return self.codes[cands[0]]
        wanted = normalize_name(town)
        for i in cands:
            if self.names[i] == wanted:
                return self.codes[i]
        return None
