from __future__ import annotations

import json

from processing.transformations.geo import CommuneIndex, normalize_name


def test_normalize_name() -> None:
    assert normalize_name("St-Lys") == "SAINT LYS"
    assert normalize_name("L'Isle-Jourdain") == "L ISLE JOURDAIN"
    assert normalize_name("Ste Foy la Grande") == "SAINTE FOY LA GRANDE"
    assert normalize_name("Villenave-d'Ornon") == normalize_name("VILLENAVE D ORNON")
    assert normalize_name(None) == ""


def square(x0: float, y0: float, x1: float, y1: float) -> str:
    return json.dumps(
        {"type": "Polygon", "coordinates": [[[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]]]}
    )


def build() -> CommuneIndex:
    return CommuneIndex.build(
        [
            ("33063", "Bordeaux", ["33000", "33100", "33200"], square(-0.7, 44.8, -0.5, 44.9)),
            ("33281", "Mérignac", ["33700"], square(-0.75, 44.8, -0.7, 44.9)),
            ("33119", "Cenon", ["33150"], square(-0.5, 44.8, -0.45, 44.9)),
            ("31555", "Toulouse", ["31000", "31100"], square(1.3, 43.5, 1.5, 43.7)),
            ("31069", "Balma", ["31130"], square(1.5, 43.5, 1.6, 43.7)),
            ("31148", "Colomiers", ["31770"], None),
        ]
    )


def test_resolve_point() -> None:
    idx = build()
    assert idx.resolve_point(-0.58, 44.84) == "33063"
    assert idx.resolve_point(1.55, 43.6) == "31069"
    assert idx.resolve_point(2.35, 48.85) is None  # Paris: outside every polygon


def test_resolve_postal() -> None:
    idx = build()
    assert idx.resolve_postal("33700", "MERIGNAC") == "33281"  # unique postal code
    assert idx.resolve_postal("33000", "BORDEAUX") == "33063"
    assert idx.resolve_postal("31130", "Balma") == "31069"
    assert idx.resolve_postal("99999", "NOWHERE") is None
    assert idx.resolve_postal(None, "BORDEAUX") is None
