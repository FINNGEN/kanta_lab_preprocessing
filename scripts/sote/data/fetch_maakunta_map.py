#!/usr/bin/env python3
"""Download the maakunta (region) boundaries from Statistics Finland's open regional-division service (1:1 000 000,
EPSG:3067; CC BY 4.0, (c) Statistics Finland) and write a small, simplified version for drawing: maakunta_map.json next
to this script, {"source", "viewBox", "regions": {maakunta name: SVG path}}. Islands under --min-area km2 are dropped
and every ring is simplified with Douglas-Peucker (--tolerance metres), so the file stays small enough to put on every
page; the QC portal (scripts/qc_checks/qc_portal.py) uses it to show a service provider's region.

Usage
-----
  python3 data/fetch_maakunta_map.py [--tolerance 1500] [--min-area 30] [--width 300]
"""
import argparse
import json
import urllib.request
from pathlib import Path

import numpy as np

URL = ("https://geo.stat.fi/geoserver/tilastointialueet/wfs?service=WFS&version=2.0.0&request=GetFeature"
       "&typeName=tilastointialueet:maakunta1000k&outputFormat=json&srsName=EPSG:3067")
OUT = Path(__file__).resolve().parent / "maakunta_map.json"


def douglas_peucker(p, tol):
    """Points of the polyline p (n x 2) kept by Douglas-Peucker with tolerance tol (iterative)."""
    keep = np.zeros(len(p), bool)
    keep[[0, -1]] = True
    stack = [(0, len(p) - 1)]
    while stack:
        a, b = stack.pop()
        if b <= a + 1:
            continue
        seg = p[b] - p[a]
        n = np.hypot(*seg)
        v = p[a + 1:b] - p[a]
        d = np.abs(seg[0] * v[:, 1] - seg[1] * v[:, 0]) / n if n > 0 else np.hypot(v[:, 0], v[:, 1])
        i = int(np.argmax(d))
        if d[i] > tol:
            keep[a + 1 + i] = True
            stack += [(a, a + 1 + i), (a + 1 + i, b)]
    return p[keep]


def area(ring):
    x, y = ring[:, 0], ring[:, 1]
    return 0.5 * abs(np.dot(x, np.roll(y, 1)) - np.dot(y, np.roll(x, 1)))


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--tolerance", type=float, default=1500, help="Douglas-Peucker tolerance (metres)")
    ap.add_argument("--min-area", type=float, default=30, help="Drop islands smaller than this (km2)")
    ap.add_argument("--width", type=float, default=300, help="Width of the drawing (SVG units)")
    args = ap.parse_args()
    with urllib.request.urlopen(URL, timeout=120) as r:
        data = json.load(r)
    rings = {}
    for f in data["features"]:
        g = f["geometry"]
        polys = g["coordinates"] if g["type"] == "MultiPolygon" else [g["coordinates"]]
        out = []
        for poly in polys:
            outer = np.asarray(poly[0], float)  # the outer ring only: holes (lakes, enclaves) are not drawn
            if area(outer) / 1e6 >= args.min_area:
                s = douglas_peucker(outer, args.tolerance)
                if len(s) >= 4:
                    out.append(s)
        if out:
            rings[f["properties"]["nimi"]] = out
    allp = np.vstack([r for rs in rings.values() for r in rs])
    x0, y0 = allp.min(axis=0)
    x1, y1 = allp.max(axis=0)
    k = args.width / (x1 - x0)
    height = (y1 - y0) * k
    paths = {}
    for name, rs in rings.items():
        parts = []
        for r in rs:
            pts = [(round((x - x0) * k, 1), round((y1 - y) * k, 1)) for x, y in r]
            parts.append("M" + " L".join(f"{a:g},{b:g}" for a, b in pts) + "Z")
        paths[name] = "".join(parts)
    OUT.write_text(json.dumps({"source": "Statistics Finland, regional division (maakunta) 1:1 000 000, CC BY 4.0",
                               "viewBox": f"0 0 {args.width:g} {height:.0f}", "regions": paths},
                              ensure_ascii=False) + "\n")
    print(f"{len(paths)} regions, {sum(len(p) for p in paths.values()) / 1024:.0f} kB of paths -> {OUT}")


if __name__ == "__main__":
    main()
