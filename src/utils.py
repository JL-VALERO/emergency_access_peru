"""Shared utility functions."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed"
FIGURES = ROOT / "output" / "figures"
TABLES = ROOT / "output" / "tables"

for _p in [RAW, PROCESSED, FIGURES, TABLES]:
    _p.mkdir(parents=True, exist_ok=True)
