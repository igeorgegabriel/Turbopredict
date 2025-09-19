#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd
import re


def slug(s: str) -> str:
    s = re.sub(r"\s+", "_", s.strip())
    s = re.sub(r"[^A-Za-z0-9_\-]", "_", s)
    return s


def main():
    ap = argparse.ArgumentParser(description="Check which PI tags are present in a unit Parquet")
    ap.add_argument('--parquet', required=True, type=Path)
    ap.add_argument('--tags-file', required=True, type=Path)
    args = ap.parse_args()

    tags = [t.strip() for t in args.tags_file.read_text(encoding='utf-8').splitlines() if t.strip() and not t.startswith('#')]
    slugs = [slug(t) for t in tags]

    df = pd.read_parquet(args.parquet, columns=['tag']).dropna()
    present = set(df['tag'].unique().tolist())

    found = [t for t, s in zip(tags, slugs) if s in present]
    missing = [t for t, s in zip(tags, slugs) if s not in present]

    print(f"Total in file: {len(present)} unique tags")
    print(f"Requested: {len(tags)} | Found: {len(found)} | Missing: {len(missing)}")
    if missing:
        print("\nMissing tags:")
        for m in missing:
            print("  -", m)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

