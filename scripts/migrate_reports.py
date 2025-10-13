#!/usr/bin/env python3
"""
Migrate legacy report folders to the new master scan structure.

Target structure:
  reports/DD-MM-YYYY_HH-MMAM/
    <UNIT>/
      ANOMALY_*         (verified anomaly plots)
      EXTENDED_*        (extended plots)
      ...               (unit summaries, etc.)

Sources handled:
  - reports/anomaly_alerts/anomaly_session_YYYYmmdd_HHMMSS
  - reports/controlled_analysis_YYYYmmdd_HHMMSS
  - reports/extended_plots/YYYYmmdd_HHMMSS
  - reports/unit_anomaly_plots_YYYYmmdd_HHMMSS

Usage:
  python scripts/migrate_reports.py [--reports reports] [--dry-run] [--copy]
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from datetime import datetime
import shutil
import sys

# Use shared helpers for naming
try:
    from pi_monitor.plot_controls import build_scan_root_dir, ensure_unit_dir
except Exception:
    # Fallback if module import path differs
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from pi_monitor.plot_controls import build_scan_root_dir, ensure_unit_dir  # type: ignore


TS_RE = re.compile(r"(?P<y>\d{4})(?P<m>\d{2})(?P<d>\d{2})[_-]?(?P<H>\d{2})(?P<M>\d{2})(?P<S>\d{2})")


def _parse_ts_from_name(name: str) -> datetime | None:
    m = TS_RE.search(name)
    if not m:
        return None
    try:
        return datetime(
            int(m.group("y")), int(m.group("m")), int(m.group("d")),
            int(m.group("H")), int(m.group("M")), int(m.group("S"))
        )
    except Exception:
        return None


def _infer_unit_from_filename(p: Path) -> str | None:
    n = p.name
    # ANOMALY_<UNIT>_...
    m = re.match(r"ANOMALY_([A-Z0-9\-]+)_", n, re.IGNORECASE)
    if m:
        return m.group(1)
    # EXTENDED_<UNIT>_...
    m = re.match(r"EXTENDED_([A-Z0-9\-]+)_", n, re.IGNORECASE)
    if m:
        return m.group(1)
    # <UNIT>_extended_analysis.png
    m = re.match(r"([A-Z0-9\-]+)_extended_analysis\.(png|jpg)$", n, re.IGNORECASE)
    if m:
        return m.group(1)
    return None


def _move(src: Path, dst: Path, *, copy: bool = False, dry_run: bool = True) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dry_run:
        print(f"DRY-RUN: {'COPY' if copy else 'MOVE'} {src} -> {dst}")
        return
    if copy:
        shutil.copy2(src, dst)
    else:
        shutil.move(str(src), str(dst))


def migrate_reports(reports_dir: Path, *, dry_run: bool = True, copy: bool = False) -> int:
    moved = 0

    # 1) anomaly_alerts/anomaly_session_YYYYmmdd_HHMMSS
    anomaly_root = reports_dir / "anomaly_alerts"
    if anomaly_root.exists():
        for session in sorted(anomaly_root.glob("anomaly_session_*")):
            ts = _parse_ts_from_name(session.name) or datetime.fromtimestamp(session.stat().st_mtime)
            scan_root = build_scan_root_dir(reports_dir, when=ts)
            for img in session.glob("*.png"):
                unit = _infer_unit_from_filename(img)
                if not unit:
                    continue
                unit_dir = ensure_unit_dir(scan_root, unit)
                # Keep original filename
                _move(img, unit_dir / img.name, copy=copy, dry_run=dry_run)
                moved += 1

    # 2) controlled_analysis_YYYYmmdd_HHMMSS with subdirs per unit
    for controlled in sorted(reports_dir.glob("controlled_analysis_*")):
        ts = _parse_ts_from_name(controlled.name) or datetime.fromtimestamp(controlled.stat().st_mtime)
        scan_root = build_scan_root_dir(reports_dir, when=ts)
        for unit_dir in [p for p in controlled.iterdir() if p.is_dir()]:
            unit = unit_dir.name
            target = ensure_unit_dir(scan_root, unit)
            for f in unit_dir.glob("**/*"):
                if f.is_file():
                    _move(f, target / f.name, copy=copy, dry_run=dry_run)
                    moved += 1

    # 3) extended_plots/TS
    ext_root = reports_dir / "extended_plots"
    if ext_root.exists():
        for ext in sorted(ext_root.glob("*")):
            if not ext.is_dir():
                continue
            ts = _parse_ts_from_name(ext.name) or datetime.fromtimestamp(ext.stat().st_mtime)
            scan_root = build_scan_root_dir(reports_dir, when=ts)
            for img in ext.glob("*.png"):
                unit = _infer_unit_from_filename(img)
                if not unit:
                    # Try parent dir timestamp naming with unit guessed from filename legacy pattern
                    unit = img.stem.split("_")[0]
                unit_dir = ensure_unit_dir(scan_root, unit)
                # Rename to EXTENDED_<UNIT>_<timestamp>.png for consistency
                new_name = f"EXTENDED_{unit}_{ts.strftime('%Y%m%d_%H%M%S')}.png"
                _move(img, unit_dir / new_name, copy=copy, dry_run=dry_run)
                moved += 1

    # 4) unit_anomaly_plots_YYYYmmdd_HHMMSS (older utility)
    for uap in sorted(reports_dir.glob("unit_anomaly_plots_*")):
        ts = _parse_ts_from_name(uap.name) or datetime.fromtimestamp(uap.stat().st_mtime)
        scan_root = build_scan_root_dir(reports_dir, when=ts)
        for unit_dir in [p for p in uap.iterdir() if p.is_dir()]:
            unit = unit_dir.name
            target = ensure_unit_dir(scan_root, unit)
            for f in unit_dir.glob("**/*"):
                if f.is_file():
                    _move(f, target / f.name, copy=copy, dry_run=dry_run)
                    moved += 1

    # 5) Flatten previously migrated single-level folders like DD-MM-YYYY_HH-MMAM
    legacy_session_pattern = re.compile(r"^(?P<date>\d{2}-\d{2}-\d{4})_(?P<hour>\d{2})-(?P<minute>\d{2})(?P<ampm>AM|PM)$")
    for legacy in sorted([p for p in reports_dir.iterdir() if p.is_dir()]):
        m = legacy_session_pattern.match(legacy.name)
        if not m:
            continue
        # Build target session directory using hour-only naming
        try:
            dt = datetime.strptime(f"{m.group('date')} {m.group('hour')}:{m.group('minute')} {m.group('ampm')}", "%d-%m-%Y %I:%M %p")
        except Exception:
            continue
        target_session = build_scan_root_dir(reports_dir, when=dt)
        # Move files and subfolders under proper date/time/unit nesting
        for item in legacy.glob("**/*"):
            if item.is_file():
                # Infer unit from path (take the immediate parent if looks like a unit)
                unit = _infer_unit_from_filename(item) or item.parent.name
                unit_dir = ensure_unit_dir(target_session, unit)
                _move(item, unit_dir / item.name, copy=copy, dry_run=dry_run)
                moved += 1
    
    return moved


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Migrate legacy report folders to day-of-scan master structure")
    p.add_argument("--reports", type=Path, default=Path("reports"))
    p.add_argument("--dry-run", action="store_true", help="Preview actions without moving files")
    p.add_argument("--copy", action="store_true", help="Copy instead of move (leave originals)")
    args = p.parse_args(argv)

    moved = migrate_reports(args.reports, dry_run=args.dry_run, copy=args.copy)
    if args.dry_run:
        print(f"\nDRY-RUN complete. {moved} file actions planned.")
    else:
        print(f"\nMigration complete. {moved} files moved.")


if __name__ == "__main__":
    main()
