#!/usr/bin/env python3
"""AI Agent Runner (Sigma-only) with no-overlap per-unit leases.

Usage (examples):
  # Agent 1 on Machine A: ABFSB only
  UNIT_LEASE_DIR=\\\\share\\turbopredict\\locks \
  AGENT_NAME=agent-abfsb AGENT_SCOPE=PLANT=ABFSB MAX_WORKERS=2 \
  python scripts/ai_agent_runner.py

  # Agent 2 on Machine B: PCFS only
  UNIT_LEASE_DIR=\\\\share\\turbopredict\\locks \
  AGENT_NAME=agent-pcfs AGENT_SCOPE=PLANT=PCFS MAX_WORKERS=3 \
  python scripts/ai_agent_runner.py

  # Agents 3–6 on Machines C–F: PCMSB sharded 4 ways
  UNIT_LEASE_DIR=\\\\share\\turbopredict\\locks \
  AGENT_NAME=agent-pcmsb-1 AGENT_SCOPE=PLANT=PCMSB SHARD_INDEX=0 SHARD_TOTAL=4 \
  python scripts/ai_agent_runner.py
"""

from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# Ensure project root is on sys.path so `pi_monitor` resolves correctly
import sys as _sys_path_fix
_sys_path_fix.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pi_monitor.parquet_database import ParquetDatabase
from pi_monitor.parquet_auto_scan import ParquetAutoScanner
from corrected_unit_classification import get_corrected_unit_classification, classify_unit_by_name


def _env_str(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    return v if v is not None else default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except Exception:
        return default


def _state_path(agent_name: str) -> Path:
    root = Path("state")
    root.mkdir(parents=True, exist_ok=True)
    safe = "".join(c for c in agent_name if c.isalnum() or c in "._-") if agent_name else "agent"
    return root / f"{safe}_state.json"


def _load_state(agent_name: str) -> Dict[str, str]:
    p = _state_path(agent_name)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_state(agent_name: str, state: Dict[str, str]) -> None:
    p = _state_path(agent_name)
    try:
        p.write_text(json.dumps(state, indent=2), encoding="utf-8")
    except Exception:
        pass


def _scope_units(db: ParquetDatabase, scope: str) -> List[str]:
    scope = (scope or "").strip()
    all_units = db.get_all_units()
    if not scope:
        return all_units
    if scope.upper().startswith("PLANT="):
        plant = scope.split("=", 1)[1].strip().upper()
        return [u for u in all_units if classify_unit_by_name(u).upper() == plant]
    if scope.upper().startswith("UNITS="):
        allow = [s.strip() for s in scope.split("=", 1)[1].split(",") if s.strip()]
        allow_set = set(allow)
        return [u for u in all_units if u in allow_set]
    # Unknown scope → default all
    return all_units


def _apply_shard(units: List[str], shard_index: int, shard_total: int) -> List[str]:
    if shard_total <= 1:
        return units
    out: List[str] = []
    for i, u in enumerate(sorted(units)):
        if i % shard_total == (shard_index % shard_total):
            out.append(u)
    return out


def _apply_exclude(units: List[str]) -> List[str]:
    """Filter out units listed in AGENT_EXCLUDE_UNITS (comma-separated).

    Matching is case-insensitive and compares exact unit ids after strip().
    """
    raw = _env_str("AGENT_EXCLUDE_UNITS", "") or ""
    if not raw.strip():
        return units
    deny = {u.strip().upper() for u in raw.split(',') if u.strip()}
    return [u for u in units if u.strip().upper() not in deny]

def _units_with_new_data(db: ParquetDatabase, units: List[str], last_state: Dict[str, str]) -> List[str]:
    out = []
    for u in units:
        ts = db.get_latest_timestamp(u)
        if ts is None:
            continue
        last = last_state.get(u)
        if not last:
            out.append(u)
            continue
        try:
            prev = datetime.fromisoformat(last)
            if ts > prev:
                out.append(u)
        except Exception:
            out.append(u)
    return out


def _analyze_one(scanner: ParquetAutoScanner, unit: str) -> Dict[str, object]:
    try:
        res = scanner.analyze_unit_data(unit, run_anomaly_detection=True, days_limit=90)
        return {
            "unit": unit,
            "status": res.get("status", "ok"),
            "records": int(res.get("records", 0) or 0),
            "analysis": res,
            "anomalies": res.get("anomalies", {}),
        }
    except Exception as e:
        return {"unit": unit, "status": "error", "error": str(e)}


def main() -> None:
    agent_name = _env_str("AGENT_NAME", "agent")
    scope = _env_str("AGENT_SCOPE", "")  # e.g., PLANT=PCMSB or UNITS=K-31-01,C-202
    # Windows file locking issue: use 1 worker (sequential) by default
    # On Unix systems, can use MAX_WORKERS=3+ for parallelism
    # Override with MAX_WORKERS env var for custom concurrency
    import platform
    default_workers = 1 if platform.system() == 'Windows' else 3
    max_workers = _env_int("MAX_WORKERS", default_workers)
    shard_total = _env_int("SHARD_TOTAL", 1)
    shard_index = _env_int("SHARD_INDEX", 0)
    only_new = _env_str("ONLY_NEW", "1").lower() in ("1", "true", "yes", "y")

    print(f"[agent] name={agent_name} scope={scope} workers={max_workers} shard={shard_index}/{shard_total} only_new={only_new}")
    if os.getenv("UNIT_LEASE_DIR"):
        print(f"[agent] lease dir={os.getenv('UNIT_LEASE_DIR')}")

    db = ParquetDatabase()
    scanner = ParquetAutoScanner()

    units = _scope_units(db, scope)
    units_all = list(units)
    units = _apply_shard(units, shard_index, shard_total)
    units = _apply_exclude(units)
    print(f"[agent] candidate units: {len(units)} -> {units}")
    excluded_count = len(units_all) - len(units)
    if excluded_count > 0:
        print(f"[agent] excluded {excluded_count} unit(s) via AGENT_EXCLUDE_UNITS")

    state = _load_state(agent_name)
    if only_new:
        units = _units_with_new_data(db, units, state)
        print(f"[agent] units with new data: {len(units)}")

    if not units:
        print("[agent] no work")
        return

    results: Dict[str, Dict[str, object]] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_analyze_one, scanner, u): u for u in units}
        def _verified_sigma_only_count(by_tag: Dict[str, dict]) -> int:
            count = 0
            for _t, info in (by_tag or {}).items():
                try:
                    recent = int((info.get('recency_breakdown', {}) or {}).get('last_24h', 0) or 0)
                    max_run = int(info.get('sigma_consecutive_ge_n', 0) or 0)
                    if recent > 0 and max_run >= 6:
                        count += 1
                except Exception:
                    continue
            return count

        for f in as_completed(futures):
            r = f.result()
            u = r.get("unit")
            results[u] = r
            # Summary-only line (avoid dumping large JSON structures)
            status = r.get("status", "ok")
            records = r.get("records", 0)
            an = r.get("anomalies") if isinstance(r, dict) else {}
            by_tag = an.get("by_tag", {}) if isinstance(an, dict) else {}
            total = int(an.get("total_anomalies", 0) or 0) if isinstance(an, dict) else 0
            verified = _verified_sigma_only_count(by_tag) if isinstance(by_tag, dict) else 0
            print(f"[agent] {u}: status={status} records={records:,} tags={len(by_tag)} anomalies={total} verified={verified}")

            # Update last scanned ts if available
            ts = db.get_latest_timestamp(u)
            if ts is not None:
                state[u] = ts.isoformat()

    _save_state(agent_name, state)

    # After analysis completes: publish to TurboBubble + generate PDF + send email
    # by leveraging the anomaly-triggered plotter which already performs
    # Supabase sync and email sending.
    publish = _env_str("AGENT_PUBLISH", "1").lower() in ("1", "true", "yes", "y")
    if publish:
        try:
            from pi_monitor.anomaly_triggered_plots import generate_anomaly_plots
            # Ensure plotter uses sigma-only verification path (we disabled MTD/IF)
            os.environ.setdefault('SIGMA_ONLY', '1')
            detection_results: Dict[str, dict] = {}
            # IMPORTANT: Include ALL units analyzed, not just those with anomalies
            # This ensures the session report shows all units and data quality issues
            for unit, r in results.items():
                an = r.get("anomalies") if isinstance(r, dict) else {}
                if isinstance(an, dict):
                    # Include unit in results even if by_tag is empty (no anomalies = healthy)
                    detection_results[unit] = an
            if detection_results:
                print(f"[agent] Publishing {len(detection_results)} units to TurboBubble + PDF/email...")
                session_dir = generate_anomaly_plots(detection_results)
                print(f"[agent] Report session: {session_dir}")
            else:
                print("[agent] No results to publish (all units empty)")
        except Exception as e:
            print(f"[agent] Publish failed: {e}")

    print(f"[agent] done. processed={len(results)}")


if __name__ == "__main__":
    main()
