from __future__ import annotations

import argparse
from pathlib import Path

from .config import Config
from .excel_refresh import refresh_with_xlwings, refresh_excel_safe
from .ingest import load_latest_frame, write_parquet
from .anomaly import add_anomalies
from .pipeline import run_pipeline


def _add_common_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--xlsx", type=Path, help="Path to source Excel file (overrides config)")
    p.add_argument("--parquet", type=Path, help="Output Parquet path (overrides config)")
    p.add_argument("--dataset-dir", type=Path, help="Optional Parquet dataset directory to append to")
    p.add_argument("--sheet", type=str, help="Excel sheet name to read from")
    p.add_argument("--roll", type=int, default=None, help="Rolling window for anomalies")
    p.add_argument("--drop-pct", type=float, default=None, help="Drop percent threshold (0-1)")
    p.add_argument("--unit", type=str, default=None, help="Equipment/unit identifier to stamp into Parquet")
    p.add_argument("--plant", type=str, default=None, help="Plant identifier to stamp")
    p.add_argument("--tag", type=str, default=None, help="PI tag identifier to stamp")


def main(argv: list[str] | None = None) -> None:
    cfg = Config()

    parser = argparse.ArgumentParser(prog="pi-monitor", description="PI data anomaly pipeline (scaffold)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_refresh = sub.add_parser("refresh", help="Refresh Excel workbook via xlwings")
    _add_common_args(p_refresh)

    p_ingest = sub.add_parser("ingest", help="Ingest Excel -> Parquet")
    _add_common_args(p_ingest)

    p_scan = sub.add_parser("scan", help="Scan anomalies (Excel -> Parquet -> scan)")
    _add_common_args(p_scan)
    p_scan.add_argument("--save-plot", type=Path, default=None, help="Optional path to save plot PNG")
    p_scan.add_argument("--show-plot", action="store_true", help="Show plot interactively")

    p_run = sub.add_parser("run", help="Run full pipeline and email")
    _add_common_args(p_run)
    p_run.add_argument("--no-refresh", action="store_true", help="Skip Excel refresh")
    p_run.add_argument("--no-email", action="store_true", help="Skip email notification")
    p_run.add_argument("--save-plot", type=Path, default=None, help="Optional path to save plot PNG")
    p_run.add_argument("--show-plot", action="store_true", help="Show plot interactively")

    # Batch unit builder: fetch many tags via DataLink and write a single Parquet
    p_batch = sub.add_parser("batch-unit", help="Loop tags via DataLink and build a unit Parquet")
    p_batch.add_argument("--xlsx", type=Path, required=True)
    p_batch.add_argument("--tags", type=Path, required=True, help="Text file with one PI tag per line")
    p_batch.add_argument("--tags-sheet", type=str, help="Read tags from this sheet row 2 (A2..)")
    p_batch.add_argument("--out", type=Path, required=True, help="Output Parquet file path")
    p_batch.add_argument("--plant", type=str, required=True)
    p_batch.add_argument("--unit", type=str, required=True)
    p_batch.add_argument("--server", type=str, default="\\\\PTSG-1MMPDPdb01")
    p_batch.add_argument("--start", type=str, default="-1y")
    p_batch.add_argument("--end", type=str, default="*")
    p_batch.add_argument("--step", type=str, default="-0.1h")
    p_batch.add_argument("--sheet", type=str, default="DL_WORK")
    p_batch.add_argument("--settle-seconds", type=float, default=1.5)
    p_batch.add_argument("--visible", action="store_true", help="Show Excel window during fetch (more reliable)")

    # Plot all series from a Parquet (one PNG per tag)
    p_plot = sub.add_parser("plot", help="Plot time series for all tags in a Parquet")
    p_plot.add_argument("--parquet", type=Path, required=True, help="Input Parquet path")
    p_plot.add_argument("--out-dir", type=Path, default=Path("reports"))
    p_plot.add_argument("--roll", type=int, default=5)
    p_plot.add_argument("--drop-pct", type=float, default=0.10)
    p_plot.add_argument("--plant", type=str, default=None)
    p_plot.add_argument("--unit", type=str, default=None)
    p_plot.add_argument("--tag", type=str, default=None)
    p_plot.add_argument("--limit", type=int, default=None)
    p_plot.add_argument("--plain", action="store_true", help="Plot without anomalies or rolling mean")

    # Deduplicate Parquet by keys (plant, unit, tag, time)
    p_dedup = sub.add_parser("dedup", help="Remove duplicate rows from Parquet")
    p_dedup.add_argument("--parquet", type=Path, required=True)
    p_dedup.add_argument("--out", type=Path, default=None)
    p_dedup.add_argument("--keys", type=str, default="plant,unit,tag,time", help="Comma-separated key columns")

    # Archive stray Parquet files (not prefixed by a unit id)
    p_archive = sub.add_parser("archive-strays", help="Archive non-unit Parquet files in processed dir")
    p_archive.add_argument("--processed", type=Path, default=Path("data/processed"))

    # PI Web API connectivity check
    p_web = sub.add_parser("webapi-check", help="Check PI Web API connectivity and optional tag fetch")
    p_web.add_argument("--base-url", type=str, default=None, help="PI Web API base URL (e.g., https://host/piwebapi)")
    p_web.add_argument("--server", type=str, default=None, help="PI Data Archive server name (e.g., PTSG-1MMPDPdb01)")
    p_web.add_argument("--tag", type=str, default=None, help="Optional PI tag to resolve/fetch")
    p_web.add_argument("--start", type=str, default="-2h", help="Relative start (e.g., -2h)")
    p_web.add_argument("--end", type=str, default="*", help="End time (default now '*')")
    p_web.add_argument("--step", type=str, default="-0.1h", help="Step (e.g., -0.1h = 6 minutes)")
    p_web.add_argument("--timeout", type=float, default=15.0, help="HTTP timeout seconds")
    p_web.add_argument("--insecure", action="store_true", help="Disable SSL verification (testing only)")

    # Fix unit alias inside Parquet files and filenames
    p_fix_alias = sub.add_parser("fix-unit-alias", help="Replace unit alias with canonical name in Parquet files")
    p_fix_alias.add_argument("--alias", required=True, help="Alias unit string to replace (e.g., FI-07001)")
    p_fix_alias.add_argument("--canonical", required=True, help="Canonical unit string (e.g., 07-MT01-K001)")
    p_fix_alias.add_argument("--processed", type=Path, default=Path("data/processed"))

    # TURBOPREDICT X PROTEAN: Auto-scan with intelligent local database caching
    p_auto_scan = sub.add_parser("auto-scan", help="[TURBOPREDICT X PROTEAN] Auto-scan local database and fetch from PI if stale")
    p_auto_scan.add_argument("--xlsx", type=Path, required=True, help="Excel workbook for PI DataLink")
    p_auto_scan.add_argument("--tags", type=Path, help="Text file with one PI tag per line")
    p_auto_scan.add_argument("--tags-sheet", type=str, help="Read tags from Excel sheet (row 2+)")
    p_auto_scan.add_argument("--plant", type=str, required=True, help="Plant identifier")
    p_auto_scan.add_argument("--unit", type=str, required=True, help="Unit identifier") 
    p_auto_scan.add_argument("--server", type=str, default="\\\\PTSG-1MMPDPdb01", help="PI server path")
    p_auto_scan.add_argument("--max-age-hours", type=float, default=1.0, help="Max data age before fetching from PI")
    p_auto_scan.add_argument("--start", type=str, default="-24h", help="Start time for PI fetch")
    p_auto_scan.add_argument("--end", type=str, default="*", help="End time for PI fetch")
    p_auto_scan.add_argument("--step", type=str, default="-6min", help="Time step for PI fetch")
    p_auto_scan.add_argument("--batch-size", type=int, default=10, help="Tags per batch for PI fetch")
    p_auto_scan.add_argument("--force-refresh", action="store_true", help="Force refresh even if data is fresh")
    p_auto_scan.add_argument("--no-anomaly", action="store_true", help="Skip anomaly detection")
    p_auto_scan.add_argument("--no-plots", action="store_true", help="Skip plot generation")
    p_auto_scan.add_argument("--no-email", action="store_true", help="Skip email notifications")
    p_auto_scan.add_argument("--db-path", type=Path, help="Custom local database path")
    p_auto_scan.add_argument("--cleanup-days", type=int, default=90, help="Days of data to keep in local DB")

    # Database status command
    p_db_status = sub.add_parser("db-status", help="Show local database status and statistics")
    p_db_status.add_argument("--db-path", type=Path, help="Custom local database path")
    p_db_status.add_argument("--cleanup-days", type=int, help="Clean up old data (days to keep)")
    p_db_status.add_argument("--tag", type=str, help="Show details for specific tag")
    p_db_status.add_argument("--plant", type=str, help="Filter by plant")
    p_db_status.add_argument("--unit", type=str, help="Filter by unit")

    args = parser.parse_args(argv)

    # Override config from CLI if provided
    if getattr(args, "xlsx", None):
        cfg.paths.xlsx_path = args.xlsx
    if getattr(args, "parquet", None):
        cfg.paths.parquet_path = args.parquet
    if getattr(args, "roll", None) is not None:
        cfg.roll = args.roll
    if getattr(args, "drop_pct", None) is not None:
        cfg.drop_pct = args.drop_pct
    if getattr(args, "unit", None) is not None:
        cfg.unit = args.unit
    if getattr(args, "plant", None) is not None:
        cfg.plant = args.plant
    if getattr(args, "tag", None) is not None:
        cfg.tag = args.tag

    if args.cmd == "refresh":
        refresh_excel_safe(cfg.paths.xlsx_path)
        return

    if args.cmd == "ingest":
        df = load_latest_frame(cfg.paths.xlsx_path, unit=cfg.unit, plant=getattr(cfg, 'plant', None), tag=getattr(cfg, 'tag', None), sheet_name=args.sheet)
        out = write_parquet(df, cfg.paths.parquet_path)
        print(f"Wrote Parquet: {out}")
        if args.dataset_dir:
            from .dataset import write_dataset
            write_dataset(df, args.dataset_dir)
            print(f"Appended to dataset: {args.dataset_dir}")
        return

    if args.cmd == "scan":
        # No refresh in scan. Just ingest and analyze.
        df = load_latest_frame(cfg.paths.xlsx_path, unit=cfg.unit, plant=getattr(cfg, 'plant', None), tag=getattr(cfg, 'tag', None), sheet_name=args.sheet)
        df = add_anomalies(df, roll=cfg.roll, drop_pct=cfg.drop_pct)
        out = write_parquet(df, cfg.paths.parquet_path)
        print(f"Scanned and wrote Parquet: {out}")
        if args.save_plot or args.show_plot:
            from .plotting import plot_anomalies
            plot_anomalies(df, save_to=args.save_plot, show=args.show_plot)
            if args.save_plot:
                print(f"Saved plot: {args.save_plot}")
        if args.dataset_dir:
            from .dataset import write_dataset
            write_dataset(df, args.dataset_dir)
            print(f"Appended to dataset: {args.dataset_dir}")
        return

    if args.cmd == "run":
        out = run_pipeline(
            cfg,
            refresh_excel=not args.no_refresh,
            send_mail=not args.no_email,
            plot_path=args.save_plot,
            show_plot=args.show_plot,
        )
        print(f"Pipeline completed. Parquet: {out}")
        return

    if args.cmd == "batch-unit":
        from .batch import build_unit_from_tags, read_tags_from_sheet
        if args.tags_sheet:
            tags = read_tags_from_sheet(args.xlsx, sheet_name=args.tags_sheet)
        else:
            raw_tags = [t.strip() for t in Path(args.tags).read_text(encoding="utf-8").splitlines() if t.strip() and not t.strip().startswith('#')]
            # De-duplicate while preserving order
            tags = list(dict.fromkeys(raw_tags))
        out = build_unit_from_tags(
            args.xlsx,
            tags,
            args.out,
            plant=args.plant,
            unit=args.unit,
            server=args.server,
            start=args.start,
            end=args.end,
            step=args.step,
            work_sheet=args.sheet,
            settle_seconds=args.settle_seconds,
            visible=args.visible,
        )
        print(f"Batch completed. Wrote Parquet: {out}")
        try:
            import pandas as pd
            df = pd.read_parquet(out)
            print(f"Rows: {len(df):,}; Tags: {df['tag'].nunique() if 'tag' in df.columns else 'n/a'}")
        except Exception as e:
            print(f"Note: Could not read back Parquet (maybe empty): {e}")
        return

    if args.cmd == "plot":
        from .plotting import plot_all_from_parquet
        paths = plot_all_from_parquet(
            args.parquet,
            args.out_dir,
            roll=args.roll,
            drop_pct=args.drop_pct,
            filter_plant=args.plant,
            filter_unit=args.unit,
            filter_tag=args.tag,
            limit=args.limit,
            plain=args.plain,
        )
        print(f"Saved {len(paths)} plot(s) to {args.out_dir}")
        for p in paths[:5]:
            print(" -", p)
        return

    if args.cmd == "dedup":
        from .clean import dedup_parquet
        keys = [s.strip() for s in args.keys.split(",") if s.strip()]
        out = dedup_parquet(args.parquet, args.out, keys=keys)
        try:
            import pandas as pd
            before = len(pd.read_parquet(args.parquet))
            after = len(pd.read_parquet(out))
            print(f"Dedup complete. {before-after:,} duplicates removed. Wrote: {out}")
        except Exception:
            print(f"Dedup complete. Wrote: {out}")
        return

    if args.cmd == "archive-strays":
        from .parquet_database import ParquetDatabase
        processed_dir = args.processed
        db = ParquetDatabase(processed_dir.parent)
        archived = db.archive_non_unit_parquet()
        if archived:
            print(f"Archived {len(archived)} stray file(s) to {processed_dir / 'archive'}")
            for p in archived[:10]:
                print(" -", p.name)
            if len(archived) > 10:
                print(f" ... and {len(archived)-10} more")
        else:
            print("No stray Parquet files found to archive.")
        return

    if args.cmd == "fix-unit-alias":
        import pandas as pd
        import re
        processed_dir = args.processed
        alias = args.alias
        canonical = args.canonical

        safe_alias_tokens = {alias, alias.replace('/', '-'), alias.replace('/', '_')}
        safe_canonical = canonical.replace('/', '-')

        changed = 0
        renamed = 0
        for p in processed_dir.glob('*.parquet'):
            try:
                name_token = p.name.split('_')[0] if '_' in p.name else p.stem
                # Rename file if it starts with alias token
                if name_token in safe_alias_tokens:
                    new_name = p.name.replace(name_token, safe_canonical, 1)
                    target = processed_dir / new_name
                    if target.exists():
                        # Avoid overwriting; add suffix
                        stem, suffix = target.stem, target.suffix
                        i = 1
                        while (processed_dir / f"{stem}.{i}{suffix}").exists():
                            i += 1
                        target = processed_dir / f"{stem}.{i}{suffix}"
                    p.rename(target)
                    p = target
                    renamed += 1

                # Update Parquet content if it contains alias in 'unit'
                df = pd.read_parquet(p)
                if 'unit' in df.columns and df['unit'].astype(str).str.contains(re.escape(alias)).any():
                    df['unit'] = df['unit'].replace({alias: canonical, alias.replace('/', '-'): canonical, alias.replace('/', '_'): canonical})
                    df.to_parquet(p, index=False)
                    changed += 1
            except Exception:
                continue

        print(f"Updated {changed} file(s) with canonical unit; renamed {renamed} file(s).")
        return

    if args.cmd == "auto-scan":
        from .parquet_auto_scan import ParquetAutoScanner
        from .parquet_database import ParquetDatabase
        from .batch import read_tags_from_sheet
        import json
        
        # Initialize parquet-based systems
        parquet_db = ParquetDatabase()
        scanner = ParquetAutoScanner()
        
        # Get tags
        if args.tags_sheet:
            tags = read_tags_from_sheet(args.xlsx, sheet_name=args.tags_sheet)
        elif args.tags:
            tags = [t.strip() for t in Path(args.tags).read_text(encoding="utf-8").splitlines() 
                   if t.strip() and not t.strip().startswith('#')]
        else:
            print("Error: Must specify either --tags or --tags-sheet")
            return
        
        if not tags:
            print("Error: No tags found to process")
            return
            
        print(f"[TURBOPREDICT X PROTEAN] Auto-Scan starting for {len(tags)} tags")
        print(f"Plant: {args.plant}, Unit: {args.unit}")
        print(f"Max age: {args.max_age_hours} hours, Force refresh: {args.force_refresh}")
        
        try:
            # Check data freshness and refresh if needed using ParquetAutoScanner
            print(f"Checking data freshness (max age: {args.max_age_hours} hours)...")
            
            results = scanner.refresh_stale_units_with_progress(
                xlsx_path=args.xlsx,
                max_age_hours=args.max_age_hours
            )
            
            # Reload parquet database to see fresh data
            print("Reloading database with fresh data...")
            parquet_db = ParquetDatabase()  # Reinitialize to pick up fresh data
            
            # Display results summary
            print(f"\nParquet auto-refresh complete!")
            if results.get("units_processed"):
                print(f"Units processed: {results['units_processed']}")
            if results.get("total_records_refreshed"):
                print(f"Records refreshed: {results['total_records_refreshed']:,}")
            if results.get("refresh_time_seconds"):
                print(f"Refresh time: {results['refresh_time_seconds']:.2f} seconds")
            
            # Show current database status after refresh
            print(f"\nCurrent database overview:")
            all_units = parquet_db.get_all_units()
            print(f"Available units: {', '.join(all_units)}")
            
            for unit in all_units:
                freshness = parquet_db.get_data_freshness_info(unit)
                status = "FRESH" if not freshness['is_stale'] else "STALE"
                print(f"  {unit}: {freshness['total_records']:,} records, "
                      f"{freshness['data_age_hours']:.1f}h old, {status}")
            
            # Save refresh results
            results_file = cfg.paths.reports_dir / f"parquet_refresh_results_{args.plant}.json"
            results_file.parent.mkdir(parents=True, exist_ok=True)
            with open(results_file, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            print(f"Refresh results saved: {results_file}")
            
        except Exception as e:
            print(f"Parquet auto-refresh failed: {e}")
            import traceback
            traceback.print_exc()
        return

    if args.cmd == "webapi-check":
        import os
        from .webapi import PIWebAPIClient, fetch_tags_via_webapi

        server = args.server or os.getenv("PI_SERVER_NAME") or "PTSG-1MMPDPdb01"
        base_url = args.base_url or os.getenv("PI_WEBAPI_URL")
        if not base_url:
            # Construct from server if not provided
            base_url = f"https://{server}/piwebapi"

        print("PI Web API Check")
        print("=" * 50)
        print(f"Base URL : {base_url}")
        print(f"Server   : {server}")

        client = PIWebAPIClient(base_url=base_url, auth_mode="windows", timeout=args.timeout, verify_ssl=not args.insecure)
        ok, info = client.health_check()
        if ok:
            print("Health   : OK")
        else:
            print(f"Health   : FAIL ({info})")
            # No need to proceed to tag fetch, but continue if user passed a tag explicitly

        if args.tag:
            print(f"\nResolving tag: {args.tag}")
            try:
                webid = client.resolve_point_webid(server, args.tag)
                print(f"WebId    : {webid if webid else 'NOT FOUND'}")
            except Exception as e:
                print(f"Resolve  : ERROR ({e})")
                webid = None

            print("\nFetching small window via Web API (if possible)…")
            try:
                df = fetch_tags_via_webapi(
                    tags=[args.tag],
                    server=server,
                    start=args.start,
                    end=args.end,
                    step=args.step,
                    base_url=base_url,
                    timeout=args.timeout,
                    verify_ssl=not args.insecure,
                    max_workers=1,
                    qps=1.0,
                    retries=0,
                )
                rows = len(df)
                uniq = (df["tag"].nunique() if (rows and "tag" in df.columns) else 0)
                print(f"Rows     : {rows}")
                print(f"Tags     : {uniq}")
                if rows:
                    # Print first few timestamps/values for confirmation
                    try:
                        head = df.head(5)[["time", "value"]]
                        print(head.to_string(index=False))
                    except Exception:
                        pass
            except Exception as e:
                print(f"Fetch    : ERROR ({e})")
        return

    if args.cmd == "db-status":
        from .auto_scan import AutoScanner
        import json
        
        # Initialize scanner for database access
        scanner = AutoScanner(cfg, db_path=args.db_path)
        
        # Cleanup if requested
        if args.cleanup_days:
            cleaned = scanner.db.cleanup_old_data(args.cleanup_days)
            print(f"Cleaned up {cleaned:,} records older than {args.cleanup_days} days")
        
        # Get database status
        status = scanner.get_database_status()
        
        print("TURBOPREDICT X PROTEAN Database Status")
        print("=" * 50)
        print(f"Database: {status['database_path']}")
        print(f"Total records: {status['total_records']:,}")
        print(f"Metadata records: {status['metadata_records']:,}")
        print(f"Unique tags: {status['unique_tags']}")
        print(f"Unique plants: {status['unique_plants']}")
        print(f"Unique units: {status['unique_units']}")
        
        if status['earliest_data'] and status['latest_data']:
            print(f"Data range: {status['earliest_data']} to {status['latest_data']}")
        
        print(f"\nRecent Activity (last 24h):")
        if status['recent_activity']:
            for activity in status['recent_activity'][:10]:
                print(f"   * {activity['tag']}: {activity['records']:,} records, latest: {activity['latest_data']}")
        else:
            print("   No recent activity")
        
        # Show specific tag details if requested
        if args.tag:
            info = scanner.db.get_data_freshness_info(args.tag, args.plant, args.unit)
            print(f"\nTag Details: {args.tag}")
            print(f"   * Latest data: {info['latest_data_timestamp']}")
            print(f"   * Records: {info['local_record_count']:,}")
            print(f"   * Age: {info['data_age_hours']:.1f} hours" if info['data_age_hours'] else "N/A")
            print(f"   * Is stale: {info['is_stale']}")
        
        return


if __name__ == "__main__":  # pragma: no cover
    main()
