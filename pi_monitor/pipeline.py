from __future__ import annotations

from pathlib import Path
from dataclasses import asdict
from typing import Optional
import datetime as dt

from .config import Config
from .excel_refresh import refresh_excel_safe
from .ingest import load_latest_frame, write_parquet
from .anomaly import add_anomalies
from .plotting import plot_anomalies
from .emailer import send_email


def run_pipeline(
    cfg: Config,
    *,
    refresh_excel: bool = True,
    send_mail: bool = True,
    plot_path: Path | None = None,
    show_plot: bool = False,
) -> Path:
    """Run: refresh (optional) -> ingest -> anomaly -> write parquet -> email.

    Returns the Parquet path written.
    """
    p = cfg.paths

    before = None
    if refresh_excel:
        # Peek at current latest timestamp (before refresh), if possible
        try:
            before = load_latest_frame(p.xlsx_path, unit=cfg.unit)["time"].max()
            print(f"Before refresh latest: {before}")
        except Exception:
            print("Before refresh: couldn't parse (will proceed).")
        refresh_excel_safe(p.xlsx_path)

    df = load_latest_frame(p.xlsx_path, unit=cfg.unit)
    df = add_anomalies(df, roll=cfg.roll, drop_pct=cfg.drop_pct)

    out = write_parquet(df, p.parquet_path)

    latest = df["time"].max() if not df.empty else None
    print(f"After refresh latest : {latest}")
    try:
        print(df.tail(3))
    except Exception:
        pass

    if before and latest and latest <= before:
        print("Note: Excel saved but timestamp did not advance.")
        print("      DataLink may not have pulled new PI data yet.")

    # Optional plot
    if plot_path is None and cfg.paths.plot_path:
        plot_path = cfg.paths.plot_path
    try:
        plot_anomalies(df, save_to=plot_path, show=show_plot)
        if plot_path:
            print(f"Saved plot: {plot_path}")
    except Exception as e:
        print(f"[plot] Failed to create plot: {e}")

    if send_mail:
        n_alerts = int(df["alert"].sum()) if "alert" in df.columns else 0
        subject = f"PI Monitor: {n_alerts} alerts @ {latest}"
        body_lines = [
            "PI Data Monitor Report",
            "",
            f"Latest timestamp: {latest}",
            f"Rows: {len(df):,}",
            f"Alerts: {n_alerts:,}",
            "",
            "Config:",
            str(asdict(cfg.email)),
            str(asdict(cfg.paths)),
        ]
        try:
            send_email(
                smtp_host=cfg.email.smtp_host,
                smtp_port=cfg.email.smtp_port,
                sender=cfg.email.sender,
                recipients=cfg.email.recipients,
                subject=subject,
                body="\n".join(body_lines),
                attachments=[out] + ([plot_path] if plot_path else []),
                username=cfg.email.username,
                password=cfg.email.password,
                use_tls=cfg.email.use_tls,
            )
        except Exception as e:
            # Fail soft in scaffold
            print(f"[email] Failed to send email: {e}")

    return out
