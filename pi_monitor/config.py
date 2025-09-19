from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import os


@dataclass
class EmailConfig:
    smtp_host: str = os.getenv("SMTP_HOST", "smtp.example.com")
    smtp_port: int = int(os.getenv("SMTP_PORT", "587"))
    sender: str = os.getenv("EMAIL_SENDER", "noreply@example.com")
    recipients: list[str] = field(
        default_factory=lambda: [r.strip() for r in os.getenv("EMAIL_RECIPIENTS", "").split(",") if r.strip()]
    )
    username: str | None = os.getenv("SMTP_USERNAME")
    password: str | None = os.getenv("SMTP_PASSWORD")
    use_tls: bool = os.getenv("SMTP_USE_TLS", "true").lower() != "false"


@dataclass
class Paths:
    project_root: Path = Path(__file__).resolve().parents[1]
    raw_data_dir: Path = project_root / "data" / "raw"
    processed_dir: Path = project_root / "data" / "processed"
    reports_dir: Path = project_root / "reports"
    xlsx_path: Path = Path(os.getenv("XLSX_PATH", str(raw_data_dir / "Automation.xlsx")))
    parquet_path: Path = Path(os.getenv("PARQUET_PATH", str(processed_dir / "timeseries.parquet")))
    plot_path: Path = Path(os.getenv("PLOT_PATH", str(reports_dir / "timeseries.png")))


@dataclass
class Config:
    paths: Paths = field(default_factory=Paths)
    email: EmailConfig = field(default_factory=EmailConfig)
    roll: int = int(os.getenv("ROLL", "5"))
    drop_pct: float = float(os.getenv("DROP_PCT", "0.10"))
    # Maximum data age (hours) before considered stale
    max_age_hours: float = float(os.getenv("MAX_AGE_HOURS", "1.0"))
    unit: str | None = os.getenv("UNIT") or None
    plant: str | None = os.getenv("PLANT") or None
    tag: str | None = os.getenv("TAG") or None
