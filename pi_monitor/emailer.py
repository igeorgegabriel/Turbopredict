from __future__ import annotations

from pathlib import Path
from typing import Iterable
import smtplib
from email.message import EmailMessage


def send_email(
    smtp_host: str,
    smtp_port: int,
    sender: str,
    recipients: Iterable[str],
    subject: str,
    body: str,
    attachments: list[Path] | None = None,
    username: str | None = None,
    password: str | None = None,
    use_tls: bool = True,
) -> None:
    """Send a plain-text email with optional attachments.

    This is a minimal implementation suitable for internal SMTP relays.
    """
    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = ", ".join([r for r in recipients if r])
    msg["Subject"] = subject
    msg.set_content(body)

    for path in attachments or []:
        p = Path(path)
        if not p.exists():
            continue
        data = p.read_bytes()
        msg.add_attachment(data, maintype="application", subtype="octet-stream", filename=p.name)

    with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as smtp:
        if use_tls:
            try:
                smtp.starttls()
            except Exception:
                pass
        if username and password:
            smtp.login(username, password)
        smtp.send_message(msg)

