"""
Email sender module for sending PDF reports
"""
import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from pathlib import Path
from typing import Optional, List
import logging

logger = logging.getLogger(__name__)


def send_pdf_report(
    pdf_path: Path,
    recipient_email: str = "george.gabrielujai@petronas.com.my",
    smtp_server: Optional[str] = None,
    smtp_port: Optional[int] = None,
    sender_email: Optional[str] = None,
    use_auth: bool = False,
    username: Optional[str] = None,
    password: Optional[str] = None
) -> bool:
    """
    Send PDF report via email using Office365 SMTP.

    Args:
        pdf_path: Path to PDF file
        recipient_email: Recipient email address
        smtp_server: SMTP server (default: smtp.office365.com)
        smtp_port: SMTP port (default: 587 with STARTTLS)
        sender_email: Sender email (default: george.gabrielujai@petronas.com.my)
        use_auth: Whether to use SMTP authentication (auto-enabled for Office365)
        username: SMTP username (defaults to sender_email)
        password: SMTP password (required for Office365 - set SMTP_PASSWORD env var)

    Environment Variables:
        SMTP_SERVER: SMTP server address (default: smtp.office365.com)
        SMTP_PORT: SMTP port (default: 587)
        SMTP_USERNAME: Username for authentication (default: sender email)
        SMTP_PASSWORD: Password for authentication (REQUIRED for Office365)
        SENDER_EMAIL: Sender email address

    Returns:
        True if email sent successfully, False otherwise
    """
    try:
        # Default SMTP settings - Office365 for PETRONAS email
        if smtp_server is None:
            smtp_server = os.getenv('SMTP_SERVER', 'smtp.office365.com')
        if smtp_port is None:
            smtp_port = int(os.getenv('SMTP_PORT', '587'))  # 587 for STARTTLS
        if sender_email is None:
            sender_email = os.getenv('SENDER_EMAIL', 'george.gabrielujai@petronas.com.my')

        # Office365 requires authentication
        if 'office365' in smtp_server.lower():
            use_auth = True
            if username is None:
                username = os.getenv('SMTP_USERNAME', sender_email)
            if password is None:
                password = os.getenv('SMTP_PASSWORD', None)

            # Check if password is provided
            if password is None:
                logger.warning("Office365 SMTP requires authentication")
                print("[EMAIL] Skipped - Office365 requires authentication")
                print("[EMAIL] Set environment variable: SMTP_PASSWORD=your-email-password")
                print("[EMAIL] Or use app-specific password from Microsoft account")
                return False

        # Verify PDF exists
        if not pdf_path.exists():
            logger.error(f"PDF file not found: {pdf_path}")
            return False

        # Create message
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = f"TurboPredict Anomaly Report - {pdf_path.stem}"

        # Email body
        body = f"""
Hello,

This is an automated email from TurboPredict system.

Attached is the anomaly analysis report generated on {pdf_path.stat().st_mtime}.

Report Details:
- Filename: {pdf_path.name}
- Size: {pdf_path.stat().st_size / 1024 / 1024:.2f} MB
- Path: {pdf_path}

This report contains anomaly detection results for all monitored units.

---
TurboPredict X Protean - Industrial PI Data Monitoring System
"""

        msg.attach(MIMEText(body, 'plain'))

        # Attach PDF
        with open(pdf_path, 'rb') as f:
            attachment = MIMEBase('application', 'pdf')
            attachment.set_payload(f.read())
            encoders.encode_base64(attachment)
            attachment.add_header(
                'Content-Disposition',
                f'attachment; filename= {pdf_path.name}'
            )
            msg.attach(attachment)

        # Send email
        logger.info(f"Connecting to SMTP server: {smtp_server}:{smtp_port}")

        with smtplib.SMTP(smtp_server, smtp_port, timeout=30) as server:
            # Use TLS if port 587
            if smtp_port == 587:
                server.starttls()

            # Authenticate if required
            if use_auth and username and password:
                logger.info(f"Authenticating as {username}")
                server.login(username, password)

            # Send email
            logger.info(f"Sending PDF report to {recipient_email}")
            server.send_message(msg)
            logger.info(f"Email sent successfully!")

        return True

    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False


def send_multiple_pdfs(
    pdf_paths: List[Path],
    recipient_email: str = "george.gabrielujai@petronas.com.my",
    **kwargs
) -> bool:
    """
    Send multiple PDF reports in a single email.

    Args:
        pdf_paths: List of PDF file paths
        recipient_email: Recipient email address
        **kwargs: Additional SMTP settings

    Returns:
        True if email sent successfully, False otherwise
    """
    try:
        smtp_server = kwargs.get('smtp_server', os.getenv('SMTP_SERVER', 'mail.petronas.com.my'))
        smtp_port = kwargs.get('smtp_port', int(os.getenv('SMTP_PORT', '25')))
        sender_email = kwargs.get('sender_email', os.getenv('SENDER_EMAIL', 'turbopredict@petronas.com.my'))

        # Create message
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = f"TurboPredict Anomaly Reports - {len(pdf_paths)} Files"

        # Email body
        body = f"""
Hello,

This is an automated email from TurboPredict system.

Attached are {len(pdf_paths)} anomaly analysis reports.

Reports:
"""
        for pdf_path in pdf_paths:
            if pdf_path.exists():
                body += f"\n- {pdf_path.name} ({pdf_path.stat().st_size / 1024 / 1024:.2f} MB)"

        body += """

---
TurboPredict X Protean - Industrial PI Data Monitoring System
"""

        msg.attach(MIMEText(body, 'plain'))

        # Attach all PDFs
        for pdf_path in pdf_paths:
            if not pdf_path.exists():
                logger.warning(f"Skipping missing PDF: {pdf_path}")
                continue

            with open(pdf_path, 'rb') as f:
                attachment = MIMEBase('application', 'pdf')
                attachment.set_payload(f.read())
                encoders.encode_base64(attachment)
                attachment.add_header(
                    'Content-Disposition',
                    f'attachment; filename= {pdf_path.name}'
                )
                msg.attach(attachment)

        # Send email
        logger.info(f"Connecting to SMTP server: {smtp_server}:{smtp_port}")

        with smtplib.SMTP(smtp_server, smtp_port, timeout=30) as server:
            if smtp_port == 587:
                server.starttls()

            if kwargs.get('use_auth') and kwargs.get('username') and kwargs.get('password'):
                server.login(kwargs['username'], kwargs['password'])

            logger.info(f"Sending {len(pdf_paths)} PDF reports to {recipient_email}")
            server.send_message(msg)
            logger.info(f"Email sent successfully!")

        return True

    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False
