# Email Setup Guide for TurboPredict

## Quick Setup for Office365 Email

TurboPredict can automatically send PDF reports via email using Office365 SMTP.

### Step 1: Set Environment Variables

```cmd
REM Required - Your email password
set SMTP_PASSWORD=your-email-password

REM Optional - Override defaults if needed
set SMTP_SERVER=smtp.office365.com
set SMTP_PORT=587
set SENDER_EMAIL=george.gabrielujai@petronas.com.my
set SMTP_USERNAME=george.gabrielujai@petronas.com.my
```

### Step 2: Get Your Password

**Option A: Use Your Regular Password**
- Use your PETRONAS email password
- Only works if MFA (Multi-Factor Authentication) is NOT enabled

**Option B: Use App-Specific Password (Recommended)**

If MFA is enabled, you need an app-specific password:

1. Go to: https://account.microsoft.com/security
2. Click "Advanced security options"
3. Under "App passwords", click "Create a new app password"
4. Copy the generated password
5. Use this password in `SMTP_PASSWORD` environment variable

### Step 3: Test Email

Run TurboPredict Option [2] to generate a report and test email:

```cmd
python turbopredict.py
# Select option [2] GENERATE ANOMALY PLOTS & PDF
```

Expected output:
```
[PDF] Consolidated anomaly report: reports\...\ANOMALY_REPORT_20251015_114M.pdf
[TIMING] PDF generation: 69.42s

[EMAIL] Sending PDF report to george.gabrielujai@petronas.com.my...
[EMAIL] ✓ Report sent successfully!
```

### Troubleshooting

#### Error: "Office365 requires authentication"
**Fix**: Set `SMTP_PASSWORD` environment variable
```cmd
set SMTP_PASSWORD=your-password-here
```

#### Error: "Authentication failed"
**Cause**: Wrong password or MFA enabled
**Fix**: Use app-specific password (see Option B above)

#### Error: "[Errno 11001] getaddrinfo failed"
**Cause**: Not connected to internet or DNS issue
**Fix**:
- Check internet connection
- Try: `ping smtp.office365.com`
- Ensure no VPN/firewall blocking port 587

#### Error: "Connection timeout"
**Cause**: Firewall blocking port 587
**Fix**:
- Check corporate firewall settings
- Try port 465 (SSL) instead:
  ```cmd
  set SMTP_PORT=465
  ```

### Alternative SMTP Servers

If Office365 doesn't work, try:

**Gmail (requires app password):**
```cmd
set SMTP_SERVER=smtp.gmail.com
set SMTP_PORT=587
set SENDER_EMAIL=your-gmail@gmail.com
set SMTP_PASSWORD=your-app-specific-password
```

**PETRONAS Internal Mail (if available):**
```cmd
set SMTP_SERVER=mail.petronas.com.my
set SMTP_PORT=25
set SENDER_EMAIL=george.gabrielujai@petronas.com.my
REM Internal mail may not require password
```

### Disable Email Temporarily

To skip email sending without errors:

```cmd
set SMTP_PASSWORD=
```

System will show:
```
[EMAIL] Skipped - Office365 requires authentication
```

PDF reports will still be generated and saved locally.

---

## Configuration Details

### Default Settings

| Setting | Default Value | Description |
|---------|--------------|-------------|
| SMTP_SERVER | smtp.office365.com | Office365 SMTP server |
| SMTP_PORT | 587 | STARTTLS port |
| SENDER_EMAIL | george.gabrielujai@petronas.com.my | From address |
| SMTP_USERNAME | (same as SENDER_EMAIL) | Login username |
| SMTP_PASSWORD | (none) | **REQUIRED** - Your password |

### Security Notes

- **Never commit passwords to git**
- Use app-specific passwords when possible
- Environment variables are session-specific (not saved permanently)
- For permanent setup, add to Windows System Environment Variables

### Permanent Environment Variable Setup

**Option 1: User Environment Variables (Recommended)**

1. Open: Control Panel → System → Advanced system settings
2. Click "Environment Variables"
3. Under "User variables", click "New"
4. Variable name: `SMTP_PASSWORD`
5. Variable value: your-password
6. Click OK

**Option 2: System-wide (Requires Admin)**

```cmd
setx SMTP_PASSWORD "your-password" /M
```

**Option 3: Add to Startup Script**

Create `email_config.bat`:
```cmd
@echo off
set SMTP_PASSWORD=your-password-here
set SENDER_EMAIL=george.gabrielujai@petronas.com.my
```

Run before starting TurboPredict:
```cmd
call email_config.bat
python turbopredict.py
```

---

## Email Report Contents

Each automated email includes:

**Subject:** `TurboPredict Anomaly Report - ANOMALY_REPORT_20251015_114M`

**Body:**
- Report generation timestamp
- Filename and file size
- Report location path
- Brief description

**Attachment:**
- PDF with all anomaly plots (typically 5-10 MB)
- 195 plots across 13 units
- Color-coded anomaly indicators

---

## Support

If issues persist:
1. Check this guide for troubleshooting steps
2. Verify internet/VPN connection
3. Test SMTP manually: `telnet smtp.office365.com 587`
4. Check Windows Event Viewer for detailed errors
5. Review logs in `logs/` directory

---

**Last Updated:** October 2025
