#!/usr/bin/env python3
"""
Auto-commit script for TURBOPREDICT X PROTEAN
Automatically commits and pushes changes every hour
"""

import subprocess
import sys
import os
from datetime import datetime
import json

def run_command(cmd, cwd=None):
    """Run a command and return the result"""
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=300
        )
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", "Command timed out"
    except Exception as e:
        return False, "", str(e)

def get_git_status():
    """Check if there are any changes to commit"""
    success, stdout, stderr = run_command("git status --porcelain")
    if not success:
        print(f"Error checking git status: {stderr}")
        return False, []

    changes = [line.strip() for line in stdout.split('\n') if line.strip()]
    return len(changes) > 0, changes

def commit_and_push():
    """Commit and push changes"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Add all changes
    success, stdout, stderr = run_command("git add .")
    if not success:
        print(f"Error adding files: {stderr}")
        return False

    # Create commit message
    commit_msg = f"""Hourly auto-commit: {timestamp}

Automated backup of TURBOPREDICT X PROTEAN system changes."""

    # Commit changes
    success, stdout, stderr = run_command(f'git commit -m "{commit_msg}"')
    if not success:
        if "nothing to commit" in stderr:
            print("No changes to commit")
            return True
        print(f"Error committing: {stderr}")
        return False

    print(f"Committed changes at {timestamp}")

    # Push to remote
    success, stdout, stderr = run_command("git push origin master")
    if not success:
        print(f"Error pushing: {stderr}")
        return False

    print(f"Successfully pushed changes at {timestamp}")
    return True

def log_activity(status, message):
    """Log auto-commit activity"""
    log_file = "auto_commit.log"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] {status}: {message}\n")
    except Exception as e:
        print(f"Error writing to log: {e}")

def main():
    """Main auto-commit function"""
    print("=== TURBOPREDICT X PROTEAN Auto-Commit ===")

    # Change to project directory
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(project_dir)
    print(f"Working directory: {project_dir}")

    # Check for changes
    has_changes, changes = get_git_status()

    if not has_changes:
        message = "No changes detected"
        print(message)
        log_activity("INFO", message)
        return True

    print(f"Found {len(changes)} changes:")
    for change in changes[:10]:  # Show first 10 changes
        print(f"  {change}")
    if len(changes) > 10:
        print(f"  ... and {len(changes) - 10} more")

    # Commit and push
    if commit_and_push():
        message = f"Successfully committed and pushed {len(changes)} changes"
        print(message)
        log_activity("SUCCESS", message)
        return True
    else:
        message = "Failed to commit and push changes"
        print(message)
        log_activity("ERROR", message)
        return False

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nAuto-commit interrupted by user")
        log_activity("INTERRUPTED", "Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        message = f"Unexpected error: {e}"
        print(message)
        log_activity("ERROR", message)
        sys.exit(1)