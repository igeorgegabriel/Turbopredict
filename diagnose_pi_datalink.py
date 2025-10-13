"""Diagnose why PI DataLink is not loading in automation"""
import xlwings as xw
from pathlib import Path
import time

print("=" * 70)
print("PI DATALINK DIAGNOSTIC")
print("=" * 70)

# Open Excel with same settings as automation
print("\n1. Opening Excel (visible=True)...")
app = xw.App(visible=True, add_book=False)
app.display_alerts = False

try:
    # Set low security
    app.api.AutomationSecurity = 1
    print("   Security set to low")
except Exception as e:
    print(f"   Could not set security: {e}")

# Open workbook
excel_path = Path("excel/PCFS/PCFS_Automation.xlsx")
if not excel_path.exists():
    print(f"\nERROR: Excel file not found: {excel_path}")
    print(f"Current directory: {Path.cwd()}")
    app.quit()
    exit(1)

print(f"\n2. Opening workbook: {excel_path}")
wb = app.books.open(str(excel_path), update_links=False)

# List all COM Add-ins
print("\n3. COM Add-ins:")
try:
    found_pi = False
    for c in app.api.COMAddIns:
        try:
            desc = str(getattr(c, 'Description', ''))
            prog = str(getattr(c, 'ProgId', ''))
            connected = getattr(c, 'Connect', False)
            print(f"   - {desc or prog}")
            print(f"     ProgId: {prog}")
            print(f"     Connected: {connected}")

            if ('pi' in (desc or prog).lower() and 'datalink' in (desc or prog).lower()):
                found_pi = True
                if not connected:
                    print(f"     >>> Attempting to connect...")
                    c.Connect = True
                    time.sleep(1)
                    c.Connect = True
                    print(f"     >>> Connect status: {getattr(c, 'Connect', False)}")
        except Exception as e:
            print(f"   Error reading add-in: {e}")

    if not found_pi:
        print("\n   [!] NO PI DataLink COM add-in found!")
except Exception as e:
    print(f"   Error listing COM add-ins: {e}")

# List Excel Add-ins
print("\n4. Excel Add-ins:")
try:
    for addin in app.api.AddIns:
        try:
            name = str(getattr(addin, 'Name', ''))
            installed = getattr(addin, 'Installed', False)
            print(f"   - {name}")
            print(f"     Installed: {installed}")

            if 'pi' in name.lower() and 'datalink' in name.lower():
                if not installed:
                    print(f"     >>> Attempting to install...")
                    addin.Installed = True
                    print(f"     >>> Installed: {getattr(addin, 'Installed', False)}")
        except Exception as e:
            print(f"   Error reading add-in: {e}")
except Exception as e:
    print(f"   Error listing Excel add-ins: {e}")

# Test a PI formula
print("\n5. Testing PI formula...")
try:
    sheet = wb.sheets[0]
    cell = sheet.range("A1")
    cell.formula = '=PICompDat("PCFS.K-12-01.12PI-007.PV","*","\\\\PTSG-1MMPDPdb01")'
    print(f"   Formula: {cell.formula}")

    # Wait and calculate
    print("   Waiting 5s for calculation...")
    time.sleep(5)
    app.api.CalculateFull()
    time.sleep(2)

    value = cell.value
    print(f"   Result: {value}")

    if value and not str(value).startswith('#'):
        print("   [SUCCESS] PI DataLink is working!")
    else:
        print(f"   [FAILED] PI DataLink returned error or empty: {value}")

except Exception as e:
    print(f"   Error testing formula: {e}")

print("\n" + "=" * 70)
print("Diagnostic complete. Excel window left open for inspection.")
print("Check if PI DataLink toolbar is visible in Excel.")
print("Close Excel manually when done.")
print("=" * 70)

input("\nPress Enter to close Excel and exit...")
wb.close(save_as=False)
app.quit()
