"""Test different PI formula variations to find what works"""
import xlwings as xw
from pathlib import Path
import time

print("=" * 70)
print("TESTING DIFFERENT PI FORMULA VARIATIONS")
print("=" * 70)

app = xw.App(visible=True, add_book=False)
app.display_alerts = False
app.api.AutomationSecurity = 1

# Connect PI DataLink
for c in app.api.COMAddIns:
    try:
        if 'PI DataLink' in str(getattr(c, 'ProgId', '')):
            c.Connect = True
            print(f"[OK] PI DataLink connected")
    except Exception:
        pass

wb = app.books.open(str(Path("excel/PCFS/PCFS_Automation.xlsx").absolute()))

try:
    sht = wb.sheets["TEST_FORMULAS"]
    sht.clear()
except:
    sht = wb.sheets.add("TEST_FORMULAS")

tag = "PCFS.K-12-01.12PI-007.PV"
server = "\\\\PTSG-1MMPDPdb01"

print("\nWaiting 10s for PI DataLink to initialize...")
time.sleep(10)

# Test different formula variations
formulas = [
    ("A2", f'=PISampDat("{tag}","-2h","*","-0.1h",1,"{server}")', "Mode=1 (timestamps on)"),
    ("D2", f'=PISampDat("{tag}","-2h","*","-0.1h",0,"{server}")', "Mode=0 (values only)"),
    ("G2", f'=PISampDat("{tag}","-2h","*","-0.1h",,"{server}")', "Mode=default (empty)"),
]

for cell, formula, description in formulas:
    print(f"\n[TEST] {description}")
    print(f"  Cell: {cell}")
    print(f"  Formula: {formula}")

    sht.range(cell).formula = formula

print("\n[INFO] Forcing calculation...")
app.api.CalculateFull()
time.sleep(5)
app.api.CalculateUntilAsyncQueriesDone()

print("\n[INFO] Waiting 30s for data to appear...")
time.sleep(30)

# Check results
print("\n" + "=" * 70)
print("RESULTS:")
print("=" * 70)

for cell, formula, description in formulas:
    print(f"\n{description} ({cell}):")

    # Check main cell
    val = sht.range(cell).value
    print(f"  {cell}: {val} (type: {type(val).__name__})")

    # Check next cell (should have value if 2-column)
    next_col = chr(ord(cell[0]) + 1) + cell[1:]
    next_val = sht.range(next_col).value
    print(f"  {next_col}: {next_val}")

    # Try spill
    try:
        spill = sht.range(cell).expand().value
        if spill and isinstance(spill, (list, tuple)) and len(spill) > 0:
            print(f"  Spill: {len(spill)} rows")
            print(f"  First row: {spill[0] if len(spill) > 0 else 'N/A'}")
            print(f"  Second row: {spill[1] if len(spill) > 1 else 'N/A'}")
        else:
            print(f"  Spill: None or empty")
    except Exception as e:
        print(f"  Spill error: {e}")

print("\n" + "=" * 70)
print("Excel left open - check the TEST_FORMULAS sheet")
print("Look for which formula variant produces 2 columns")
print("=" * 70)

input("\nPress Enter to close...")
wb.close(save_as=False)
app.quit()
