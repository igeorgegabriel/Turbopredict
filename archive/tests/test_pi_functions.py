"""Test different PI DataLink functions to find one that returns arrays"""
import xlwings as xw
from pathlib import Path
import time

print("=" * 70)
print("TESTING DIFFERENT PI DATALINK FUNCTIONS")
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
    sht = wb.sheets["TEST_PI_FUNCTIONS"]
    sht.clear()
except:
    sht = wb.sheets.add("TEST_PI_FUNCTIONS")

tag = "PCFS.K-12-01.12PI-007.PV"
server = "\\\\PTSG-1MMPDPdb01"

print("\nWaiting 10s for PI DataLink to initialize...")
time.sleep(10)

# Test different PI functions
tests = [
    ("A1", f'=PIArchDat("{tag}","-2h","*","{server}")', "PIArchDat (archived data)"),
    ("D1", f'=PITimeDat("{tag}","-2h","-1h",10,"{server}")', "PITimeDat (time-based, 10 samples)"),
]

for cell, formula, description in tests:
    print(f"\n[TEST] {description}")
    print(f"  Cell: {cell}")
    print(f"  Formula: {formula}")
    sht.range(cell).formula = formula

print("\n[INFO] Forcing calculation...")
app.api.CalculateFull()
time.sleep(10)
app.api.CalculateUntilAsyncQueriesDone()

print("\n[INFO] Waiting 30s for data to appear...")
time.sleep(30)

# Check results
print("\n" + "=" * 70)
print("RESULTS:")
print("=" * 70)

for cell, formula, description in tests:
    print(f"\n{description} ({cell}):")

    # Try reading 10x10 area
    col_letter = cell[0]
    start_row = int(cell[1:])
    range_str = f"{col_letter}{start_row}:{chr(ord(col_letter)+5)}{start_row+20}"

    try:
        data = sht.range(range_str).value
        if data:
            # Count non-None rows
            valid_rows = [r for r in data if any(c is not None for c in (r if isinstance(r, (list, tuple)) else [r]))]
            print(f"  Valid rows in {range_str}: {len(valid_rows)}")

            if valid_rows:
                print(f"  First 3 rows:")
                for i, row in enumerate(valid_rows[:3]):
                    print(f"    {i+1}: {row}")
        else:
            print(f"  No data")
    except Exception as e:
        print(f"  Error: {e}")

print("\n" + "=" * 70)
print("Excel left open - check TEST_PI_FUNCTIONS sheet")
print("=" * 70)

input("\nPress Enter to close...")
wb.close(save_as=False)
app.quit()
