"""Test PICompDat instead of PISampDat - might return arrays"""
import xlwings as xw
from pathlib import Path
import time

print("=" * 70)
print("TESTING PICompDat (Compressed Data)")
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
    sht = wb.sheets["TEST_PICOMP"]
    sht.clear()
except:
    sht = wb.sheets.add("TEST_PICOMP")

tag = "PCFS.K-12-01.12PI-007.PV"
server = "\\\\PTSG-1MMPDPdb01"

print("\nWaiting 10s for PI DataLink to initialize...")
time.sleep(10)

# Try PICompDat which returns time-value pairs
formula = f'=PICompDat("{tag}","-2h","*",1,"{server}")'
print(f"\n[TEST] PICompDat Formula")
print(f"  Cell: A2")
print(f"  Formula: {formula}")

sht.range("A2").formula = formula

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

# Check main cell
val = sht.range("A2").value
print(f"A2: {val} (type: {type(val).__name__})")

# Check next cells
for col in ['B', 'C', 'D']:
    next_val = sht.range(f"{col}2").value
    print(f"{col}2: {next_val}")

# Try expanding
try:
    spill = sht.range("A2").expand().value
    if spill and isinstance(spill, (list, tuple)):
        print(f"\nSpill detected: {len(spill)} rows x {len(spill[0]) if isinstance(spill[0], (list, tuple)) else 1} cols")
        print(f"First 3 rows:")
        for i, row in enumerate(spill[:3]):
            print(f"  {i+1}: {row}")
    else:
        print(f"\nNo spill detected")
except Exception as e:
    print(f"\nSpill error: {e}")

print("\n" + "=" * 70)
print("Excel left open - check the TEST_PICOMP sheet")
print("PICompDat should return 2-column array (timestamp, value)")
print("=" * 70)

input("\nPress Enter to close...")
wb.close(save_as=False)
app.quit()
