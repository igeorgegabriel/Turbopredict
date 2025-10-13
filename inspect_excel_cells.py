"""Open Excel, inject PI formula, and LEAVE IT OPEN for manual inspection"""
import xlwings as xw
from pathlib import Path
import time

print("=" * 70)
print("EXCEL CELL INSPECTION - MANUAL VERIFICATION")
print("=" * 70)

app = xw.App(visible=True, add_book=False)
app.display_alerts = False
app.api.AutomationSecurity = 1

# Connect PI DataLink
for c in app.api.COMAddIns:
    try:
        prog = str(getattr(c, 'ProgId', ''))
        if prog.strip() == 'PI DataLink':
            c.Connect = True
            print(f"[OK] PI DataLink connected: {c.Connect}")
    except Exception:
        pass

# Open workbook
wb = app.books.open(str(Path("excel/PCFS/PCFS_Automation.xlsx").absolute()))
print("[OK] Workbook opened")

# Wait for PI DataLink
print("[INFO] Waiting 15s for PI DataLink to initialize...")
time.sleep(15)
app.api.CalculateFull()

# Get or create worksheet
try:
    sht = wb.sheets["DL_WORK"]
    sht.clear()
except:
    sht = wb.sheets.add("DL_WORK")

# Inject PI formula
tag = "PCFS.K-12-01.12PI-007.PV"
server = "\\\\PTSG-1MMPDPdb01"
formula = f'=PISampDat("{tag}","-2h","*","-0.1h",1,"{server}")'

print(f"\n[INFO] Injecting formula in A2:")
print(f"  {formula}")

sht.range("A2").formula = formula

# Force calculation
print("\n[INFO] Forcing full calculation...")
app.api.CalculateFull()
time.sleep(3)
app.api.CalculateUntilAsyncQueriesDone()
time.sleep(2)

# Check what's in A2
print("\n[INFO] Waiting 30s for PI DataLink to fetch data...")
time.sleep(30)

a2_value = sht.range("A2").value
print(f"\n[RESULT] Cell A2 value: {a2_value}")
print(f"[RESULT] Cell A2 type: {type(a2_value)}")

# Try to read spill
try:
    spill = sht.range("A2").expand().value
    if spill:
        print(f"\n[RESULT] Spill detected!")
        print(f"  Type: {type(spill)}")
        if isinstance(spill, (list, tuple)):
            print(f"  Rows: {len(spill)}")
            if len(spill) > 0:
                print(f"  First row: {spill[0]}")
                if len(spill) > 1:
                    print(f"  Second row: {spill[1]}")
    else:
        print(f"\n[RESULT] No spill detected - value is None")
except Exception as e:
    print(f"\n[ERROR] Could not read spill: {e}")

# Also check B2 manually
b2_value = sht.range("B2").value
print(f"\n[RESULT] Cell B2 value: {b2_value}")

# Check range A2:B10 manually
try:
    range_vals = sht.range("A2:B10").value
    print(f"\n[RESULT] Range A2:B10:")
    for i, row in enumerate(range_vals, start=2):
        print(f"  Row {i}: {row}")
except Exception as e:
    print(f"\n[ERROR] Could not read range: {e}")

print("\n" + "=" * 70)
print("EXCEL WINDOW IS NOW OPEN FOR YOUR INSPECTION")
print("=" * 70)
print("\nPlease check:")
print("1. Is PI DataLink toolbar visible?")
print("2. What's in cell A2? (should have the formula)")
print("3. Is there data in A2:B downwards?")
print("4. Is there an error like #N/A or #REF!?")
print("5. Try pressing F9 (recalculate) - does data appear?")
print("\n" + "=" * 70)

input("\nPress Enter when done inspecting (will close Excel)...")
wb.close(save_as=False)
app.quit()
