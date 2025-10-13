"""Test dual-formula strategy: timestamps in A, values in B"""
import xlwings as xw
from pathlib import Path
import time

print("=" * 70)
print("TESTING DUAL-FORMULA STRATEGY")
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
    sht = wb.sheets["TEST_DUAL"]
    sht.clear()
except:
    sht = wb.sheets.add("TEST_DUAL")

tag = "PCFS.K-12-01.12PI-007.PV"
server = "\\\\PTSG-1MMPDPdb01"

print("\nWaiting 10s for PI DataLink to initialize...")
time.sleep(10)

# Two formulas side-by-side
formula_time = f'=PISampDat("{tag}","-2h","*","-0.1h",1,"{server}")'
formula_value = f'=PISampDat("{tag}","-2h","*","-0.1h",0,"{server}")'

print(f"\n[TEST] Dual Formula Strategy")
print(f"  A2 (timestamps): {formula_time}")
print(f"  B2 (values):     {formula_value}")

sht.range("A2").formula = formula_time
sht.range("B2").formula = formula_value

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

# Check expanded range
try:
    data = sht.range("A2:B200").value  # Read first 200 rows
    if data:
        # Filter out None rows
        valid_rows = [r for r in data if r[0] is not None and r[1] is not None]
        print(f"Valid rows with both timestamp and value: {len(valid_rows)}")

        if valid_rows:
            print(f"\nFirst 5 rows:")
            for i, row in enumerate(valid_rows[:5]):
                print(f"  {i+1}: {row}")

            print(f"\nLast 5 rows:")
            for i, row in enumerate(valid_rows[-5:]):
                print(f"  {len(valid_rows)-4+i}: {row}")
        else:
            print("\n[ERROR] No valid rows found!")
            print(f"A2: {sht.range('A2').value}")
            print(f"B2: {sht.range('B2').value}")
    else:
        print("[ERROR] No data returned")
except Exception as e:
    print(f"[ERROR] Reading data: {e}")

print("\n" + "=" * 70)
print("Excel left open - check the TEST_DUAL sheet")
print("Should see timestamps in column A, values in column B")
print("=" * 70)

input("\nPress Enter to close...")
wb.close(save_as=False)
app.quit()
