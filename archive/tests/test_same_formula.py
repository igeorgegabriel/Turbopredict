"""Test using SAME formula in both columns (like working Excel files)"""
import xlwings as xw
from pathlib import Path
import time

print("=" * 70)
print("TESTING SAME-FORMULA STRATEGY (WORKING EXCEL APPROACH)")
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
    sht = wb.sheets["TEST_SAME_FORMULA"]
    sht.clear()
except:
    sht = wb.sheets.add("TEST_SAME_FORMULA")

tag = "PCFS.K-12-01.12PI-007.PV"
server = "\\\\PTSG-1MMPDPdb01"

print("\nWaiting 10s for PI DataLink to initialize...")
time.sleep(10)

# Use SAME formula in both columns (mode=1 for timestamps)
formula = f'=PISampDat("{tag}","-2h","*","-0.1h",1,"{server}")'

print(f"\n[TEST] Same Formula Strategy")
print(f"  Formula (A2 and B2): {formula}")
print(f"  Filling range: A2:B30 (~20 expected rows for 2h at 0.1h)")

# Fill entire range with same formula
sht.range("A2:B30").formula = formula

print("\n[INFO] Forcing calculation...")
app.api.CalculateFull()
time.sleep(15)
app.api.CalculateUntilAsyncQueriesDone()

print("\n[INFO] Waiting 30s for formulas to calculate...")
time.sleep(30)

# Check results
print("\n" + "=" * 70)
print("RESULTS:")
print("=" * 70)

try:
    data = sht.range("A2:B30").value
    if data:
        # Find valid rows
        valid_rows = [(i+2, row) for i, row in enumerate(data)
                     if row[0] is not None and row[1] is not None
                     and (not isinstance(row[0], str) or not row[0].startswith('#'))]

        print(f"Valid data rows: {len(valid_rows)}/29")

        if valid_rows:
            print(f"\nFirst 5 rows:")
            for row_num, row in valid_rows[:5]:
                print(f"  Row {row_num}: time={row[0]}, value={row[1]}")

            print(f"\nLast 5 rows:")
            for row_num, row in valid_rows[-5:]:
                print(f"  Row {row_num}: time={row[0]}, value={row[1]}")

            # Check if columns are different
            timestamps = [r[1][0] for r in valid_rows]
            values = [r[1][1] for r in valid_rows]

            print(f"\nColumn A type: {type(timestamps[0]).__name__}")
            print(f"Column B type: {type(values[0]).__name__}")

            if all(isinstance(t, type(v)) for t, v in zip(timestamps, values)):
                print("\n[WARNING] Both columns have same type - might be duplicates!")
            else:
                print("\n[SUCCESS] Columns have different types - working correctly!")
        else:
            print("\n[ERROR] No valid rows!")
            print(f"A2: {sht.range('A2').value}")
            print(f"B2: {sht.range('B2').value}")
except Exception as e:
    print(f"[ERROR] {e}")

print("\n" + "=" * 70)
print("Excel left open - check TEST_SAME_FORMULA sheet")
print("=" * 70)

input("\nPress Enter to close...")
wb.close(save_as=False)
app.quit()
