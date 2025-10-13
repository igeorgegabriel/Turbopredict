"""Test copying formulas down to get full array"""
import xlwings as xw
from pathlib import Path
import time

print("=" * 70)
print("TESTING FORMULA COPY-DOWN STRATEGY")
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
    sht = wb.sheets["TEST_COPYDOWN"]
    sht.clear()
except:
    sht = wb.sheets.add("TEST_COPYDOWN")

tag = "PCFS.K-12-01.12PI-007.PV"
server = "\\\\PTSG-1MMPDPdb01"

print("\nWaiting 10s for PI DataLink to initialize...")
time.sleep(10)

# Request last 2 hours at 0.1h intervals = ~20 rows
formula_time = f'=PISampDat("{tag}","-2h","*","-0.1h",1,"{server}")'
formula_value = f'=PISampDat("{tag}","-2h","*","-0.1h",0,"{server}")'

print(f"\n[TEST] Copy-Down Strategy (expecting ~20 rows)")
print(f"  Putting formulas in A2:B2")
print(f"  Then copying down to A2:B50")

# Put base formulas
sht.range("A2").formula = formula_time
sht.range("B2").formula = formula_value

print("\n[INFO] Copying formulas down...")
# Copy down to row 50 (should be enough for ~20 data points)
sht.range("A2").copy(sht.range("A2:A50"))
sht.range("B2").copy(sht.range("B2:B50"))

print("\n[INFO] Forcing calculation...")
app.api.CalculateFull()
time.sleep(10)
app.api.CalculateUntilAsyncQueriesDone()

print("\n[INFO] Waiting 30s for all formulas to calculate...")
time.sleep(30)

# Check results
print("\n" + "=" * 70)
print("RESULTS:")
print("=" * 70)

try:
    data = sht.range("A2:B50").value
    if data:
        # Find valid rows (both columns have data)
        valid_rows = []
        for i, row in enumerate(data):
            if row[0] is not None and row[1] is not None:
                # Check if it's actual data, not error
                if not isinstance(row[0], str) or not row[0].startswith('#'):
                    valid_rows.append((i+2, row))  # i+2 because row 1 is header

        print(f"Valid data rows: {len(valid_rows)}")

        if valid_rows:
            print(f"\nFirst 5 rows:")
            for row_num, row in valid_rows[:5]:
                print(f"  Row {row_num}: timestamp={row[0]}, value={row[1]}")

            print(f"\nLast 5 rows:")
            for row_num, row in valid_rows[-5:]:
                print(f"  Row {row_num}: timestamp={row[0]}, value={row[1]}")

            # Check for duplicates
            timestamps = [r[1][0] for r in valid_rows]
            unique_timestamps = len(set(timestamps))
            print(f"\nUnique timestamps: {unique_timestamps}/{len(valid_rows)}")
            if unique_timestamps < len(valid_rows):
                print("[WARNING] Duplicate timestamps detected - formula might not iterate!")
        else:
            print("\n[ERROR] No valid rows!")
            print(f"A2: {sht.range('A2').value}")
            print(f"B2: {sht.range('B2').value}")
            print(f"A3: {sht.range('A3').value}")
            print(f"B3: {sht.range('B3').value}")
except Exception as e:
    print(f"[ERROR] {e}")

print("\n" + "=" * 70)
print("Excel left open - check TEST_COPYDOWN sheet")
print("=" * 70)

input("\nPress Enter to close...")
wb.close(save_as=False)
app.quit()
