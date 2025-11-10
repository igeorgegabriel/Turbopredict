"""Debug: Check what data is actually in DL_WORK after refresh"""
import xlwings as xw
from pathlib import Path
import time

excel_path = Path("excel/PCFS/PCFS_Automation.xlsx")
unit_tag = "PCFS.K-16-01.16SI-501B.PV"
server = "\\\\PTSG-1MMPDPdb01"

print("Opening Excel...")
app = xw.App(visible=False, add_book=False)
app.display_alerts = False

wb = app.books.open(str(excel_path.absolute()), update_links=False, read_only=False)

print("Waiting for PI DataLink...")
time.sleep(5)

sht = wb.sheets["DL_WORK"]

# Update formula
print(f"Updating formula for: {unit_tag}")
formula = f'=PISampDat("{unit_tag}","-26h","*","-0.1h",1,"{server}")'
sht.range("A2:B2").formula = formula

# Refresh
print("Refreshing...")
wb.api.RefreshAll()
time.sleep(15)
app.api.CalculateUntilAsyncQueriesDone()
time.sleep(10)

# Check what's in the cells
print("\n=== Checking DL_WORK content ===")
print(f"A2 formula: {sht.range('A2').formula}")
print(f"B2 formula: {sht.range('B2').formula}")
print(f"\nA2 value: {sht.range('A2').value}")
print(f"B2 value: {sht.range('B2').value}")
print(f"A3 value: {sht.range('A3').value}")
print(f"B3 value: {sht.range('B3').value}")

# Check used range
try:
    used_range = sht.used_range
    print(f"\nUsed range: {used_range.address}")
    print(f"Used range rows: {used_range.rows.count}")
except Exception as e:
    print(f"Used range error: {e}")

# Try reading first 10 rows
print("\n=== First 10 rows of data ===")
data = sht.range("A2:B11").value
for i, row in enumerate(data, start=2):
    print(f"Row {i}: {row}")

# Check if formulas are calculating
print(f"\n=== Formula status ===")
a2_text = str(sht.range("A2").value)
if a2_text.startswith('#'):
    print(f"ERROR in A2: {a2_text}")
else:
    print(f"A2 appears to have data: {type(sht.range('A2').value)}")

wb.close()
app.quit()
