"""Debug ABF data fetch"""
import xlwings as xw
from pathlib import Path
import time

excel_path = Path("excel/ABFSB/ABFSB_Automation_Master.xlsx")
tag = "ABF.07-MT001.FI-07001.PV"
server = "\\\\PTSG-1MMPDPdb01"

print("Opening Excel...")
app = xw.App(visible=False, add_book=False)
app.display_alerts = False

wb = app.books.open(str(excel_path.absolute()), update_links=False, read_only=False)
time.sleep(5)

sht = wb.sheets["DL_WORK"]

# Clear and set formula
print("Clearing and setting new formulas...")
try:
    sht.used_range.clear_contents()
except:
    sht.range("A1:B1000").clear_contents()

time.sleep(2)

formula_time = f'=PISampDat("{tag}","-12h","*","-0.1h",1,"{server}")'
formula_value = f'=PISampDat("{tag}","-12h","*","-0.1h",0,"{server}")'

sht.range("A2:A120").api.FormulaArray = formula_time
sht.range("B2:B120").api.FormulaArray = formula_value

wb.save()
time.sleep(2)

# Refresh
print("Refreshing...")
wb.api.RefreshAll()
time.sleep(15)
app.api.CalculateUntilAsyncQueriesDone()
time.sleep(10)

# Check results
print("\n=== Results ===")
print(f"A2 formula: {sht.range('A2').formula}")
print(f"B2 formula: {sht.range('B2').formula}")
print(f"A2 value: {sht.range('A2').value} (type: {type(sht.range('A2').value)})")
print(f"B2 value: {sht.range('B2').value} (type: {type(sht.range('B2').value)})")

# Check if there are errors
a2_val = sht.range('A2').value
if isinstance(a2_val, str) and a2_val.startswith('#'):
    print(f"\n[ERROR] Cell A2 has error: {a2_val}")

# Check first few rows
print(f"\nFirst 5 rows:")
data = sht.range("A2:B6").value
for i, row in enumerate(data, start=2):
    print(f"  Row {i}: {row}")

wb.close()
app.quit()
