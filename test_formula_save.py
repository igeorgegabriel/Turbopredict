"""Test if formula update + save works"""
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

# Show old formula
print(f"\nBEFORE:")
print(f"A2 formula: {sht.range('A2').formula}")

# Update formula
print(f"\nUpdating formula for: {unit_tag}")
formula = f'=PISampDat("{unit_tag}","-26h","*","-0.1h",1,"{server}")'
sht.range("A2:B2").formula = formula

print(f"\nAFTER (before save):")
print(f"A2 formula: {sht.range('A2').formula}")

# Save
print(f"\nSaving workbook...")
wb.save()
time.sleep(2)

print(f"\nAFTER (after save):")
print(f"A2 formula: {sht.range('A2').formula}")

# Now refresh
print("\nRefreshing...")
wb.api.RefreshAll()
time.sleep(15)
app.api.CalculateUntilAsyncQueriesDone()
time.sleep(5)

print(f"\nAFTER (after refresh):")
print(f"A2 formula: {sht.range('A2').formula}")
print(f"A2 value: {sht.range('A2').value}")
print(f"B2 value: {sht.range('B2').value}")

# Check first few data points
print(f"\nFirst 3 data rows:")
data = sht.range("A2:B4").value
for i, row in enumerate(data, start=2):
    print(f"  Row {i}: {row}")

wb.close()
app.quit()

print("\n[OK] Test complete")
