"""Test formula update using Excel API directly"""
import xlwings as xw
from pathlib import Path
import time

excel_path = Path("excel/PCFS/PCFS_Automation.xlsx")
unit_tag = "PCFS.K-16-01.16SI-501B.PV"
server = "\\\\PTSG-1MMPDPdb01"

print("Opening Excel...")
app = xw.App(visible=True, add_book=False)  # VISIBLE for debugging
app.display_alerts = False

wb = app.books.open(str(excel_path.absolute()), update_links=False, read_only=False)

print("Waiting for PI DataLink...")
time.sleep(5)

sht = wb.sheets["DL_WORK"]

# Show old formula
print(f"\nBEFORE:")
print(f"A2 formula: {sht.range('A2').formula}")
print(f"A2 value: {sht.range('A2').value}")

# Try different methods to update formula
print(f"\nUpdating formula for: {unit_tag}")
formula = f'=PISampDat("{unit_tag}","-26h","*","-0.1h",1,"{server}")'

# Method 1: Clear and then set formula
print("Method: Clear first, then set formula")
sht.range("A2:B2").clear_contents()
time.sleep(1)

sht.range("A2").api.Formula = formula
sht.range("B2").api.Formula = formula
time.sleep(1)

print(f"\nAFTER update:")
print(f"A2 formula: {sht.range('A2').formula}")

# Save
print(f"\nSaving...")
wb.save()
time.sleep(2)

print(f"\nAFTER save:")
print(f"A2 formula: {sht.range('A2').formula}")

input("\nPress Enter to close (check Excel visually)...")

wb.close()
app.quit()
