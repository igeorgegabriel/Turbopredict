"""Check what tag is currently in DL_WORK sheet"""
import xlwings as xw
from pathlib import Path

excel_path = Path("excel/PCFS/PCFS_Automation.xlsx")

app = xw.App(visible=False, add_book=False)
app.display_alerts = False

wb = app.books.open(str(excel_path.absolute()), update_links=False, read_only=True)
sht = wb.sheets["DL_WORK"]

# Check the formula in A2 and B2
formula_a2 = sht.range("A2").formula
formula_b2 = sht.range("B2").formula

print(f"A2 formula: {formula_a2}")
print(f"B2 formula: {formula_b2}")

# Extract tag name from formula
if "PISampDat" in formula_a2:
    import re
    match = re.search(r'PISampDat\("([^"]+)"', formula_a2)
    if match:
        tag = match.group(1)
        print(f"\nCurrent tag: {tag}")

wb.close()
app.quit()
