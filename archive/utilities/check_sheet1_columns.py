"""Check what units are in Sheet1"""
import xlwings as xw
from pathlib import Path

excel_path = Path("excel/PCFS/PCFS_Automation.xlsx")

app = xw.App(visible=False, add_book=False)
app.display_alerts = False

wb = app.books.open(str(excel_path.absolute()), update_links=False, read_only=True)
sht = wb.sheets["Sheet1"]

# Read headers (row 1)
headers = sht.range("A1:Z1").value

print("Sheet1 columns:")
for i, header in enumerate(headers):
    if header:
        col_letter = chr(65 + i)  # A=65
        print(f"  {col_letter}: {header}")

wb.close()
app.quit()
