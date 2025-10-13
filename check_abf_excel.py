"""Check ABF Excel file configuration"""
import xlwings as xw
from pathlib import Path

excel_path = Path("excel/ABFSB/ABFSB_Automation_Master.xlsx")

print(f"Opening: {excel_path}")
app = xw.App(visible=False, add_book=False)
app.display_alerts = False

wb = app.books.open(str(excel_path.absolute()), update_links=False, read_only=True)

print(f"\nSheets in workbook:")
for sht in wb.sheets:
    print(f"  - {sht.name}")

# Check if DL_WORK exists
try:
    sht = wb.sheets["DL_WORK"]
    print(f"\nDL_WORK sheet found!")
    print(f"  A2 formula: {sht.range('A2').formula}")
    print(f"  B2 formula: {sht.range('B2').formula}")
    print(f"  A2 value: {sht.range('A2').value}")
    print(f"  B2 value: {sht.range('B2').value}")

    # Check used range
    try:
        used_range = sht.used_range
        print(f"  Used range: {used_range.address}")
    except:
        pass
except Exception as e:
    print(f"\nDL_WORK sheet NOT found: {e}")

wb.close()
app.quit()
