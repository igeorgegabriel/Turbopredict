"""Check what sheets exist for each unit"""
import xlwings as xw
from pathlib import Path

excel_path = Path("excel/PCFS/PCFS_Automation.xlsx")

app = xw.App(visible=False, add_book=False)
app.display_alerts = False

wb = app.books.open(str(excel_path.absolute()), update_links=False, read_only=True)

print("Sheets in workbook:")
for sht in wb.sheets:
    print(f"  - {sht.name}")

    # Check if it's a DL_ sheet
    if sht.name.startswith("DL_") and sht.name != "DL_WORK":
        # Read tag from A2
        try:
            tag = sht.range("A2").value
            print(f"      Tag: {tag}")
        except:
            pass

wb.close()
app.quit()
