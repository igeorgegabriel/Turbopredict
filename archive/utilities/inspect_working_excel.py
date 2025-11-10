"""Inspect how PI DataLink formulas are set up in working Excel file"""
import xlwings as xw
from pathlib import Path

print("Opening working Excel file...")
excel_path = Path("excel/PCFS/PCFS_Automation.xlsx")

app = xw.App(visible=True, add_book=False)
app.display_alerts = False

wb = app.books.open(str(excel_path.absolute()))

# Find sheets with data
print("\n" + "=" * 70)
print("SHEETS IN WORKBOOK:")
print("=" * 70)

for i, sht in enumerate(wb.sheets):
    print(f"\n[{i+1}] {sht.name}")

    # Check if there's data in A1:B10
    try:
        data = sht.range("A1:C5").value
        print(f"  Sample data (A1:C5):")
        for row in data:
            if any(c is not None for c in row):
                print(f"    {row}")

        # Check for formulas in row 2
        formula_a2 = sht.range("A2").formula
        formula_b2 = sht.range("B2").formula

        if formula_a2:
            print(f"\n  A2 formula: {formula_a2}")
        if formula_b2:
            print(f"  B2 formula: {formula_b2}")

        # Check how many rows have data
        try:
            used_range = sht.used_range
            print(f"  Used range: {used_range.address}")
        except:
            pass

    except Exception as e:
        print(f"  Error reading: {e}")

print("\n" + "=" * 70)
input("\nPress Enter to close...")
wb.close(save_as=False)
app.quit()
