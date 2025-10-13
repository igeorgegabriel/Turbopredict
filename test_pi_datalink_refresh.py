"""Test PI DataLink's built-in refresh capability"""
import xlwings as xw
from pathlib import Path
import time

print("=" * 70)
print("TESTING PI DATALINK REFRESH COMMAND")
print("=" * 70)

app = xw.App(visible=True, add_book=False)
app.display_alerts = False
app.api.AutomationSecurity = 1

# Connect PI DataLink
pi_datalink = None
for c in app.api.COMAddIns:
    try:
        if 'PI DataLink' in str(getattr(c, 'ProgId', '')):
            c.Connect = True
            pi_datalink = c
            print(f"[OK] PI DataLink connected")
            print(f"  ProgId: {getattr(c, 'ProgId', 'N/A')}")
            print(f"  Description: {getattr(c, 'Description', 'N/A')}")
    except Exception as e:
        print(f"Error connecting: {e}")

if pi_datalink:
    print("\nChecking PI DataLink object attributes...")
    try:
        obj = pi_datalink.Object
        print(f"  Object type: {type(obj)}")
        print(f"  Object attributes: {[a for a in dir(obj) if not a.startswith('_')][:20]}")
    except Exception as e:
        print(f"  No Object attribute: {e}")

wb = app.books.open(str(Path("excel/PCFS/PCFS_Automation.xlsx").absolute()))

# Check existing DL_WORK sheet
sht = wb.sheets["DL_WORK"]
print(f"\nDL_WORK sheet info:")
print(f"  Used range: {sht.used_range.address}")
print(f"  A2 formula: {sht.range('A2').formula}")
print(f"  B2 formula: {sht.range('B2').formula}")

# Check first few rows
data = sht.range("A2:B5").value
print(f"\n  First 3 data rows:")
for i, row in enumerate(data[:3]):
    print(f"    Row {i+2}: {row}")

print("\n" + "=" * 70)
print("Try using PI DataLink's toolbar 'Refresh' command manually")
print("Excel left open for inspection")
print("=" * 70)

input("\nPress Enter to close...")
wb.close(save_as=False)
app.quit()
