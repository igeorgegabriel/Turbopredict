#!/usr/bin/env python3
"""Quick test to verify PI DataLink is working"""
import xlwings as xw
import time

print("Testing PI DataLink availability...")
print("=" * 70)

try:
    # Open Excel
    print("\n1. Opening Excel...")
    app = xw.App(visible=True, add_book=False)
    app.display_alerts = False

    print("2. Opening PCFS_Automation.xlsx...")
    wb = app.books.open(r"C:\Users\george.gabrielujai\Documents\CodeX\excel\PCFS_Automation.xlsx")

    print("3. Checking PI DataLink add-in...")
    time.sleep(3)

    # Check if PI DataLink is loaded
    try:
        # Try to access PI DataLink through COM
        for addin in app.api.COMAddIns:
            print(f"   Found add-in: {addin.Description}")
            if "PI" in addin.Description:
                print(f"   ✓ PI DataLink found! Connected: {addin.Connect}")
                if not addin.Connect:
                    print("   ! PI DataLink is NOT connected - enabling now...")
                    addin.Connect = True
                    time.sleep(2)
    except Exception as e:
        print(f"   Warning: Could not enumerate add-ins: {e}")

    print("\n4. Testing formula in DL_WORK sheet...")
    ws = wb.sheets["DL_WORK"]

    # Check cell A2 value
    val = ws.range("A2").value
    print(f"   Cell A2 value: {val}")
    print(f"   Cell A2 type: {type(val)}")

    if val and "#NAME" in str(val):
        print("\n   ❌ ERROR: #NAME? error detected - PI DataLink NOT working")
        print("   This means PISampDat function is not recognized")
    elif val and isinstance(val, (int, float)):
        print("\n   ✓ SUCCESS: PI DataLink is working! Got numeric value")
    else:
        print(f"\n   ? UNKNOWN: Got value '{val}' - may need calculation")
        print("   Triggering calculation...")
        app.api.CalculateFull()
        time.sleep(5)
        val2 = ws.range("A2").value
        print(f"   Cell A2 after calc: {val2}")

    print("\n5. Closing Excel...")
    wb.close(save_as_original=False)
    app.quit()

    print("\n" + "=" * 70)
    print("Test complete!")

except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()
    print("\nTrying to close Excel...")
    try:
        app.quit()
    except:
        pass
