#!/usr/bin/env python3
"""Check PI DataLink installation and enable it"""
import xlwings as xw
import time
import sys

print("Checking PI DataLink status...")
print("=" * 70)

app = None
try:
    # Open Excel
    print("\n1. Starting Excel...")
    app = xw.App(visible=True, add_book=False)
    time.sleep(2)

    print("\n2. Checking COM Add-ins...")
    addins = app.api.COMAddIns
    pi_datalink_found = False
    pi_datalink_connected = False

    for i in range(1, addins.Count + 1):
        addin = addins.Item(i)
        desc = addin.Description
        print(f"   - {desc} (Connected: {addin.Connect})")

        if "PI DataLink" in desc or "PI" in desc:
            pi_datalink_found = True
            pi_datalink_connected = addin.Connect

            if not addin.Connect:
                print(f"\n   [!] PI DataLink is DISABLED - Enabling now...")
                try:
                    addin.Connect = True
                    time.sleep(2)
                    print(f"   [OK] PI DataLink enabled successfully")
                except Exception as e:
                    print(f"   [ERROR] Failed to enable: {e}")

    print("\n" + "=" * 70)
    if not pi_datalink_found:
        print("ERROR: PI DataLink add-in NOT FOUND!")
        print("\nPI DataLink may not be installed or registered.")
        print("You may need to:")
        print("  1. Reinstall PI DataLink")
        print("  2. Check if it's installed at: C:\\Program Files\\PIPC\\Excel")
        print("  3. Register the add-in manually in Excel")
    elif pi_datalink_connected:
        print("SUCCESS: PI DataLink is enabled and connected!")
    else:
        print("WARNING: PI DataLink found but could not enable it")

    print("\n3. Keeping Excel open for 5 seconds...")
    print("   Check if 'PI DataLink' appears in Excel ribbon menu")
    time.sleep(5)

    input("\nPress Enter to close Excel...")

except Exception as e:
    print(f"\nERROR: {e}")
    import traceback
    traceback.print_exc()
finally:
    if app:
        try:
            print("\nClosing Excel...")
            app.quit()
        except:
            pass

sys.exit(0)
