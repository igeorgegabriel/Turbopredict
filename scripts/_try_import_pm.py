import sys, os
print('CWD:', os.getcwd())
try:
    import pi_monitor
    print('OK: imported pi_monitor. Package path:', pi_monitor.__file__)
except Exception as e:
    print('FAIL:', e)

