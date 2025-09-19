from pathlib import Path
import xlwings as xw
from pi_monitor.batch import _fetch_single

xlsx = Path(r"c:\\Users\\george.gabrielujai\\Documents\\CodeX\\excel\\ABF_Automation.xlsx")
app = xw.App(visible=False, add_book=False)
try:
    wb = app.books.open(str(xlsx))
    df = _fetch_single(wb, 'DL_TEST', 'PRISM.ABF.07-MT001.FI-07001.PV', r"\\PTSG-1MMPDPdb01", '-1d', '*', '-0.1h', settle_seconds=1.0)
    print('rows prism', len(df))
    df2 = _fetch_single(wb, 'DL_TEST', 'ABF.07-MT001.FI-07001.PV', r"\\PTSG-1MMPDPdb01", '-1d', '*', '-0.1h', settle_seconds=1.0)
    print('rows no prefix', len(df2))
finally:
    app.quit()
