import openpyxl
wb = openpyxl.load_workbook(r"excel/ABFSB/ABF LIMIT REVIEW (CURRENT).xlsx", data_only=False, read_only=True)
ws = wb.active
found = 0
for row in ws.iter_rows(min_col=5, max_col=5, min_row=2, max_row=120):
    cell = row[0]
    v = cell.value
    if isinstance(v, str) and v.strip().startswith('='):
        print(cell.coordinate, v[:120])
        found += 1
        if found >= 10:
            break
wb.close()
