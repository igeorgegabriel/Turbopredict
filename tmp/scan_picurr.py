import openpyxl
wb = openpyxl.load_workbook(r'excel/ABFSB/ABF LIMIT REVIEW (CURRENT).xlsx', data_only=False, read_only=True)
count=0
for ws in wb.worksheets:
    for row in ws.iter_rows():
        for c in row:
            v=c.value
            if isinstance(v,str) and 'PICurrVal' in v:
                print(ws.title, c.coordinate, v)
                count+=1
                if count>10:
                    break
        if count>10:
            break
    if count>10:
        break
print('total matches',count)
wb.close()
