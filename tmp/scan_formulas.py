import openpyxl
wb = openpyxl.load_workbook(r'excel/ABFSB/ABF LIMIT REVIEW (CURRENT).xlsx', data_only=False, read_only=True)
found=[]
for ws in wb.worksheets:
    for row in ws.iter_rows():
        for c in row:
            v=c.value
            if isinstance(v,str) and v.upper().startswith('PI'):
                found.append((ws.title,c.coordinate,v[:120]))
                if len(found)>=40:
                    break
        if len(found)>=40:
            break
    if len(found)>=40:
        break
print('found', len(found))
for t in found:
    print(t[0], t[1], t[2])
wb.close()
