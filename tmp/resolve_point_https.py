import os, sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from pi_monitor.webapi import PIWebAPIClient, fetch_tags_via_webapi

base = 'https://PTSG-1MMPDPdb01/piwebapi'
server = 'PTSG-1MMPDPdb01'
print('Resolving via', base, 'server=', server, 'tag=', 'PCFS.K-12-01.12SI-401B.PV')
client = PIWebAPIClient(base_url=base, auth_mode='windows', timeout=15.0, verify_ssl=False)
try:
    webid = client.resolve_point_webid(server, 'PCFS.K-12-01.12SI-401B.PV')
    print('WebId:', webid)
    if webid:
        import pandas as pd
        df = client.fetch_interpolated(webid, start='*-2h', end='*', interval='6m')
        print('Interpolated rows:', len(df))
    else:
        print('Point did not resolve')
except Exception as e:
    import traceback as tb
    print('ERROR:', type(e).__name__, str(e))
    tb.print_exc()
