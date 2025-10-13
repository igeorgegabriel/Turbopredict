import os, sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from pi_monitor.webapi import fetch_tags_via_webapi

os.environ['PI_WEBAPI_URL'] = os.getenv('PI_WEBAPI_URL','http://PTSG-1MMPDPdb01/piwebapi')
server = os.getenv('PI_SERVER_NAME','PTSG-1MMPDPdb01')
print('Testing PI Web API:', os.environ['PI_WEBAPI_URL'], 'server=', server)
try:
    df = fetch_tags_via_webapi(tags=['PCFS.K-12-01.12SI-401B.PV'], server=server, start='-2h', end='*', step='-0.1h', timeout=15.0, max_workers=1, qps=1.0, retries=0)
    print('RESULT_ROWS=', len(df))
    print('UNIQUE_TAGS=', (df['tag'].nunique() if not df.empty and 'tag' in df.columns else 0))
    print(df.head(5).to_string(index=False))
except Exception as e:
    import traceback as tb
    print('ERROR:', type(e).__name__, str(e))
    tb.print_exc()
