from pathlib import Path
from pi_monitor.batch import build_unit_from_tags
from pi_monitor.clean import dedup_parquet

root=Path('.').resolve()
xlsx=root/ 'excel/PCFS/PCFS_Automation.xlsx'
tags_file=root/ 'config/tags_k12_01.txt'
outs=root/'tmp/k12'
outs.mkdir(parents=True, exist_ok=True)

tags=[s.strip() for s in tags_file.read_text(encoding='utf-8').splitlines() if s.strip() and not s.strip().startswith('#')]
probe=tags[:5]
print('probing tags:', probe)
parq=outs/'K-12-01_probe.parquet'
out=build_unit_from_tags(xlsx, probe, parq, plant='PCFS', unit='K-12-01', start='-2h', end='*', step='-1m', work_sheet='DL_WORK', settle_seconds=2.0, visible=True, use_working_copy=True)
print('wrote', out)
