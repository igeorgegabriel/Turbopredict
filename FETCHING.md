# FETCHING STATUS – October 2025

Summary of the investigation that stabilized Option [1] and Excel/PI DataLink fetching across PCFS, ABF, and PCMSB units. Use this document as a reference point if future bugs appear.

## Key Fixes Implemented

1. **Per-unit/per-plant PI server overrides**
   - `scripts/incremental_refresh_safe.py` now prefers `PLANT_UNIT_PI_SERVER` > `PLANT_PI_SERVER` > default.
   - Environment variables used in production:
     ```cmd
     set PCFS_PI_SERVER=\PTSG-1MMPDPdb01
     set ABF_21_K002_PI_SERVER=\VSARMNGPIMDB01
     set ABF_07_MT01_K001_PI_SERVER=\PTSG-1MMPDPdb01
     set PCMSB_PI_SERVER=\PTSG-1MMPDPdb01
     ```

2. **Excel PISampDat formula placement fixed**
   - Both `simple_incremental_refresh.py` and `incremental_refresh_safe.py` now use array formulas (and dynamic-spill fallback) to fill columns A/B, avoiding the implicit `@PISampDat` single-cell result.

3. **Web API disabled when unreachable**
   - `set PI_WEBAPI_URL=` ensures the scripts don’t waste time attempting PI Web API calls.

4. **Log cleanup**
   - Removed Pandas `SettingWithCopyWarning` messages.

5. **Runtime metrics added**
   - Option [1] summary now prints per-unit runtime and plant totals (e.g., `PCFS/K-12-01 (6m 15s)`).

## Known Good Settings Before Running Option [1]

```cmd
set PI_WEBAPI_URL=
set EXCEL_VISIBLE=1
set PI_FETCH_TIMEOUT=45
set PI_FETCH_LINGER=10
set PCFS_PI_SERVER=\PTSG-1MMPDPdb01
set ABF_21_K002_PI_SERVER=\VSARMNGPIMDB01
set ABF_07_MT01_K001_PI_SERVER=\PTSG-1MMPDPdb01
set PCMSB_PI_SERVER=\PTSG-1MMPDPdb01
```

## Validation Results (11 Oct 2025)

| Unit            | Tags (Active/Total) | Rows Added |
|-----------------|----------------------|------------|
| PCFS/K-12-01    | 56/56                | 6,387      |
| PCFS/K-16-01    | 124/124              | 612,641    |
| PCFS/K-19-01    | 149/149              | 754,929    |
| PCFS/K-31-01    | 156/156              | 792,227    |
| ABF/07-MT01-K001| 125/125              | 889,915    |
| ABF/21-K002     | 125/125              | 889,915    |
| PCMSB/C-02001   | 80/80                | 272,057    |
| PCMSB/C-104     | 60/60                | 394,505    |
| PCMSB/C-13001   | 81/81                | 499,196    |
| PCMSB/C-1301    | 59/60                | 410,457    |
| PCMSB/C-1302    | 82/82                | 505,337    |
| PCMSB/C-201     | 77/77                | 532,871    |
| PCMSB/C-202     | 54/54                | 328,404    |
| PCMSB/XT-07002  | 116/116              | 713,726    |

All 13 units refreshed successfully in the final run (`[OK] Refreshed: 13/13 units successfully`).

## Lessons Learned / SOP

1. Run the one-tag Excel check with explicit `--server` before blaming PI or Excel.
2. Clear `PI_WEBAPI_URL` if your Web API endpoint isn’t responsive.
3. Keep Excel visible and kill zombie EXCEL.EXE before launching Option [1].
4. If one plant is slow, use `SKIP_UNITS` to let others proceed, then rerun the skipped units.
5. Use array-formula injection for PISampDat whenever dynamic arrays are disabled.

## Suggested Future Enhancements

1. Add CSV/JSON logging for per-unit runtimes.
2. Implement plant interleaving or per-unit time budgets if unit count grows >50.
3. Build a PI Web API path once a reliable endpoint is available.

