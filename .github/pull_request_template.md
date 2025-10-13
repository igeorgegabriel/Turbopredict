Title: <brief summary of the change>

Summary
- What problem does this PR solve? Link any tickets.
- What’s the approach and why is it safe?

PI/Excel Specifics (when applicable)
- Confirm PI DataLink formulas use dynamic spill or bounded ranges.
- Confirm robust wait (no fixed sleeps only): uses CalculationState and async completion.
- Confirm no concurrent refresh across units (coordination lock respected).

Verification
- Manual/Automated checks performed (attach logs or screenshots if useful).
- Data sanity (row counts, date ranges) and Parquet readback verified.

Rollout & Risk
- Backward compatibility and failure modes.
- Any config or ENV changes needed (e.g., PI_FETCH_TIMEOUT, EXCEL_CALC_MODE).

Checklist
- [ ] CodeRabbit review is green (no critical or high issues outstanding)
- [ ] CI/build green
- [ ] No secrets/credentials committed
- [ ] Updated docs where appropriate

