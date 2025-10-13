# Optimized PI Data Fetch Strategy

## Problem Statement
Original design would take **40+ hours** to refresh 13 units:
- 13 units × 50 tags × 4 minutes = 2,600 minutes (43 hours)
- Completely impractical for hourly/daily refreshes

## Solution: Fail-Fast Strategy

### Key Changes

#### 1. Reduced Timeouts (Balanced)
```
PI_FETCH_TIMEOUT: 240s → 60s  (75% reduction)
PI_FETCH_LINGER:   90s → 20s  (78% reduction)
Total per tag:    330s → 80s  (76% reduction)
```

#### 2. Early Error Detection
- Checks for Excel errors (#N/A, #REF!, etc.) after 5 seconds
- Immediately skips tags that don't exist or have server issues
- Fails in 5-10s instead of waiting full 60s for bad tags

#### 3. Progress Tracking
Real-time visibility:
```
[23/56] Fetching: PCFS.K-12-01.12SI-401B.PV
[progress] Success: 22/56 | No data: 1/56 | Remaining: 33
```

### Performance Improvement

**Old Design:**
- Good tags: 270s × 45 = 12,150s (3.4 hours)
- Bad tags:  270s × 11 = 2,970s (0.8 hours)
- **Total per unit: ~4.2 hours**
- **Total all units: ~55 hours**

**New Design:**
- Good tags: 80s × 45 = 3,600s (1.0 hour)
- Bad tags:  10s × 11 = 110s (0.02 hours)  ← **Fail fast!**
- **Total per unit: ~1.0 hour**
- **Total all units: ~13 hours**

**Speed improvement: 4.2× faster** (55h → 13h)

### Estimated Time per Unit (56 tags)

| Scenario | Old Time | New Time | Improvement |
|----------|----------|----------|-------------|
| All tags good | 3.4h | 1.0h | 3.4× faster |
| 80% good, 20% bad | 4.2h | 1.0h | 4.2× faster |
| 50% good, 50% bad | 5.6h | 0.7h | 8.0× faster |

### Why This Works

1. **Tags with data complete quickly** (usually <30s)
2. **Excel errors detected immediately** (5-10s) - no need to wait full timeout
3. **True timeouts only for network issues** (rare)

### Trade-offs

✅ **Pros:**
- 4-8× faster overall
- Fails fast on bad tags
- Still reliable for good tags
- Real-time progress visibility

⚠️ **Cons:**
- Tags that take 60-240s will timeout (but these are rare)
- If network is very slow, might miss some data

### Recommendation

Run the new optimized version. Monitor the summary:
```
======================================================================
FETCH SUMMARY FOR K-12-01
======================================================================
Total tags:       56
Success:          45 (80%)
No data:          11 (19%)
Total rows fetched: 125,430
======================================================================
```

If **Success rate is <70%**, tags that should have data are timing out. Consider:
1. Investigating network connectivity to PI server
2. Checking if those specific tags are actually active
3. Only then increasing timeouts for specific problem tags

### Configuration

All settings in `scripts/incremental_refresh_safe.py`:
```python
PI_FETCH_TIMEOUT = '60'          # Per-tag timeout
PI_FETCH_LINGER = '20'           # Post-timeout linger
PI_EARLY_ERROR_DETECT = '1'      # Enable fail-fast
```

Adjust based on your network:
- **Fast network**: Reduce to 45s + 15s
- **Slow network**: Increase to 90s + 30s
- **Very slow**: 120s + 45s (still better than 270s!)
