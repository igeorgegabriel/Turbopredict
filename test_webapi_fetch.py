"""Test PI Web API fetching - this should be INSTANT vs 1 hour Excel fetch"""
import os
os.environ['PI_WEBAPI_URL'] = 'http://PTSG-1MMPDPdb01/piwebapi'

from pi_monitor.webapi import fetch_tags_via_webapi
import pandas as pd

# Test with a few K-12-01 tags
test_tags = [
    "PCFS.K-12-01.12PI-007.PV",  # You confirmed this has data
    "PCFS.K-12-01.12FI-004A.PV",
    "PCFS.K-12-01.12FI-004B.PV",
]

print("=" * 70)
print("TESTING PI WEB API FETCH (Should be INSTANT)")
print("=" * 70)
print(f"URL: {os.environ['PI_WEBAPI_URL']}")
print(f"Tags: {len(test_tags)}")
print(f"Time range: -53h to *")
print("=" * 70 + "\n")

try:
    df = fetch_tags_via_webapi(
        tags=test_tags,
        server="PTSG-1MMPDPdb01",
        start="-53h",
        end="*",
        step="-0.1h",
    )

    print(f"\nResults:")
    print(f"  Total rows: {len(df):,}")
    print(f"  Columns: {df.columns.tolist()}")

    if not df.empty:
        print(f"\nSuccess! PI Web API is working!")
        print(f"\nSample data:")
        print(df.head(10))
        print(f"\nDate range: {df['time'].min()} to {df['time'].max()}")

        # Show per-tag breakdown
        print(f"\nPer-tag breakdown:")
        for tag in df['tag'].unique():
            tag_df = df[df['tag'] == tag]
            print(f"  {tag}: {len(tag_df):,} rows")
    else:
        print("\nFAILED: PI Web API returned empty data")
        print("This is why system falls back to slow Excel method")

except Exception as e:
    print(f"\nERROR: {e}")
    print("\nPI Web API is not working - will use Excel fallback (SLOW)")

print("\n" + "=" * 70)
