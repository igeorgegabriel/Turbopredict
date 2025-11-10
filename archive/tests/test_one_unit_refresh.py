"""Test simple refresh for just one unit"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "scripts"))

from simple_incremental_refresh import simple_refresh_unit

# Test K-16-01 which showed the error
success = simple_refresh_unit("K-16-01", "PCFS")
print(f"\nResult: {'SUCCESS' if success else 'FAILED'}")
