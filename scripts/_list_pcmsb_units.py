from __future__ import annotations

import importlib.util
from pathlib import Path


def main() -> None:
    p = Path(__file__).resolve().parents[0] / "build_pcmsb.py"
    spec = importlib.util.spec_from_file_location("_build_pcmsb", str(p))
    mod = importlib.util.module_from_spec(spec)  # type: ignore
    assert spec and spec.loader
    spec.loader.exec_module(mod)  # type: ignore
    units = list(getattr(mod, "PCMSB_UNITS", {}).keys())
    print("\n".join(units))


if __name__ == "__main__":
    main()

