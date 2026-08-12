# conftest.py — pytest configuration for lt_app tests.
from __future__ import annotations

import sys
from pathlib import Path

_sys_path_root = str(Path(__file__).resolve().parents[2])
if _sys_path_root not in sys.path:
    sys.path.insert(0, _sys_path_root)