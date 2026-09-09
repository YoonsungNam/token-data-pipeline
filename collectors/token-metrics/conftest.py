"""pytest 루트 conftest — collectors/token-metrics/ 를 import 루트로 고정 (tests/__init__.py 와 한 쌍)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
