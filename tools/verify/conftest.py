import pathlib
import sys

# run_invariants.py는 app/ 패키지가 아닌 평면 스크립트라(tools/verify/run_invariants.py),
# tests/ 아래에서 `import run_invariants`가 항상 되도록 이 디렉터리를 sys.path에 명시적으로
# 얹는다 (pytest rootdir 추론에 기대지 않음 — tools/data-admin/conftest.py와 동일 관례).
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
