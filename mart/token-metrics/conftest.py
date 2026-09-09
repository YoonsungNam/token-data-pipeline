import pathlib
import sys

# tests/는 패키지(tests/__init__.py)이지만, `python -m pytest`를 다른 cwd에서 호출하거나
# rootdir 추론이 바뀌어도 `import app`이 항상 되도록 모듈 루트(mart/token-metrics)를
# sys.path에 명시적으로 얹는다 (tools/verify/conftest.py와 동일 관례).
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
