import pathlib
import sys

# delete_data.py는 app/ 패키지가 아닌 평면 스크립트라(tools/data-admin/delete_data.py),
# tests/ 아래에서 `import delete_data`가 항상 되도록 이 디렉터리를 sys.path에 명시적으로
# 얹는다 (pytest rootdir 추론에 기대지 않음 — 다른 모듈 conftest.py의 빈 파일과 달리 여기는
# app/ 패키지가 없어 자동 삽입이 보장되지 않는다).
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
