"""install.sh / k8s 매니페스트 계약 테스트 (Plan 6c T8, fix1).

install.sh는 bash라 단위 실행이 불가 — 텍스트 파싱으로 (1) 읽기 계약 배열이 app/preflight.py와
동일한지, (2) 프리플라이트가 DDL 적용 전에 오는지(설계 §6.1 "불일치 시 설치 중단"), (3) Secret 키
목록(설계 §6.1 — 11개, EXPECTED_LATE_SERVICES 없음), (4) CronJob 계약 수치(설계 §6.1), (5) 프리플라이트
DESCRIBE가 default 계정이 아니라 앱 계정(ch_query, here-string 비밀번호)으로 실행되는지를 고정한다.
"""
from __future__ import annotations

import pathlib
import re
import shutil
import subprocess

import pytest
import yaml

from app.ch import DB_TOKEN_DIM, DB_TOKEN_MART
from app.preflight import READ_CONTRACT

ROOT = pathlib.Path(__file__).resolve().parents[1]
INSTALL = ROOT / "install.sh"
CRONJOB_YAML = ROOT / "k8s" / "base" / "cronjob.yaml"
STAGE_KUST = ROOT / "k8s" / "overlays" / "stage" / "kustomization.yaml"
VERIFY_KUST = ROOT / "k8s" / "overlays" / "company-verify" / "kustomization.yaml"

SECRET_KEYS = ("CH_HOST", "CH_PORT", "CH_USER", "CH_PASSWORD", "CH_CLUSTER",
               "CH_DB_FACT", "CH_DB_DIM", "CH_DB_MART", "CH_DB_TOKEN_MART", "CH_DB_TOKEN_DIM",
               "MART_METRICS_MAX_MUTATIONS_PER_RUN")


def _install_text() -> str:
    return INSTALL.read_text(encoding="utf-8")


def _install_contract() -> dict[str, list[str]]:
    """install.sh의 READ_CONTRACT=( "db.table_dist:col" … ) → {"db.table": [col, …]} (선언 순서 유지).

    ${CH_DB_TOKEN_MART}/${CH_DB_TOKEN_DIM}은 app.ch.DB_TOKEN_MART/DB_TOKEN_DIM(런타임 env 반영,
    fix1 D9)로 치환한다 — 하드코딩된 "mart"/"gpu_data" 리터럴을 쓰면 CH_DB_TOKEN_MART=... 로 프로세스를
    띄운 테스트 실행에서 install.sh 배열과 app.preflight.READ_CONTRACT가 어긋나도 감지하지 못한다.
    """
    m = re.search(r"^READ_CONTRACT=\((.*?)^\)", _install_text(), re.S | re.M)
    assert m, "install.sh에 READ_CONTRACT=( … ) 배열이 없다"
    entries = re.findall(r'"([^"]+)"', m.group(1))
    out: dict[str, list[str]] = {}
    for entry in entries:
        entry = entry.replace("${CH_DB_TOKEN_MART}", DB_TOKEN_MART).replace("${CH_DB_TOKEN_DIM}", DB_TOKEN_DIM)
        table, col = entry.split(":")
        assert table.endswith("_dist"), f"프리플라이트는 _dist 테이블을 DESCRIBE 한다: {entry}"
        out.setdefault(table[: -len("_dist")], []).append(col)
    return out


def test_install_read_contract_equals_preflight():
    got = _install_contract()
    assert got == {k: list(v) for k, v in READ_CONTRACT.items()}
    assert sum(len(v) for v in got.values()) == 13
    assert len(got) == 3


def test_install_steps_six_and_preflight_before_ddl():
    text = _install_text()
    idx = [text.index(f'"[{k}/6]') for k in range(1, 7)]
    assert idx == sorted(idx), "단계 [1/6]..[6/6]가 순서대로 나타나야 한다"
    assert text.index('"[3/6]') < text.index('"[4/6]')                  # 프리플라이트 → DDL
    assert "PREFLIGHT FAIL read_contract missing=" in text
    assert re.search(r"^\s*exit 3\s*$", text, re.M), "프리플라이트 실패는 exit 3"
    assert "DESCRIBE TABLE" in text
    assert "mart_metrics_tables.sql" in text
    assert re.search(r"\bset env\b", text) is None                      # 정적 env 주입 없음 (CH_HOST도 Secret 키)


def test_install_preflight_uses_app_account_via_ch_query():
    # fix1 MUST-1: DESCRIBE는 pod 기본 계정이 아니라 앱 계정(ch_user/ch_pass)으로 실행돼야
    # GRANT 누락도 함께 잡는다 (collectors/token-metrics/install.sh ch_query()와 동일 관례).
    text = _install_text()
    assert re.search(r"^ch_query\(\)\s*\{", text, re.M), "ch_query() 함수 정의가 없다"
    assert 'cols="$(ch_query "DESCRIBE TABLE ${table}"' in text, \
        "[3/6] DESCRIBE는 ch_query()를 통해 앱 계정으로 실행돼야 한다"
    assert '--password "$(cat)"' in text, "비밀번호는 here-string으로 stdin에 실어야 한다"
    # 비밀번호가 --password argv로 직접 노출되는 라인이 없어야 한다 (kubectl exec argv는 감사 로그에 남는다)
    assert re.search(r'--password\s+"?\$\{?ch_pass\b', text) is None, \
        "ch_pass가 --password argv에 직접 노출돼서는 안 된다"


def test_install_secret_keys_eleven_and_no_expected_late():
    text = _install_text()
    found = set(re.findall(r'--from-literal="([A-Z_]+)=', text))
    assert set(SECRET_KEYS) <= found, sorted(set(SECRET_KEYS) - found)
    assert "INSERT_QUORUM" in found                                       # company/company-verify 조건부
    assert "EXPECTED_LATE_SERVICES" not in found
    assert "EXPECTED_LATE_SERVICES" not in text
    assert "target-db" not in text


def test_install_pull_secret_created_only_when_absent():
    text = _install_text()
    start = text.index('"[1/6]')
    end = text.index('"[2/6]')
    block = text[start:end]
    assert "create secret docker-registry" in block
    assert "갱신" not in block, "registry-pull-secret은 없을 때만 생성 — 갱신 프롬프트 금지 (설계 §7.5)"


def test_install_usage_range_ends_before_set_euo():
    lines = _install_text().splitlines()
    m = re.search(r"sed -n '2,(\d+)p'", lines[[i for i, l in enumerate(lines) if l.startswith("usage()")][0]])
    assert m, "usage()는 sed -n '2,Np' 형식"
    last = int(m.group(1))
    assert all(lines[i].startswith("#") for i in range(1, last))        # 2..N 행은 전부 주석
    assert lines[last].startswith("set -euo pipefail")                    # N+1 행(0-based last)이 set -euo


def test_install_company_verify_suffixes_secret_and_cronjob_name():
    # SHOULD-3(a)(1): company-verify는 SECRET_NAME/CRONJOB_NAME 둘 다 -verify 접미
    text = _install_text()
    assert 'SECRET_NAME="${SECRET_NAME}-verify"' in text
    assert 'CRONJOB_NAME="${CRONJOB_NAME}-verify"' in text


def test_install_missing_entries_strip_dist_suffix():
    # SHOULD-3(a)(2): PREFLIGHT FAIL 목록은 _dist 없는 "db.table.col" 형식으로 보고한다 (C3)
    text = _install_text()
    assert 'missing+=("${table%_dist}.${col}")' in text
    assert 'missing+=("${table%_dist}.*")' in text


def test_install_secret_prompt_eof_safe():
    # SHOULD-3(a)(3): [2/6] Secret 존재 확인 프롬프트는 EOF/non-TTY에서도 set -e로 죽지 않는다
    text = _install_text()
    assert 'read -r -p "  이미 존재합니다. 갱신하시겠습니까? [y/N] " ans || ans=""' in text


def test_install_ddl_dir_mapping_lines():
    # SHOULD-3(a)(4): DDL_DIR 매핑 라인을 그대로 고정한다 (install.sh에 실제로 쓰인 문구)
    text = _install_text()
    assert 'DDL_DIR="ddl/company"' in text
    assert '[[ "${ENV}" == "company-verify" ]] && DDL_DIR="ddl/company-verify"' in text
    assert '[[ "${ENV}" == "stage" ]] && DDL_DIR="ddl/stage"' in text


def test_cronjob_yaml_contract():
    text = CRONJOB_YAML.read_text(encoding="utf-8")
    for needle in ('name: token-mart-metrics', 'schedule: "20 10 * * *"', "timeZone: Asia/Seoul",
                   "concurrencyPolicy: Forbid", "startingDeadlineSeconds: 1800",
                   "activeDeadlineSeconds: 1800", "backoffLimit: 1", "restartPolicy: Never",
                   "successfulJobsHistoryLimit: 3", "failedJobsHistoryLimit: 3",
                   "name: registry-pull-secret", "name: token-mart-metrics-ch-secret",
                   "image: token-mart-metrics:latest", "memory: 256Mi", "memory: 1Gi", "cpu: 100m"):
        assert needle in text, needle
    assert "EXPECTED_LATE_SERVICES" not in text
    assert "token-mart-daily" not in text                                 # 원형 이름 잔재 금지
    assert "ghcr.io/yoonsungnam/token-mart-metrics" in STAGE_KUST.read_text(encoding="utf-8")
    verify = VERIFY_KUST.read_text(encoding="utf-8")
    assert "nameSuffix: -verify" in verify
    assert "value: token-mart-metrics-ch-secret-verify" in verify


def test_cronjob_yaml_labels_app_on_cronjob_jobtemplate_and_pod():
    # SHOULD-3(a)(5): app: token-mart-metrics 라벨이 CronJob/jobTemplate/pod 템플릿 3곳에 있어야 한다 (D4)
    doc = yaml.safe_load(CRONJOB_YAML.read_text(encoding="utf-8"))
    assert doc["metadata"]["labels"]["app"] == "token-mart-metrics"
    job_spec = doc["spec"]["jobTemplate"]
    assert job_spec["metadata"]["labels"]["app"] == "token-mart-metrics"
    pod_template = job_spec["spec"]["template"]
    assert pod_template["metadata"]["labels"]["app"] == "token-mart-metrics"
    containers = pod_template["spec"]["containers"]
    assert containers[0]["name"] == "token-mart-metrics"


@pytest.mark.skipif(shutil.which("kubectl") is None, reason="kubectl 없음 — CI manifests 잡이 대신 검증")
def test_kustomize_overlays_render_contract():
    for overlay, needles in (
        ("stage", ("ghcr.io/yoonsungnam/token-mart-metrics:latest", "name: token-mart-metrics-ch-secret\n")),
        ("company", ("image: token-mart-metrics:latest", "name: token-mart-metrics-ch-secret\n")),
        ("company-verify", ("name: token-mart-metrics-verify\n", "name: token-mart-metrics-ch-secret-verify\n")),
    ):
        out = subprocess.run(["kubectl", "kustomize", str(ROOT / "k8s" / "overlays" / overlay)],
                             check=True, capture_output=True, text=True).stdout
        assert "schedule: 20 10 * * *" in out, overlay
        assert "startingDeadlineSeconds: 1800" in out and "activeDeadlineSeconds: 1800" in out, overlay
        for needle in needles:
            assert needle in out, (overlay, needle)
