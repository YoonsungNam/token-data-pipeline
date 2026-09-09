"""배포 계층 계약 잠금 (설계 §5.2 CronJob 수치·§5.6 이름·§7.5 zero-diff).

kubectl 없이 YAML 텍스트를 직접 파싱해 검사한다(CI unit job). kubectl이 PATH에 있으면
kustomize 렌더 결과도 추가로 검사한다(없으면 그 테스트만 skip — CI manifests job이 동일 grep을 수행).
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

HERE = Path(__file__).resolve().parent.parent
BASE_CRONJOB = HERE / "k8s" / "base" / "cronjob.yaml"
BASE_KUSTOMIZATION = HERE / "k8s" / "base" / "kustomization.yaml"
OVERLAYS = HERE / "k8s" / "overlays"
APP = "token-metrics-collector"

EXPECTED_ENV = {
    "ENDPOINTS_FILE": "/etc/token-metrics/endpoints.yaml",
    "SOFT_DEADLINE_MINUTES": "40",
    "LOAD_BUDGET_S": "1200",
    "FINAL_HOUR_KST": "9",
    "MAX_RESPONSE_BYTES": "5000000",
    "METRICS_MAX_MUTATIONS_PER_RUN": "45",
}


def _load(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _cronjob() -> dict:
    return _load(BASE_CRONJOB)


def _pod_spec(cj: dict) -> dict:
    return cj["spec"]["jobTemplate"]["spec"]["template"]["spec"]


def _container(cj: dict) -> dict:
    containers = _pod_spec(cj)["containers"]
    assert len(containers) == 1
    return containers[0]


def _env(cj: dict) -> dict:
    return {e["name"]: e["value"] for e in _container(cj)["env"]}


def _resolve_pointer(obj, pointer: str):
    """JSON pointer(/a/0/b)를 dict/list에 적용 — overlay 패치 경로가 base 구조와 맞는지 검사용."""
    for part in pointer.strip("/").split("/"):
        obj = obj[int(part)] if isinstance(obj, list) else obj[part]
    return obj


def test_cronjob_spec_values():
    cj = _cronjob()
    assert cj["apiVersion"] == "batch/v1" and cj["kind"] == "CronJob"
    assert cj["metadata"]["name"] == APP
    assert cj["metadata"]["labels"] == {"app": APP}
    spec = cj["spec"]
    assert spec["schedule"] == "5 2-9 * * *"
    assert spec["timeZone"] == "Asia/Seoul"
    assert spec["concurrencyPolicy"] == "Forbid"
    assert spec["startingDeadlineSeconds"] == 540
    assert spec["successfulJobsHistoryLimit"] == 3
    assert spec["failedJobsHistoryLimit"] == 3
    assert spec["jobTemplate"]["metadata"]["labels"] == {"app": APP}
    job = spec["jobTemplate"]["spec"]
    assert job["backoffLimit"] == 0
    assert job["activeDeadlineSeconds"] == 3000
    assert job["template"]["metadata"]["labels"] == {"app": APP}
    assert _pod_spec(cj)["restartPolicy"] == "Never"


def test_slot_arithmetic_locked():
    # §5.2 슬롯 산식: 지연 시작 ≤540 + activeDeadlineSeconds 3000 + grace 30 = 3570 < 3600
    cj = _cronjob()
    starting = cj["spec"]["startingDeadlineSeconds"]
    active = cj["spec"]["jobTemplate"]["spec"]["activeDeadlineSeconds"]
    assert starting + active + 30 < 3600
    # activeDeadlineSeconds = SOFT_DEADLINE_MINUTES×60 + 종료 마진 600; SOFT×60 > LOAD_BUDGET_S (test_config.py와 동일 불변식)
    env = _env(cj)
    assert int(env["SOFT_DEADLINE_MINUTES"]) * 60 + 600 == active
    assert int(env["SOFT_DEADLINE_MINUTES"]) * 60 > int(env["LOAD_BUDGET_S"])


def test_container_name_image_and_env():
    cj = _cronjob()
    c = _container(cj)
    assert c["name"] == APP
    assert c["image"].split(":")[0] == APP  # kustomize images 치환 대상 이름
    assert c["imagePullPolicy"] == "Always"
    assert c["envFrom"] == [{"secretRef": {"name": "token-metrics-ch-secret"}}]
    assert [e["name"] for e in c["env"]] == list(EXPECTED_ENV)  # 순서 고정
    assert _env(cj) == EXPECTED_ENV
    assert all(isinstance(e["value"], str) for e in c["env"])  # k8s env value는 문자열
    assert "CH_HOST" not in _env(cj)  # install.sh [7/7] set env가 주입
    assert "VM_PUSH_URL" not in _env(cj)  # VM push 없음 (§5.2)


def test_volumes_order_and_names():
    cj = _cronjob()
    ps = _pod_spec(cj)
    assert ps["volumes"] == [
        {"name": "endpoints", "configMap": {"name": "token-metrics-endpoints"}},
        {"name": "ca-bundle", "configMap": {"name": "token-metrics-ca-bundle", "optional": True}},
    ]
    assert _container(cj)["volumeMounts"] == [
        {"name": "endpoints", "mountPath": "/etc/token-metrics", "readOnly": True},
        {"name": "ca-bundle", "mountPath": "/etc/token-metrics-ca", "readOnly": True},
    ]
    assert ps["imagePullSecrets"] == [{"name": "registry-pull-secret"}]
    assert _container(cj)["resources"] == {
        "requests": {"cpu": "100m", "memory": "256Mi"},
        "limits": {"cpu": "1", "memory": "1Gi"},
    }


def test_base_kustomization():
    assert _load(BASE_KUSTOMIZATION)["resources"] == ["cronjob.yaml"]


def test_stage_overlay_image():
    k = _load(OVERLAYS / "stage" / "kustomization.yaml")
    assert k["resources"] == ["../../base"]
    assert "namespace" not in k
    assert k["images"] == [{
        "name": APP,
        "newName": "ghcr.io/yoonsungnam/token-metrics-collector",
        "newTag": "latest",
    }]


def test_company_overlay_is_resources_only():
    k = _load(OVERLAYS / "company" / "kustomization.yaml")
    assert k["resources"] == ["../../base"]
    assert "namespace" not in k and "images" not in k and "nameSuffix" not in k and "patches" not in k


def test_company_verify_overlay_names():
    k = _load(OVERLAYS / "company-verify" / "kustomization.yaml")
    assert k["nameSuffix"] == "-verify"
    assert k["resources"] == ["../../base"]
    assert "namespace" not in k and "images" not in k
    assert len(k["patches"]) == 1
    assert k["patches"][0]["target"] == {"kind": "CronJob", "name": APP}
    ops = yaml.safe_load(k["patches"][0]["patch"])
    assert ops == [
        {"op": "replace",
         "path": "/spec/jobTemplate/spec/template/spec/containers/0/envFrom/0/secretRef/name",
         "value": "token-metrics-ch-secret-verify"},
        {"op": "replace",
         "path": "/spec/jobTemplate/spec/template/spec/volumes/0/configMap/name",
         "value": "token-metrics-endpoints-verify"},
    ]
    # 패치 경로가 base 구조를 정확히 가리킨다 (volumes[0]=endpoints 계약)
    cj = _cronjob()
    assert _resolve_pointer(cj, ops[0]["path"]) == "token-metrics-ch-secret"
    assert _resolve_pointer(cj, ops[1]["path"]) == "token-metrics-endpoints"


def _deploy_files() -> list:
    yamls = sorted((HERE / "k8s").rglob("*.yaml"))
    assert len(yamls) == 5, yamls  # base 2 + overlays 3
    return [HERE / "Dockerfile", HERE / "build.sh", HERE / "install.sh", *yamls]


def test_no_token_usage_names_anywhere():
    # 기존 모듈 이름·리소스 참조 0 (§7.5 zero-diff / §5.1 이름 전면 교체). 공유는 registry-pull-secret뿐.
    for path in _deploy_files():
        text = path.read_text(encoding="utf-8")
        for forbidden in ("token-usage", "VM_PUSH_URL", "vminsert", "vmsingle", "raw_token_usage"):
            assert forbidden not in text, f"{path.name}: {forbidden}"
    assert "registry-pull-secret" in BASE_CRONJOB.read_text(encoding="utf-8")


def test_dockerfile_and_build_sh_contract():
    d = (HERE / "Dockerfile").read_text(encoding="utf-8")
    assert "ARG BASE_IMAGE=python:3.12-slim" in d
    assert "FROM ${BASE_IMAGE}" in d
    assert "COPY requirements.txt ." in d and "COPY app/ ./app/" in d
    assert 'CMD ["python", "-m", "app.main"]' in d
    assert not any(l.startswith("COPY") and "endpoints" in l for l in d.splitlines())  # ConfigMap이 정본
    b = (HERE / "build.sh").read_text(encoding="utf-8")
    assert 'IMAGE_NAME="token-metrics-collector"' in b
    assert 'REGISTRY="ghcr.io/yoonsungnam"' in b
    assert "harbor.example.internal" in b  # 공개 레포: 사내 주소는 플레이스홀더만
    assert "./collectors/token-metrics/install.sh" in b


def test_install_sh_contract():
    text = (HERE / "install.sh").read_text(encoding="utf-8")
    for needle in (
        'IMAGE_NAME="token-metrics-collector"',
        'CRONJOB_NAME="token-metrics-collector"',
        'SECRET_NAME="token-metrics-ch-secret"',
        'PULL_SECRET_NAME="registry-pull-secret"',
        'CONFIGMAP_NAME="token-metrics-endpoints"',
        'CA_CONFIGMAP_NAME="token-metrics-ca-bundle"',
        'CH_NAMESPACE="clickhouse"',
        "set -euo pipefail",
        "[1/7]", "[2/7]", "[3/7]", "[4/7]", "[5/7]", "[6/7]", "[7/7]",
        "이미 존재합니다 — 네임스페이스 공유 Secret이므로 갱신하지 않습니다",
        "system.databases",
        "프리플라이트 실패: DB 부재",
        "프리플라이트 실패: ClickHouse 접속 불가",                                       # 접속/인증 실패는 DB 부재와 별개 메시지(fix1 #2)
        "dim_token_service_dist",
        "프리플라이트 실패: 토큰 레지스트리 SELECT 불가",
        'clickhouse-client --user "$0" --password "$(cat)" --query "$1"',              # 프리플라이트는 앱 계정으로(GRANT 검증), 비밀번호는 stdin(fix1 #1)
        "jsonpath='{.data.CH_USER}'",                                                   # [2/7] 건너뛴 재설치도 앱 계정을 읽는다
        'apply_sql "${HERE}/${DDL_DIR}/raw_token_metrics.sql"',
        'apply_sql "${HERE}/${DDL_DIR}/dim_token_metrics_service.sql"',
        "endpoints-metrics.company.yaml",
        "token_verify_fact", "token_verify_dim",
        "COLLECTOR_API_CA_BUNDLE=/etc/token-metrics-ca/ca-bundle.pem",
        'ch_host="${ch_pod%-*}.${CH_NAMESPACE}.svc"',
        "harbor.example.internal",
    ):
        assert needle in text, needle
    assert '--password "${ch_pass}"' not in text  # kubectl exec argv에 비밀번호 평문 금지(fix1 #1)
    assert not re.search(r'(echo|printf)[^\n]*\$\{?(ch_pass|reg_pass)', text)          # 비밀 echo/printf 금지(fix1 #3)
    assert "set -x" not in text                                                        # 트레이스로 비밀 노출 금지(fix1 #3)
    assert re.search(r'apply_sql\s+"[^"]*accounts\.sql"', text) is None  # accounts.sql은 admin 수동
    assert text.count('apply_sql "${HERE}/${DDL_DIR}/') == 2
    assert "[1/6]" not in text and "[8/7]" not in text
    assert "dim_token_service.sql" not in text  # 기존 모듈 DDL 파일 — 여기서 적용하지 않음
    lines = text.splitlines()
    assert lines[0] == "#!/usr/bin/env bash"
    assert all(l.startswith("#") for l in lines[1:14])  # usage()의 sed -n '2,14p' 범위와 정합
    assert "sed -n '2,14p'" in text


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash not on PATH")
def test_scripts_executable_and_bash_syntax():
    for name in ("build.sh", "install.sh"):
        path = HERE / name
        assert os.access(path, os.X_OK), f"{name} must be chmod +x"
        subprocess.run(["bash", "-n", str(path)], check=True)
    # 인자 없이 → usage + exit 1 (kubectl 불필요 경로)
    r = subprocess.run(["bash", str(HERE / "install.sh")], capture_output=True, text=True)
    assert r.returncode == 1 and "사용법" in r.stdout
    r = subprocess.run(["bash", str(HERE / "install.sh"), "company"], capture_output=True, text=True)
    assert r.returncode == 1 and "--context" in r.stdout


@pytest.mark.skipif(shutil.which("kubectl") is None, reason="kubectl not on PATH")
def test_kustomize_render_if_available():
    def render(overlay: str) -> str:
        return subprocess.run(["kubectl", "kustomize", str(OVERLAYS / overlay)],
                              check=True, capture_output=True, text=True).stdout

    rendered = {o: render(o) for o in ("stage", "company", "company-verify")}
    for overlay, out in rendered.items():
        for needle in (
            "schedule: 5 2-9 * * *", "timeZone: Asia/Seoul", "concurrencyPolicy: Forbid",
            "startingDeadlineSeconds: 540", "activeDeadlineSeconds: 3000", "backoffLimit: 0",
            "successfulJobsHistoryLimit: 3", "memory: 1Gi", "memory: 256Mi",
            "name: registry-pull-secret", "name: token-metrics-ca-bundle",
            "name: METRICS_MAX_MUTATIONS_PER_RUN",
        ):
            assert needle in out, f"{overlay}: {needle}"
        assert "token-usage" not in out, overlay
    assert "ghcr.io/yoonsungnam/token-metrics-collector:latest" in rendered["stage"]
    assert "image: token-metrics-collector:latest" in rendered["company"]
    assert "name: token-metrics-ch-secret\n" in rendered["company"]
    assert "name: token-metrics-endpoints\n" in rendered["company"]
    verify = rendered["company-verify"]
    assert "name: token-metrics-collector-verify" in verify
    assert "name: token-metrics-ch-secret-verify" in verify
    assert "name: token-metrics-endpoints-verify" in verify
    assert "name: token-metrics-ca-bundle-verify" not in verify  # ca-bundle은 접미 없이 공용
