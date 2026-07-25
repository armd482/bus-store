"""find-path 환경변수 계층.

우선순위:
  1. 운영체제 환경변수
  2. collector/.env
  3. find-path/.env
"""
import os

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(HERE, ".."))
COMPONENT_ENV_FILE = os.path.join(HERE, ".env")
COMMON_ENV_FILES = (
    os.path.join(PROJECT_ROOT, ".env"),
    os.path.join(HERE, ".env.common"),  # macOS TCC 배포본용 루트 키 복사본
)


def _dotenv_value(path, name):
    try:
        with open(path, encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                if key.strip() == name:
                    return value.strip().strip('"').strip("'") or None
    except OSError:
        return None
    return None


def get(name, common_fallback=None):
    value = os.environ.get(name)
    if value:
        return value
    if common_fallback:
        value = os.environ.get(common_fallback)
        if value:
            return value
    value = _dotenv_value(COMPONENT_ENV_FILE, name)
    if value:
        return value
    for path in COMMON_ENV_FILES:
        value = _dotenv_value(path, name)
        if value:
            return value
    if common_fallback:
        for path in COMMON_ENV_FILES:
            value = _dotenv_value(path, common_fallback)
            if value:
                return value
    return None
