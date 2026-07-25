#!/usr/bin/env python3
"""실측 정류장 좌표 해석 — 사용자가 준 정류장 번호(ARS)로 TAGO에서 좌표를 가져온다.

TMAP 대중교통·보행자 API는 종점을 장소(건물) 좌표로 잡을 수 있어 실제 정류장과
어긋난다 (✅ Case 1 E1: 신당누리센터 '건물' 939m vs 실제 정류장 781m). 그 어긋남이
도보시간을 부풀려 연결 안내를 뒤집었다 — 모델이 아니라 종점 좌표가 틀린 것이다.

그래서 정류장 지점은 **실측 좌표**로 해석한다:
  1. lon/lat 직접 지정이 있으면 그대로 쓴다 (이미 실측한 좌표).
  2. 없으면 사용자가 준 정류장 번호(ARS)로 TAGO 정류소정보(getSttnNoList)를
     역조회해 좌표를 가져온다 (✅ Case 1 E3 에서 07492→nodeno 7492, 07488→7488 로 적용한 방식).

정류소정보(15098534)는 노선정보·위치정보와 같은 data.go.kr 키로 열린다
(계정 공통). 키는 STOP_API_KEY → GBIS_BUS_KEY → DATA_GO_KR_KEY 순으로 찾는다.
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
ENV_FILES = (os.path.join(HERE, ".env"), os.path.join(PROJECT_ROOT, ".env"))
BASE = "https://apis.data.go.kr/1613000/BusSttnInfoInqireService/getSttnNoList"


def _dotenv_value(path: str, name: str) -> str | None:
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


def load_key() -> str:
    key = (
        os.environ.get("STOP_API_KEY")
        or os.environ.get("GBIS_BUS_KEY")
        or os.environ.get("DATA_GO_KR_KEY")
    )
    for path in ENV_FILES:
        key = (
            key
            or _dotenv_value(path, "STOP_API_KEY")
            or _dotenv_value(path, "GBIS_BUS_KEY")
            or _dotenv_value(path, "DATA_GO_KR_KEY")
        )
    if not key:
        raise RuntimeError(
            "정류장 좌표 조회 키가 없습니다. STOP_API_KEY / GBIS_BUS_KEY / "
            "DATA_GO_KR_KEY 중 하나를 spike/validation/.env 또는 find-path/.env에 "
            "추가하세요 (data.go.kr 정류소정보 15098534 승인 키)."
        )
    return key


def _request_json(url: str, timeout_s: int = 15) -> dict:
    try:
        with urllib.request.urlopen(url, timeout=timeout_s) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:200]
        raise RuntimeError(f"정류소 API HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"정류소 API 연결 실패: {exc.reason}") from exc


def fetch_stop_by_ars(
    city_code: int | str, ars: int | str, key: str | None = None, name: str | None = None
) -> dict:
    """정류장 번호(ARS)로 실측 좌표를 역조회한다 — TAGO getSttnNoList.

    ⚠️ ARS 표기의 앞자리 0은 뗀다 (✅ 실측: 화면 표기 07492 → nodeno 7492).
    ⚠️ 실패가 HTTP 200 으로 올 수 있어 resultCode 를 반드시 확인한다 (§2.1).
    """
    nodeno = str(ars).strip()
    if nodeno.isdigit():
        nodeno = str(int(nodeno))
    query = urllib.parse.urlencode(
        {
            "serviceKey": key or load_key(),
            "_type": "json",
            "cityCode": city_code,
            "nodeno": nodeno,
            "numOfRows": 10,
        }
    )
    payload = _request_json(f"{BASE}?{query}")
    header = (payload.get("response") or {}).get("header") or {}
    code = str(header.get("resultCode", "?"))
    if code not in ("00", "0"):
        raise RuntimeError(
            f"정류소 조회 실패 code{code}: {header.get('resultMsg', '')[:40]}"
        )
    body = (payload.get("response") or {}).get("body") or {}
    items = ((body.get("items") or {}).get("item") or []) if isinstance(body, dict) else []
    if isinstance(items, dict):
        items = [items]
    if not items:
        raise ValueError(f"정류장 번호 {nodeno}(도시 {city_code})를 찾지 못했습니다.")
    item = items[0]
    return {
        "lon": float(item["gpslong"]),
        "lat": float(item["gpslati"]),
        "name": name or item.get("nodenm") or f"정류장 {nodeno}",
        "nodeid": item.get("nodeid"),
        "nodeno": item.get("nodeno"),
    }


def resolve_stop(config: dict, key: str | None = None) -> dict:
    """정류장 지점 → {lon, lat, name, ...}.

    - lon/lat 직접 지정: 실측 좌표 그대로 (이미 확인한 정류장 좌표)
    - stop{city_code, ars}: TAGO 역조회로 실측 좌표를 가져온다
    둘 다 있으면 lon/lat 를 우선한다 (오프라인 재현 + ID 는 출처 기록용).
    """
    if "lon" in config and "lat" in config:
        return {
            "lon": float(config["lon"]),
            "lat": float(config["lat"]),
            "name": str(config.get("name", "정류장")),
            "nodeid": config.get("nodeid"),
            "nodeno": config.get("nodeno"),
        }
    stop = config.get("stop")
    if stop and stop.get("ars") is not None and stop.get("city_code") is not None:
        return fetch_stop_by_ars(stop["city_code"], stop["ars"], key, stop.get("name"))
    raise ValueError(
        "정류장 지점에는 lon/lat 직접 좌표 또는 stop{city_code, ars}가 필요합니다."
    )
