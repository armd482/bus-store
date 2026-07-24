#!/usr/bin/env python3
"""TMAP 보행자 경로를 개인 보행시간으로 변환한다.

원본 응답은 저장하지 않고 현재 평가 실행의 메모리에서만 사용한다.
"""
from __future__ import annotations

import json
import math
import os
import urllib.error
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
URL = "https://apis.openapi.sk.com/tmap/routes/pedestrian?version=1&format=json"
KEY_FILES = (
    os.path.join(PROJECT_ROOT, "collector", ".env"),
    os.path.abspath(os.path.join(PROJECT_ROOT, "..", "bus-test", ".env.local")),
)

# 문서에 확인된 시설 유형만 보수적으로 보정한다.
# 나머지는 개인 평지 속도 1.0배로 계산한다.
DEFAULT_FACILITY_MULTIPLIERS = {
    17: 0.875,  # 계단: 기존 모델의 1.05 / 평지 1.20
    14: 0.775,  # 지하도: 기존 모델의 0.93 / 평지 1.20
}


def _dotenv_value(path: str, name: str) -> str | None:
    try:
        with open(path, encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                if key.strip() == name:
                    value = value.strip().strip('"').strip("'")
                    return value or None
    except OSError:
        return None
    return None


def load_key() -> str:
    key = os.environ.get("TMAP_APP_KEY")
    if key:
        return key
    for path in KEY_FILES:
        key = _dotenv_value(path, "TMAP_APP_KEY")
        if key:
            return key
    raise RuntimeError(
        "TMAP_APP_KEY가 없습니다. collector/.env에 TMAP_APP_KEY=...를 추가하세요."
    )


def _point(config: dict, key: str) -> tuple[float, float, str]:
    point = config[key]
    lon = float(point["lon"])
    lat = float(point["lat"])
    name = str(point.get("name", key))
    if not (-180 <= lon <= 180 and -90 <= lat <= 90):
        raise ValueError(f"잘못된 {key} 좌표: lon={lon}, lat={lat}")
    return lon, lat, name


def fetch_pedestrian(config: dict, key: str | None = None, timeout_s: int = 15) -> dict:
    """이벤트의 tmap.start/end 좌표로 보행자 경로를 한 번 조회한다."""
    sx, sy, start_name = _point(config, "start")
    ex, ey, end_name = _point(config, "end")
    body = urllib.parse.urlencode(
        {
            "startX": f"{sx:.7f}",
            "startY": f"{sy:.7f}",
            "endX": f"{ex:.7f}",
            "endY": f"{ey:.7f}",
            "startName": start_name,
            "endName": end_name,
            "reqCoordType": "WGS84GEO",
            "resCoordType": "WGS84GEO",
        }
    ).encode()
    request = urllib.request.Request(
        URL,
        data=body,
        headers={
            "appKey": key or load_key(),
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_s) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:300]
        raise RuntimeError(f"TMAP HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"TMAP 연결 실패: {exc.reason}") from exc


def _haversine_m(a: list[float], b: list[float]) -> float:
    lon1, lat1 = map(math.radians, a[:2])
    lon2, lat2 = map(math.radians, b[:2])
    dlon, dlat = lon2 - lon1, lat2 - lat1
    h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * 6_371_000 * math.asin(math.sqrt(h))


def _geometry_distance_m(coordinates: list) -> float:
    return sum(_haversine_m(a, b) for a, b in zip(coordinates, coordinates[1:]))


def extract_segments(payload: dict) -> list[dict]:
    """TMAP GeoJSON LineString을 거리·시설유형 세그먼트로 정규화한다."""
    segments = []
    for feature in payload.get("features", []):
        geometry = feature.get("geometry") or {}
        if geometry.get("type") != "LineString":
            continue
        properties = feature.get("properties") or {}
        coordinates = geometry.get("coordinates") or []
        distance = properties.get("distance")
        try:
            distance_m = float(distance)
        except (TypeError, ValueError):
            distance_m = _geometry_distance_m(coordinates)
        if distance_m <= 0:
            continue

        raw_type = properties.get("facilityType")
        try:
            facility_type = int(raw_type) if raw_type not in (None, "") else None
        except (TypeError, ValueError):
            facility_type = None
        segments.append(
            {
                "distance_m": distance_m,
                "facility_type": facility_type,
                "feature_index": properties.get("index"),
            }
        )
    if not segments:
        raise ValueError("TMAP 응답에서 보행 LineString 세그먼트를 찾지 못했습니다.")
    return segments


def extract_crosswalks(payload: dict, speed_mps: float) -> list[dict]:
    """TMAP 횡단보도 세그먼트를 신호 매칭용 위치와 도착 오프셋으로 만든다.

    ``offset_s``는 신호 대기를 제외하고 해당 횡단보도 시작점까지 걷는 시간이다.
    실시간 신호는 첫 도보에서 이 시각에만 투영한다.
    """
    if speed_mps <= 0:
        raise ValueError("speed_mps must be positive")
    elapsed_s = 0.0
    rows = []
    for feature in payload.get("features", []):
        geometry = feature.get("geometry") or {}
        if geometry.get("type") != "LineString":
            continue
        properties = feature.get("properties") or {}
        coordinates = geometry.get("coordinates") or []
        if len(coordinates) < 2:
            continue
        try:
            distance_m = float(properties.get("distance"))
        except (TypeError, ValueError):
            distance_m = _geometry_distance_m(coordinates)
        if distance_m <= 0:
            continue
        try:
            facility_type = int(properties.get("facilityType"))
        except (TypeError, ValueError):
            facility_type = None
        if facility_type == 15:
            middle = coordinates[len(coordinates) // 2]
            rows.append(
                {
                    "feature_index": properties.get("index"),
                    "distance_m": distance_m,
                    "offset_s": elapsed_s,
                    "lon": float(middle[0]),
                    "lat": float(middle[1]),
                    "start": [float(coordinates[0][0]), float(coordinates[0][1])],
                    "end": [float(coordinates[-1][0]), float(coordinates[-1][1])],
                }
            )
        elapsed_s += distance_m / speed_mps
    return rows


def personal_walk_seconds(
    segments: list[dict],
    speed_mps: float,
    multipliers: dict[int, float] | None = None,
) -> float:
    if speed_mps <= 0:
        raise ValueError("speed_mps must be positive")
    factors = dict(DEFAULT_FACILITY_MULTIPLIERS)
    if multipliers:
        factors.update({int(k): float(v) for k, v in multipliers.items()})

    total = 0.0
    for segment in segments:
        distance_m = float(segment["distance_m"])
        factor = factors.get(segment.get("facility_type"), 1.0)
        if distance_m <= 0 or factor <= 0:
            raise ValueError(f"잘못된 TMAP 세그먼트: {segment}")
        total += distance_m / (speed_mps * factor)
    return total


def event_walk_seconds(sc: dict, key: str | None = None) -> tuple[float, dict]:
    config = sc["tmap"]
    payload = fetch_pedestrian(config, key)
    segments = extract_segments(payload)
    _, speed_mps = _profile_without_score_import(sc)
    seconds = personal_walk_seconds(
        segments, speed_mps, config.get("facility_speed_multipliers")
    )
    summary = {
        "distance_m": sum(s["distance_m"] for s in segments),
        "segments": len(segments),
        "crosswalk_segments": sum(s["facility_type"] == 15 for s in segments),
        "crosswalks": extract_crosswalks(payload, speed_mps),
    }
    return seconds, summary


def _profile_without_score_import(sc: dict) -> tuple[float, float]:
    profile = sc.get("profile", {})
    return (
        float(profile.get("walk_time_ratio", 0.68)),
        float(profile.get("speed_mps", 1.66)),
    )
