#!/usr/bin/env python3
"""TMAP 횡단보도와 교통안전 실시간 보행신호를 결합한다.

설계 문서 §7의 정책을 그대로 따른다.

* 첫 도보 + 짧은 예측 지평: 실시간 상태/잔여시간을 주기에 투영
* 뒤쪽 도보 또는 실시간 적용 불가: 주기 기반 기대대기
* 주기조차 모름: 값을 꾸며내지 않고 unavailable
"""
from __future__ import annotations

import json
import math
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
ENV_FILE = os.path.join(PROJECT_ROOT, "collector", ".env")
BASE_URL = "https://apis.data.go.kr/B551982/rti"
DIRECTIONS = ("nt", "ne", "et", "se", "st", "sw", "wt", "nw")
GREEN_STATE = "protected-Movement-Allowed"
RED_STATE = "stop-And-Remain"
NO_DATA_CS = 36001


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
        os.environ.get("SIGNAL_API_KEY")
        or _dotenv_value(ENV_FILE, "SIGNAL_API_KEY")
        or os.environ.get("GBIS_BUS_KEY")
        or _dotenv_value(ENV_FILE, "GBIS_BUS_KEY")
    )
    if not key:
        raise RuntimeError(
            "SIGNAL_API_KEY 또는 GBIS_BUS_KEY가 없습니다. collector/.env에 추가하세요."
        )
    return key


def _items(payload: dict) -> list[dict]:
    body = payload.get("body") or (payload.get("response") or {}).get("body") or {}
    raw = body.get("items")
    batch = (raw or {}).get("item", []) if isinstance(raw, dict) else (raw or [])
    if isinstance(batch, dict):
        return [batch]
    return list(batch)


def fetch_all(endpoint: str, key: str | None = None, timeout_s: int = 30) -> list[dict]:
    """서버측 crsrdId 필터를 믿지 않고 전 페이지를 가져온다."""
    api_key = key or load_key()
    rows: list[dict] = []
    page = 1
    while True:
        query = urllib.parse.urlencode(
            {
                "serviceKey": api_key,
                "pageNo": page,
                "numOfRows": 1000,
                "type": "json",
            }
        )
        request = urllib.request.Request(f"{BASE_URL}/{endpoint}?{query}")
        try:
            with urllib.request.urlopen(request, timeout=timeout_s) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:300]
            raise RuntimeError(f"신호 API HTTP {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"신호 API 연결 실패: {exc.reason}") from exc
        batch = _items(payload)
        rows.extend(batch)
        body = payload.get("body") or (payload.get("response") or {}).get("body") or {}
        total = int(body.get("totalCount") or len(rows))
        if not batch or len(rows) >= total:
            return rows
        page += 1


def haversine_m(a: tuple[float, float], b: tuple[float, float]) -> float:
    lon1, lat1 = map(math.radians, a)
    lon2, lat2 = map(math.radians, b)
    dlon, dlat = lon2 - lon1, lat2 - lat1
    h = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    return 2 * 6_371_000 * math.asin(math.sqrt(h))


def nearest_intersection(
    crossing: dict, intersections: list[dict], max_distance_m: float = 50
) -> tuple[dict | None, float | None]:
    if not intersections:
        return None, None
    point = (float(crossing["lon"]), float(crossing["lat"]))
    candidates = []
    for row in intersections:
        try:
            coord = (float(row["mapCtptIntLot"]), float(row["mapCtptIntLat"]))
        except (KeyError, TypeError, ValueError):
            continue
        candidates.append((haversine_m(point, coord), row))
    if not candidates:
        return None, None
    distance, row = min(candidates, key=lambda item: item[0])
    return (row, distance) if distance <= max_distance_m else (None, distance)


def direction_from_geometry(crossing: dict) -> str:
    """횡단 진행 방위를 8방향 RTI 필드명으로 근사한다.

    지자체 방향 정의가 다른 교차로는 crosswalks.json의 ``direction``으로
    반드시 덮어쓸 수 있게 한다.
    """
    start, end = crossing["start"], crossing["end"]
    dx = (float(end[0]) - float(start[0])) * math.cos(
        math.radians((float(start[1]) + float(end[1])) / 2)
    )
    dy = float(end[1]) - float(start[1])
    bearing = (math.degrees(math.atan2(dx, dy)) + 360) % 360
    index = int((bearing + 22.5) // 45) % 8
    return DIRECTIONS[index]


def signal_state(row: dict, direction: str) -> tuple[str, float] | None:
    state = row.get(f"{direction}PdsgSttsNm")
    raw = row.get(f"{direction}PdsgRmndCs")
    try:
        remain_cs = int(raw)
    except (TypeError, ValueError):
        return None
    if remain_cs == NO_DATA_CS or remain_cs < 0:
        return None
    if state not in (GREEN_STATE, RED_STATE):
        return None
    return state, remain_cs / 100


def projected_wait(
    state: str,
    remaining_s: float,
    seconds_ahead: float,
    cycle_s: float,
    green_s: float,
    dist_m: float,
    speed_mps: float,
) -> float:
    """현재 상태를 고정 주기로 앞으로 투영해 추가 대기를 계산한다."""
    if not (0 < green_s < cycle_s):
        raise ValueError("green_s must be between 0 and cycle_s")
    crossing_s = dist_m / speed_mps
    if crossing_s > green_s:
        raise ValueError("crossing cannot finish during pedestrian green")
    red_s = cycle_s - green_s
    if state == GREEN_STATE:
        phase_now = green_s - min(remaining_s, green_s)
    elif state == RED_STATE:
        phase_now = green_s + red_s - min(remaining_s, red_s)
    else:
        raise ValueError(f"unknown signal state: {state}")
    phase = (phase_now + max(0.0, seconds_ahead)) % cycle_s
    effective_green = green_s - crossing_s
    return 0.0 if phase <= effective_green else cycle_s - phase


def expected_wait(cycle_s: float, green_s: float, dist_m: float, speed_mps: float) -> float:
    crossing_s = dist_m / speed_mps
    if crossing_s > green_s:
        raise ValueError("crossing cannot finish during pedestrian green")
    red_s = cycle_s - green_s
    return (red_s + crossing_s) ** 2 / (2 * cycle_s)


def infer_timing(samples: list[tuple[datetime, str]]) -> dict:
    """폴링한 상태 전이로 녹색시간과 전체 주기를 추정한다.

    샘플은 같은 ``crsrdId + direction``이어야 한다. 최소 두 번의 녹색 시작이
    있어야 주기를 만들며, 폴링 간격보다 정밀하다고 주장하지 않는다.
    """
    if len(samples) < 3:
        raise ValueError("신호 주기 추정에는 상태 샘플이 더 필요합니다.")
    ordered = sorted(samples, key=lambda item: item[0])
    green_starts = []
    green_ends = []
    previous = None
    for observed_at, state in ordered:
        if state not in (GREEN_STATE, RED_STATE):
            continue
        if previous is not None and previous[1] != state:
            if state == GREEN_STATE:
                green_starts.append(observed_at)
            else:
                green_ends.append(observed_at)
        previous = (observed_at, state)
    cycles = [
        (b - a).total_seconds()
        for a, b in zip(green_starts, green_starts[1:])
        if b > a
    ]
    greens = []
    for start in green_starts:
        end = next((value for value in green_ends if value > start), None)
        if end is not None:
            greens.append((end - start).total_seconds())
    if not cycles or not greens:
        raise ValueError("녹색 시작 2회와 녹색 종료 1회 이상이 필요합니다.")
    return {
        "cycle_s": sum(cycles) / len(cycles),
        "green_s": sum(greens) / len(greens),
        "cycles_observed": len(cycles),
        "greens_observed": len(greens),
    }


def _static_for(
    intersection: dict, direction: str, crosswalks: dict
) -> tuple[str | None, dict | None, str]:
    stdg = str(intersection.get("stdgCd"))
    crsrd = str(intersection.get("crsrdId"))
    for cwid, row in crosswalks.items():
        if cwid.startswith("_") or not isinstance(row, dict):
            continue
        if (
            str(row.get("stdg_cd")) == stdg
            and str(row.get("crsrd_id")) == crsrd
        ):
            return cwid, row, str(row.get("direction", direction))
    return None, None, direction


def estimate_route_wait(
    tmap_crossings: list[dict],
    crosswalks: dict,
    walk_start: datetime,
    is_first_walk: bool,
    speed_mps: float,
    intersections: list[dict],
    signal_rows: list[dict],
    fetched_at: datetime,
    max_live_horizon_s: float = 180,
) -> dict:
    """경로 전체 신호대기와 각 횡단보도에서 쓴 근거를 반환한다."""
    signal_by_id = {
        (str(row.get("stdgCd")), str(row.get("crsrdId"))): row
        for row in signal_rows
    }
    total = 0.0
    details = []
    complete = True
    accumulated_wait = 0.0
    for crossing_index, crossing in enumerate(tmap_crossings):
        intersection, distance = nearest_intersection(crossing, intersections)
        if intersection is None:
            complete = False
            details.append({"method": "unavailable", "nearest_m": distance})
            continue
        direction = direction_from_geometry(crossing)
        cwid, static, direction = _static_for(
            intersection, direction, crosswalks
        )
        dist_m = float((static or {}).get("dist_m", crossing["distance_m"]))
        cycle_s = (static or {}).get("cycle_s")
        green_s = (static or {}).get("green_s")
        seconds_ahead = (
            walk_start - fetched_at
        ).total_seconds() + float(crossing["offset_s"]) + accumulated_wait
        live = signal_by_id.get(
            (str(intersection.get("stdgCd")), str(intersection.get("crsrdId")))
        )
        state = signal_state(live, direction) if live else None
        can_project_live = (
            is_first_walk
            and crossing_index == 0
            and state is not None
            and cycle_s is not None
            and green_s is not None
            and 0 <= seconds_ahead <= max_live_horizon_s
        )
        if can_project_live:
            wait_s = projected_wait(
                state[0],
                state[1],
                seconds_ahead,
                float(cycle_s),
                float(green_s),
                dist_m,
                speed_mps,
            )
            method = "live-phase"
        elif cycle_s is not None and green_s is not None:
            wait_s = expected_wait(
                float(cycle_s), float(green_s), dist_m, speed_mps
            )
            method = "expected"
        else:
            complete = False
            details.append(
                {
                    "method": "unavailable",
                    "intersection": intersection.get("crsrdNm"),
                    "direction": direction,
                    "nearest_m": distance,
                    "reason": "cycle/green missing",
                }
            )
            continue
        total += wait_s
        accumulated_wait += wait_s
        details.append(
            {
                "method": method,
                "crosswalk_id": cwid,
                "intersection": intersection.get("crsrdNm"),
                "direction": direction,
                "nearest_m": distance,
                "wait_s": wait_s,
            }
        )
    return {
        "wait_s": total if complete else None,
        "complete": complete,
        "crossings": len(tmap_crossings),
        "details": details,
    }
