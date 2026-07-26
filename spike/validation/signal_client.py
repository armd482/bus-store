#!/usr/bin/env python3
"""TMAP 횡단보도와 교통안전 실시간 보행신호를 결합한다.

설계 문서 §7의 정책을 그대로 따른다.

* 첫 도보 + 짧은 예측 지평: 실시간 상태/잔여시간을 주기에 투영
* 뒤쪽 도보 또는 실시간 적용 불가: 주기 기반 기대대기
* 실시간 위상·주기 기대대기 모두 불가: TMAP 횡단거리별 보수 상한
* TMAP 횡단보도도 없음: 신호 모델 unavailable
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
LOCAL_ENV_FILE = os.path.join(HERE, ".env")
ROOT_ENV_FILE = os.path.join(PROJECT_ROOT, ".env")
CALIBRATION_FILE = os.path.join(HERE, "signal_calibration.json")
BASE_URL = "https://apis.data.go.kr/B551982/rti"
DIRECTIONS = ("nt", "ne", "et", "se", "st", "sw", "wt", "nw")
GREEN_STATE = "protected-Movement-Allowed"
RED_STATE = "stop-And-Remain"
NO_DATA_CS = 36001
SENSITIVITY_SECONDS = {"OTP15": 15.0, "Expected20": 20.0}


def load_road_upper_bands(path: str = CALIBRATION_FILE) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        payload = json.load(f)
    bands = payload.get("bands") or []
    if not bands:
        raise ValueError("signal_calibration.json에 bands가 없습니다.")
    for band in bands:
        upper_s = float(band["upper_s"])
        if upper_s <= 0:
            raise ValueError(f"잘못된 신호 상한: {band}")
    return bands


ROAD_UPPER_BANDS = load_road_upper_bands()


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
        or os.environ.get("DATA_GO_KR_KEY")
        or _dotenv_value(LOCAL_ENV_FILE, "SIGNAL_API_KEY")
        or _dotenv_value(ROOT_ENV_FILE, "SIGNAL_API_KEY")
        or _dotenv_value(ROOT_ENV_FILE, "DATA_GO_KR_KEY")
    )
    if not key:
        raise RuntimeError(
            "SIGNAL_API_KEY 또는 DATA_GO_KR_KEY가 없습니다. "
            "spike/validation/.env 또는 find-path/.env에 추가하세요."
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


def road_upper_seconds(crossing: dict) -> float:
    """횡단거리 밴드의 **보수적 경험 추정치**(empirical conservative estimate)를 반환한다.

    ⚠️ 이름과 달리 **수학적 상한이 아니다** — 관측 3건(15m→3.54s·16m→45.52s·24m→120.77s)
    으로 만든 20m↓60s·초과150s 2밴드 경험값이라, 주기가 더 긴 신호에선 150s 를 넘을 수
    있다(보장 없음). ⚠️ 또 밴드는 병합 전 단일세그먼트 거리로 학습됐는데 지금은 병합 신호
    거리에 적용된다 — 병합 단위로 재학습 필요(signal_calibration.json `_caveat`).
    절벽 trip 과소예측 방지엔 보수적이라 쓰되 '항상 안전한 상한'으로 신뢰하지 말 것.
    """
    distance_m = float(crossing["distance_m"])
    if distance_m <= 0:
        raise ValueError(f"잘못된 횡단거리: {distance_m}")
    for band in ROAD_UPPER_BANDS:
        maximum = band.get("max_distance_m")
        if maximum is None or distance_m <= float(maximum):
            return float(band["upper_s"])
    raise ValueError(f"횡단거리 밴드가 범위를 덮지 못합니다: {distance_m}")


def fallback_signal_sensitivity(crossings: list[dict]) -> dict:
    """주기·실시간이 없을 때 설계 문서 §7.4의 fallback을 계산한다.

    RoadUpper는 제품·공식 평가값이고 OTP15와 Expected20은 민감도 값이다.
    ⚠️ 입력 crossings 는 이미 신호 단위로 병합된 것이어야 한다 — 2단계 횡단의
    세그먼트 분할은 tmap_client.merge_adjacent_crosswalks 가 상류에서 합친다
    (여기서 crossing 마다 상한을 더하므로, 세그먼트째 들어오면 과다 계상된다).
    """
    raw_count = len(crossings)
    crossing_upper_bounds = [
        {
            "feature_index": crossing.get("feature_index"),
            "distance_m": float(crossing["distance_m"]),
            "road_type": crossing.get("road_type"),
            "category_road_type": crossing.get("category_road_type"),
            "upper_s": road_upper_seconds(crossing),
        }
        for crossing in crossings
    ]
    return {
        "raw_count": raw_count,
        "crossing_upper_bounds": crossing_upper_bounds,
        "road_upper_s": sum(row["upper_s"] for row in crossing_upper_bounds),
        "raw_waits_s": {
            name: seconds * raw_count
            for name, seconds in SENSITIVITY_SECONDS.items()
        },
    }


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
    """균일 위상 기대대기 (설계 문서 §7.1·§7.4).

    E[wait] = (cycle − effective_green)² / (2·cycle), effective_green = green − dist/speed.
    유효 녹색 규칙(§7.4: 녹색 잔여 ≥ 횡단시간)과 일치 — 빠른 사용자일수록 대기가 준다
    (옛 red²/(2·cycle) 은 속도 무관이라 개인화가 빠졌다). 횡단 이동시간은 도보시간에
    이미 포함되므로 대기창에만 반영하고 따로 더하지 않는다.
    """
    crossing_s = dist_m / speed_mps
    if crossing_s > green_s:
        raise ValueError("crossing cannot finish during pedestrian green")
    effective_green = green_s - crossing_s
    return (cycle_s - effective_green) ** 2 / (2 * cycle_s)


# ── 유도 추정 — cycle/green 이 없을 때 채워진 '횡단거리'로 만든다 ──────────
# (docs/signal-data-pipeline.md §3.1) 표준데이터에 타이밍이 비어도 횡단거리·도로등급은
# 대체로 있으므로, 순수 추측이 아니라 경찰청 매뉴얼 기반으로 green 을 유도한다.
# ⚠️ RoadUpper 와 성격이 다르다: RoadUpper 는 '보수 추정'(대개 과대이나 상한 보장 아님),
#    유도는 '평균'(양방향). 둘 다 병합 신호 거리로 계산한다.
DESIGN_SPEED_MPS = 1.0    # 경찰청 매뉴얼: 보행녹색시간 산정용 설계속도
ENTRY_TIME_S = 7.0        # 진입시간 (보행녹색 앞 고정분)
DEFAULT_CYCLE_S = 150.0   # 도로등급 미상 시 도시 신호주기 prior (간선 통상). 이면도로면 ~90


def derived_timing(dist_m: float, cycle_s: float = DEFAULT_CYCLE_S) -> tuple[float, float]:
    """횡단거리 → (cycle, green). green = 진입 7s + 횡단거리 ÷ 1.0 m/s (경찰청)."""
    if dist_m <= 0:
        raise ValueError("dist_m must be positive")
    green = ENTRY_TIME_S + dist_m / DESIGN_SPEED_MPS
    green = min(green, cycle_s - 5.0)   # red 최소 5s 보장 (green 이 cycle 을 못 삼키게)
    return cycle_s, green


def derived_wait(dist_m: float, cycle_s: float = DEFAULT_CYCLE_S) -> float:
    """유도 cycle/green 으로 균일 위상 기대대기 red²/(2·cycle) 를 계산한다."""
    cyc, green = derived_timing(dist_m, cycle_s)
    red = cyc - green
    return red ** 2 / (2 * cyc)


def fallback_derived_total(crossings: list[dict], cycle_s: float = DEFAULT_CYCLE_S) -> float:
    """유도값 합계 — RoadUpper 대신 쓸 수 있는 '평균' 추정. 배포 가능(도보 전 아는 값만)."""
    return sum(derived_wait(float(c["distance_m"]), cycle_s) for c in crossings)


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
    """경로 전체 신호대기와 각 횡단보도에서 쓴 근거를 반환한다.

    실시간 위상과 주기 기반 기대대기 중 어느 것도 계산할 수 없으면
    설계 문서 §7.4에 따라 TMAP 횡단거리별 보수 상한을 공식 예측
    fallback으로 쓴다. TMAP 횡단보도 자체가 없을 때만 신호대기 0초
    (신호 없음)가 된다.
    """
    signal_by_id = {
        (str(row.get("stdgCd")), str(row.get("crsrdId"))): row
        for row in signal_rows
    }
    total = 0.0
    details = []
    complete = True
    accumulated_wait = 0.0
    unresolved_crossings = []
    for crossing_index, crossing in enumerate(tmap_crossings):
        intersection, distance = nearest_intersection(crossing, intersections)
        if intersection is None:
            complete = False
            unresolved_crossings.append(crossing)
            upper_s = road_upper_seconds(crossing)
            total += upper_s
            accumulated_wait += upper_s
            details.append(
                {
                    "method": "tmap-road-upper-fallback",
                    "nearest_m": distance,
                    "distance_m": float(crossing["distance_m"]),
                    "wait_s": upper_s,
                    "reason": "intersection unavailable",
                }
            )
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
            unresolved_crossings.append(crossing)
            upper_s = road_upper_seconds(crossing)
            total += upper_s
            accumulated_wait += upper_s
            details.append(
                {
                    "method": "tmap-road-upper-fallback",
                    "intersection": intersection.get("crsrdNm"),
                    "direction": direction,
                    "nearest_m": distance,
                    "distance_m": float(crossing["distance_m"]),
                    "wait_s": upper_s,
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
    fallback = (
        fallback_signal_sensitivity(unresolved_crossings)
        if unresolved_crossings
        else None
    )
    return {
        "wait_s": total,
        "complete": complete,
        "used_fallback": fallback is not None,
        "fallback": fallback,
        "crossings": len(tmap_crossings),
        "details": details,
    }
