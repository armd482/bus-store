#!/usr/bin/env python3
"""TMAP 대중교통 뼈대에 개인화 도보와 보행신호를 연결한다.

TMAP 대중교통 응답의 모든 WALK leg를 보행자 API로 다시 조회한다.
대중교통 leg는 응답의 sectionTime을 사용한다. TMAP 대중교통 응답에는
승차 대기시간이 없으므로, 제품 총 도착시간을 만들려면 transit_waits_s를
별도로 넣어야 한다. 누락하면 0으로 숨기지 않고 ``complete=False``로 남긴다.
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from datetime import datetime, timedelta

from signal_client import estimate_route_wait
from tmap_client import (
    extract_crosswalks,
    extract_segments,
    fetch_pedestrian,
    load_key,
    personal_walk_seconds,
)

TRANSIT_URL = "https://apis.openapi.sk.com/transit/routes"
TRANSIT_MODES = {"BUS", "SUBWAY", "TRAIN", "EXPRESSBUS", "AIRPLANE", "FERRY"}


def fetch_transit(config: dict, key: str | None = None, timeout_s: int = 30) -> dict:
    """TMAP 대중교통 후보를 조회한다."""
    api_key = key or load_key()
    start = config["start"]
    end = config["end"]
    body = {
        "startX": str(float(start["lon"])),
        "startY": str(float(start["lat"])),
        "endX": str(float(end["lon"])),
        "endY": str(float(end["lat"])),
        "lang": int(config.get("lang", 0)),
        "format": "json",
        "count": int(config.get("count", 10)),
    }
    if config.get("search_dttm"):
        body["searchDttm"] = str(config["search_dttm"])
    request = urllib.request.Request(
        TRANSIT_URL,
        data=json.dumps(body).encode(),
        headers={"appKey": api_key, "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_s) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:300]
        raise RuntimeError(f"TMAP 대중교통 HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"TMAP 대중교통 연결 실패: {exc.reason}") from exc


def itineraries(payload: dict) -> list[dict]:
    rows = (
        ((payload.get("metaData") or {}).get("plan") or {}).get("itineraries")
        or []
    )
    if not rows:
        raise ValueError("TMAP 대중교통 경로가 없습니다.")
    return list(rows)


def route_name(leg: dict) -> str:
    return str(leg.get("route") or leg.get("mode") or "")


def transit_route_names(itinerary: dict) -> list[str]:
    return [
        route_name(leg)
        for leg in itinerary.get("legs") or []
        if str(leg.get("mode", "")).upper() != "WALK"
    ]


def _route_matches(actual: str, wanted: str) -> bool:
    actual_tail = actual.split(":", 1)[-1].replace(" ", "").lower()
    wanted_tail = wanted.split(":", 1)[-1].replace(" ", "").lower()
    return wanted_tail == actual_tail or wanted_tail in actual_tail


def select_itinerary(
    payload: dict,
    prefer_routes: list[str] | None = None,
    itinerary_index: int | None = None,
) -> tuple[int, dict]:
    """인덱스 또는 노선 순서로 TMAP 후보를 고른다."""
    rows = itineraries(payload)
    if itinerary_index is not None:
        if not (0 <= itinerary_index < len(rows)):
            raise ValueError(f"TMAP itinerary_index 범위 초과: {itinerary_index}")
        return itinerary_index, rows[itinerary_index]
    wanted = [str(value) for value in (prefer_routes or [])]
    if wanted:
        for index, row in enumerate(rows):
            actual = transit_route_names(row)
            if len(actual) != len(wanted):
                continue
            if all(_route_matches(a, w) for a, w in zip(actual, wanted)):
                return index, row
        raise ValueError(
            "TMAP 후보에 요청 경로가 없습니다: "
            f"{wanted}; 후보={[transit_route_names(row) for row in rows]}"
        )
    return 0, rows[0]


def point_from_leg(leg: dict, key: str) -> dict:
    point = leg.get(key) or {}
    return {
        "lon": float(point["lon"]),
        "lat": float(point["lat"]),
        "name": str(point.get("name") or key),
    }


def wait_for_leg(wait_config: dict, leg_index: int, leg: dict) -> float | None:
    """leg index, routeId, route 이름 순으로 승차 대기 입력을 찾는다."""
    keys = (
        str(leg_index),
        str(leg.get("routeId") or ""),
        route_name(leg),
        route_name(leg).split(":", 1)[-1],
    )
    for key in keys:
        if key and key in wait_config:
            value = float(wait_config[key])
            if value < 0:
                raise ValueError(f"음수 대중교통 대기시간: {key}={value}")
            return value
    return None


def evaluate_itinerary(
    itinerary: dict,
    walk_start: datetime,
    speed_mps: float,
    tmap_key: str,
    intersections: list[dict],
    signal_rows: list[dict],
    signal_fetched_at: datetime,
    crosswalks: dict | None = None,
    transit_waits_s: dict | None = None,
    pedestrian_fetcher=fetch_pedestrian,
) -> dict:
    """대중교통·도보·속도·신호를 순서대로 연결해 총 시간을 계산한다."""
    if speed_mps <= 0:
        raise ValueError("speed_mps must be positive")
    static_crosswalks = crosswalks or {}
    wait_config = transit_waits_s or {}
    current = walk_start
    output_legs = []
    totals = {
        "walk_s": 0.0,
        "signal_s": 0.0,
        "transit_s": 0.0,
        "transit_wait_s": 0.0,
    }
    complete = True
    missing_wait_legs = []
    walk_index = 0

    for leg_index, leg in enumerate(itinerary.get("legs") or []):
        mode = str(leg.get("mode") or "").upper()
        if mode == "WALK":
            provider_distance = float(leg.get("distance") or 0)
            start = point_from_leg(leg, "start")
            end = point_from_leg(leg, "end")
            if (
                provider_distance <= 0
                and start["lon"] == end["lon"]
                and start["lat"] == end["lat"]
            ):
                output_legs.append(
                    {
                        "index": leg_index,
                        "mode": "WALK",
                        "provider_distance_m": provider_distance,
                        "walk_s": 0.0,
                        "signal_s": 0.0,
                        "total_s": 0.0,
                        "crossings": 0,
                        "signal_method": "none",
                    }
                )
                walk_index += 1
                continue
            config = {"start": start, "end": end}
            payload = pedestrian_fetcher(config, tmap_key)
            segments = extract_segments(payload)
            walk_s = personal_walk_seconds(segments, speed_mps)
            crossings = extract_crosswalks(payload, speed_mps)
            signal = estimate_route_wait(
                crossings,
                static_crosswalks,
                current,
                walk_index == 0,
                speed_mps,
                intersections,
                signal_rows,
                signal_fetched_at,
            )
            signal_s = float(signal["wait_s"])
            duration = walk_s + signal_s
            totals["walk_s"] += walk_s
            totals["signal_s"] += signal_s
            current += timedelta(seconds=duration)
            output_legs.append(
                {
                    "index": leg_index,
                    "mode": "WALK",
                    "start": start["name"],
                    "end": end["name"],
                    "provider_distance_m": provider_distance,
                    "tmap_distance_m": sum(float(row["distance_m"]) for row in segments),
                    "walk_s": walk_s,
                    "signal_s": signal_s,
                    "total_s": duration,
                    "crossings": len(crossings),
                    "signal_method": (
                        "road-upper-fallback"
                        if signal.get("used_fallback")
                        else ",".join(
                            row.get("method", "") for row in signal.get("details", [])
                        )
                        or "none"
                    ),
                    "signal_details": signal.get("details", []),
                }
            )
            walk_index += 1
            continue

        if mode not in TRANSIT_MODES:
            raise ValueError(f"지원하지 않는 TMAP leg mode: {mode}")
        section_s = float(leg.get("sectionTime") or 0)
        wait_s = wait_for_leg(wait_config, leg_index, leg)
        if wait_s is None:
            complete = False
            missing_wait_legs.append(leg_index)
            applied_wait_s = 0.0
        else:
            applied_wait_s = wait_s
        duration = section_s + applied_wait_s
        totals["transit_s"] += section_s
        totals["transit_wait_s"] += applied_wait_s
        current += timedelta(seconds=duration)
        output_legs.append(
            {
                "index": leg_index,
                "mode": mode,
                "route": route_name(leg),
                "route_id": leg.get("routeId"),
                "section_s": section_s,
                "wait_s": wait_s,
                "total_s": duration if wait_s is not None else None,
            }
        )

    total_s = sum(totals.values())
    return {
        "complete": complete,
        "missing_wait_legs": missing_wait_legs,
        **totals,
        "total_s": total_s,
        "arrival": current,
        "legs": output_legs,
    }


def itinerary_summary(index: int, itinerary: dict) -> dict:
    return {
        "index": index,
        "provider_total_s": float(itinerary.get("totalTime") or 0),
        "provider_walk_s": float(itinerary.get("totalWalkTime") or 0),
        "routes": transit_route_names(itinerary),
        "walk_legs": sum(
            str(leg.get("mode") or "").upper() == "WALK"
            for leg in itinerary.get("legs") or []
        ),
    }
