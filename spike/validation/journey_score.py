#!/usr/bin/env python3
"""대중교통 + 전 도보 leg + 개인속도 + 보행신호 전체 여정 Validator."""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime

from kakao_client import load_key as load_kakao_key
from kakao_client import resolve_point
from signal_client import fetch_all
from tmap_client import load_key as load_tmap_key
from tmap_journey_client import (
    evaluate_itinerary,
    fetch_transit,
    itinerary_summary,
    itineraries,
    select_itinerary,
)

HERE = os.path.dirname(os.path.abspath(__file__))


def resolve_endpoint(config: dict, kakao_key: str | None) -> dict:
    # 정류장 참조(stop{city_code,ars})면 실측 정류장 좌표로 해석한다 —
    # TMAP/장소 검색이 건물 좌표를 잡아 도보가 어긋나는 것을 막는다.
    if "stop" in config and not ("lon" in config and "lat" in config):
        from stop_client import resolve_stop
        return resolve_stop(config)
    if "lon" in config and "lat" in config:
        return resolve_point(config, kakao_key or "")
    if not kakao_key:
        raise RuntimeError("query 지점을 좌표화하려면 KAKAO_REST_API_KEY가 필요합니다.")
    return resolve_point(config, kakao_key)


def load_case(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        case = json.load(f)
    for field in ("start", "end", "walk_start"):
        if field not in case:
            raise ValueError(f"전체 여정 입력에 {field}가 없습니다.")
    return case


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="전체 여정 JSON")
    parser.add_argument("--list-routes", action="store_true")
    parser.add_argument("--itinerary-index", type=int)
    parser.add_argument("--no-signals", action="store_true")
    args = parser.parse_args()

    case = load_case(args.input)
    tmap_key = load_tmap_key()
    needs_kakao = any("query" in case[name] for name in ("start", "end"))
    kakao_key = load_kakao_key() if needs_kakao else None
    start = resolve_endpoint(case["start"], kakao_key)
    end = resolve_endpoint(case["end"], kakao_key)
    transit_config = {
        "start": start,
        "end": end,
        "search_dttm": case.get("search_dttm"),
        "count": case.get("count", 10),
    }
    payload = fetch_transit(transit_config, tmap_key)

    if args.list_routes:
        for index, row in enumerate(itineraries(payload)):
            summary = itinerary_summary(index, row)
            print(
                f"[{index}] TMAP {summary['provider_total_s']:.0f}s "
                f"walk={summary['provider_walk_s']:.0f}s "
                f"routes={' → '.join(summary['routes'])}"
            )
        return

    try:
        index, selected = select_itinerary(
            payload,
            case.get("prefer_routes"),
            args.itinerary_index
            if args.itinerary_index is not None
            else case.get("itinerary_index"),
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    intersections = []
    signal_rows = []
    if not args.no_signals:
        try:
            intersections = fetch_all("crsrd_map_info")
            signal_rows = fetch_all("tl_drct_info")
        except RuntimeError as exc:
            print(f"신호 API 사용 불가: {exc}; TMAP 거리별 fallback 적용")
    fetched_at = datetime.now()
    result = evaluate_itinerary(
        selected,
        datetime.fromisoformat(case["walk_start"]),
        float((case.get("profile") or {}).get("speed_mps", 1.66)),
        tmap_key,
        intersections,
        signal_rows,
        fetched_at,
        case.get("crosswalks") or {},
        case.get("transit_waits_s") or {},
    )

    summary = itinerary_summary(index, selected)
    print(
        f"TMAP itinerary[{index}]: {' → '.join(summary['routes'])}\n"
        f"  대중교통 이동={result['transit_s']:.2f}s\n"
        f"  대중교통 대기={result['transit_wait_s']:.2f}s\n"
        f"  개인화 순수도보={result['walk_s']:.2f}s\n"
        f"  보행신호={result['signal_s']:.2f}s\n"
        f"  전체={result['total_s']:.2f}s, 도착={result['arrival'].isoformat()}\n"
        f"  complete={result['complete']}, "
        f"missing_wait_legs={result['missing_wait_legs']}"
    )
    for leg in result["legs"]:
        if leg["mode"] == "WALK":
            print(
                f"  leg[{leg['index']}] WALK {leg.get('start', '')}→"
                f"{leg.get('end', '')}: walk={leg['walk_s']:.2f}s "
                f"signal={leg['signal_s']:.2f}s crossings={leg['crossings']} "
                f"method={leg['signal_method']}"
            )
        else:
            print(
                f"  leg[{leg['index']}] {leg['mode']} {leg['route']}: "
                f"move={leg['section_s']:.2f}s wait={leg['wait_s']}"
            )

    actual_total_s = case.get("actual_total_s")
    comparable = bool(case.get("comparable_route", False))
    if actual_total_s is not None:
        if not comparable:
            print("  실제 여정과 물리 경로가 달라 오차를 계산하지 않음")
        elif not result["complete"]:
            print("  대중교통 대기 입력이 없어 실제 대비 오차를 계산하지 않음")
        else:
            error = result["total_s"] - float(actual_total_s)
            print(f"  실제 대비 오차={error:+.2f}s")


if __name__ == "__main__":
    main()
