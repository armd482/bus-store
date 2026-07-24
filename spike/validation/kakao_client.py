#!/usr/bin/env python3
"""카카오 장소검색과 도보 경로를 검증 이벤트 입력으로 변환한다."""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
ENV_FILE = os.path.join(PROJECT_ROOT, "collector", ".env")
SEARCH_URL = "https://dapi.kakao.com/v2/local/search/keyword.json"
WALK_URL = "https://dapi.kakao.com/v2/routing/walk"
PUBLIC_TRAFFIC_URL = "https://dapi.kakao.com/v2/routing/publictraffic"


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
    key = os.environ.get("KAKAO_REST_API_KEY") or _dotenv_value(
        ENV_FILE, "KAKAO_REST_API_KEY"
    )
    if not key:
        raise RuntimeError(
            "KAKAO_REST_API_KEY가 없습니다. collector/.env에 추가하세요."
        )
    return key


def _get_json(url: str, params: dict, key: str, timeout_s: int = 15) -> dict:
    query = urllib.parse.urlencode(params)
    request = urllib.request.Request(
        f"{url}?{query}",
        headers={"Authorization": f"KakaoAK {key}"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_s) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:300]
        raise RuntimeError(f"카카오 HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"카카오 연결 실패: {exc.reason}") from exc


def search_place(
    query: str,
    key: str,
    near: dict | None = None,
    result_index: int = 0,
) -> dict:
    params = {"query": query, "size": 15}
    if near:
        params.update(
            {
                "x": float(near["lon"]),
                "y": float(near["lat"]),
                "sort": "distance",
            }
        )
        if near.get("radius_m"):
            params["radius"] = int(near["radius_m"])
    payload = _get_json(SEARCH_URL, params, key)
    documents = payload.get("documents") or []
    if not documents:
        raise ValueError(f"카카오 장소 검색 결과 없음: {query}")
    if not (0 <= result_index < len(documents)):
        raise ValueError(
            f"카카오 검색 result_index 범위 초과: {result_index}/{len(documents)}"
        )
    place = documents[result_index]
    return {
        "lon": float(place["x"]),
        "lat": float(place["y"]),
        "name": place.get("place_name") or query,
        "place_id": place.get("id"),
        "address": place.get("road_address_name") or place.get("address_name"),
    }


def resolve_point(config: dict, key: str) -> dict:
    if "lon" in config and "lat" in config:
        return {
            "lon": float(config["lon"]),
            "lat": float(config["lat"]),
            "name": str(config.get("name", "좌표")),
            "place_id": config.get("place_id"),
            "address": config.get("address"),
        }
    query = config.get("query")
    if not query:
        raise ValueError("카카오 지점에는 query 또는 lon/lat가 필요합니다.")
    return search_place(
        str(query),
        key,
        config.get("near"),
        int(config.get("result_index", 0)),
    )


def fetch_walk(
    start: dict,
    end: dict,
    key: str,
    route_mode: str = "SHORTEST",
) -> dict:
    return _get_json(
        WALK_URL,
        {
            "start_x": start["lon"],
            "start_y": start["lat"],
            "end_x": end["lon"],
            "end_y": end["lat"],
            "s_name": start["name"],
            "e_name": end["name"],
            "input_coord": "WGS84",
            "output_coord": "WGS84",
            "route_mode": route_mode,
        },
        key,
    )


def fetch_public_traffic(start: dict, end: dict, key: str) -> dict:
    return _get_json(
        PUBLIC_TRAFFIC_URL,
        {
            "start_x": start["lon"],
            "start_y": start["lat"],
            "end_x": end["lon"],
            "end_y": end["lat"],
            "input_coord": "WGS84",
            "output_coord": "WGS84",
        },
        key,
    )


def _route_vehicle_names(route: dict) -> list[str]:
    return [
        str(vehicle.get("name"))
        for step in route.get("steps") or []
        for vehicle in (step.get("properties") or {}).get("vehicles") or []
        if vehicle.get("name")
    ]


def _initial_stop_name(route: dict) -> str | None:
    for step in route.get("steps") or []:
        properties = step.get("properties") or {}
        if properties.get("type") == "WALKING":
            continue
        stops = properties.get("stops") or []
        return str(stops[0].get("name")) if stops and stops[0].get("name") else None
    return None


def select_public_route(
    payload: dict,
    prefer_vehicle: str | None = None,
    prefer_boarding_stop: str | None = None,
) -> tuple[int, dict]:
    if payload.get("status") != "OK":
        raise ValueError(f"카카오 대중교통 경로 실패: {payload.get('status')}")
    routes = payload.get("routes") or []
    if not routes:
        raise ValueError("카카오 대중교통 경로가 없습니다.")
    if prefer_vehicle or prefer_boarding_stop:
        for index, route in enumerate(routes):
            vehicle_ok = (
                not prefer_vehicle or prefer_vehicle in _route_vehicle_names(route)
            )
            stop_name = _initial_stop_name(route) or ""
            stop_ok = not prefer_boarding_stop or prefer_boarding_stop in stop_name
            if vehicle_ok and stop_ok:
                return index, route
        conditions = [
            value
            for value in (
                f"노선={prefer_vehicle}" if prefer_vehicle else None,
                f"승차점={prefer_boarding_stop}" if prefer_boarding_stop else None,
            )
            if value
        ]
        raise ValueError(
            f"카카오 후보에 관측 경로({', '.join(conditions)})가 없습니다."
        )
    return 0, routes[0]


def initial_boarding_point(route: dict) -> dict:
    """첫 대중교통 step의 path 시작점을 최초 승차 위치로 사용한다."""
    for step in route.get("steps") or []:
        properties = step.get("properties") or {}
        if properties.get("type") == "WALKING":
            continue
        points = (step.get("path") or {}).get("points") or []
        if not points:
            continue
        stops = properties.get("stops") or []
        name = stops[0].get("name") if stops else "최초 승차점"
        return {
            "lon": float(points[0][0]),
            "lat": float(points[0][1]),
            "name": name,
            "place_id": None,
            "address": None,
        }
    raise ValueError("카카오 경로에서 최초 승차점 좌표를 찾지 못했습니다.")


def extract_walk(payload: dict) -> dict:
    if payload.get("status") != "OK" or not payload.get("route"):
        raise ValueError(f"카카오 도보 경로 실패: {payload.get('status')}")
    route = payload["route"]
    properties = route.get("properties") or {}
    points = []
    steps = 0
    for leg in route.get("legs") or []:
        for step in leg.get("steps") or []:
            steps += 1
            for point in (step.get("path") or {}).get("points") or []:
                normalized = [float(point[0]), float(point[1])]
                if not points or normalized != points[-1]:
                    points.append(normalized)
    distance_m = float(properties["totalDistance"])
    if distance_m <= 0:
        raise ValueError("카카오 도보 거리가 0 이하입니다.")
    return {
        "distance_m": distance_m,
        "provider_time_s": float(properties["totalTime"]),
        "steps": steps,
        "points": points,
    }


def event_walk(sc: dict, key: str | None = None) -> tuple[float, dict, dict]:
    """장소명을 좌표화하고 개인 도보시간 및 TMAP 재조회용 좌표를 반환한다."""
    config = sc["kakao"]
    api_key = key or load_key()
    start = resolve_point(config["start"], api_key)
    route_index = None
    route_vehicles = None
    if config.get("end"):
        end = resolve_point(config["end"], api_key)
    elif config.get("trip_end"):
        trip_end = resolve_point(config["trip_end"], api_key)
        route_index, route = select_public_route(
            fetch_public_traffic(start, trip_end, api_key),
            config.get("prefer_vehicle"),
            config.get("prefer_boarding_stop"),
        )
        end = initial_boarding_point(route)
        route_vehicles = _route_vehicle_names(route)
    else:
        raise ValueError("kakao에는 end 또는 trip_end가 필요합니다.")
    walk = extract_walk(
        fetch_walk(start, end, api_key, config.get("route_mode", "SHORTEST"))
    )
    speed_mps = float(sc.get("profile", {}).get("speed_mps", 1.66))
    if speed_mps <= 0:
        raise ValueError("speed_mps must be positive")
    personal_s = walk["distance_m"] / speed_mps
    summary = {
        "start": start,
        "end": end,
        "public_route_index": route_index,
        "public_route_vehicles": route_vehicles,
        **walk,
    }
    tmap_config = {
        "start": {"lon": start["lon"], "lat": start["lat"], "name": start["name"]},
        "end": {"lon": end["lon"], "lat": end["lat"], "name": end["name"]},
    }
    if config.get("facility_speed_multipliers"):
        tmap_config["facility_speed_multipliers"] = config[
            "facility_speed_multipliers"
        ]
    return personal_s, summary, tmap_config
