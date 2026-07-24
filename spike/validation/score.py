#!/usr/bin/env python3
"""개인화 길찾기 해결책 평가기.

같은 연결 이벤트에서 아래를 실제 관측과 비교한다.

  Naver          네이버 표시 도보시간 + 네이버 선택 연결
  Speed          네이버 도보시간 × 개인 시간비율
  Speed+Signal   Speed + 횡단보도별 기대 신호대기
  Oracle         Speed + 실제 신호대기 (예측이 아닌 개선 상한)
  TMAP-Personal  TMAP 시설별 거리 / 개인 물리속도
  TMAP+Signal    TMAP-Personal + 횡단보도별 기대 신호대기
  Kakao-Personal 카카오 도보거리 / 개인 물리속도

통과 판정은 split=test, source=real 표본만 사용한다. demo/calibration 자료는
모델 개발과 입력 확인에는 쓸 수 있지만 검증 통과 근거에는 포함하지 않는다.
"""
from __future__ import annotations

import argparse
import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from statistics import mean, median

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_RATIO = 0.68
DEFAULT_SPEED_MPS = 1.66
DEFAULT_BUFFER_S = 60
MIN_TEST_EVENTS = 3
BASE_MODELS = ["Naver", "Speed", "Speed+Signal", "Oracle"]
TMAP_MODELS = ["TMAP-Personal", "TMAP+Signal", "TMAP-Oracle"]
KAKAO_MODELS = ["Kakao-Personal", "Kakao+Signal", "Kakao-Oracle"]


@dataclass
class Prediction:
    model: str
    arrival: datetime
    connection: str | None
    arrival_error_s: float
    connection_ok: bool
    dangerous: bool | None


def parse_dt(value: str) -> datetime:
    """초 단위 ISO-8601 시각. 날짜가 있으므로 자정 연결도 안전하다."""
    return datetime.fromisoformat(value)


def fmt_dt(value: datetime) -> str:
    return value.strftime("%m-%d %H:%M:%S")


def expected_wait(green_s: float, cycle_s: float, dist_m: float, speed_mps: float) -> float:
    """균일 위상에서 속도 인지 보행신호 기대대기.

    유효 녹색 = green - dist/speed 이므로
    E[wait] = (red + dist/speed)^2 / (2*cycle).
    """
    if not (0 < green_s < cycle_s):
        raise ValueError(f"green_s must be between 0 and cycle_s: {green_s}/{cycle_s}")
    if dist_m <= 0 or speed_mps <= 0:
        raise ValueError("dist_m and speed_mps must be positive")
    crossing_s = dist_m / speed_mps
    if crossing_s > green_s:
        raise ValueError(
            f"crossing cannot finish in green: crossing={crossing_s:.1f}s green={green_s:.1f}s"
        )
    red_s = cycle_s - green_s
    return (red_s + crossing_s) ** 2 / (2 * cycle_s)


def worst_wait(crosswalk: dict, speed_mps: float) -> float:
    """유효 녹색이 막 닫힌 직후 도착했을 때 다음 녹색까지의 최대 대기."""
    crossing_s = crosswalk["dist_m"] / speed_mps
    return crosswalk["cycle_s"] - crosswalk["green_s"] + crossing_s


def profile(sc: dict, ratio_override: float | None = None) -> tuple[float, float]:
    p = sc.get("profile", {})
    ratio = ratio_override if ratio_override is not None else p.get(
        "walk_time_ratio", DEFAULT_RATIO
    )
    speed_mps = p.get("speed_mps", DEFAULT_SPEED_MPS)
    if not (0 < ratio <= 2):
        raise ValueError(f"{sc['id']}: invalid walk_time_ratio {ratio}")
    if speed_mps <= 0:
        raise ValueError(f"{sc['id']}: invalid speed_mps {speed_mps}")
    return ratio, speed_mps


def crossing_wait(sc: dict, crosswalks: dict, mode: str, speed_mps: float) -> float:
    ids = sc.get("crossings", [])
    if mode == "best":
        return 0.0
    if mode == "oracle":
        return float(sc["actual"].get("signal_wait_s", 0))

    total = 0.0
    for cwid in ids:
        if cwid not in crosswalks:
            raise KeyError(f"{sc['id']}: unknown crosswalk {cwid}")
        c = crosswalks[cwid]
        if mode == "expected":
            total += expected_wait(c["green_s"], c["cycle_s"], c["dist_m"], speed_mps)
        elif mode == "worst":
            total += worst_wait(c, speed_mps)
        else:
            raise ValueError(mode)
    return total


def predicted_bus_times(sc: dict) -> list[tuple[str, datetime]]:
    snap = sc["bus_snapshot"]
    captured = parse_dt(snap["captured_at"])
    rows = [
        (c["id"], captured + timedelta(seconds=float(c["eta_s"])))
        for c in snap["candidates"]
    ]
    return sorted(rows, key=lambda row: row[1])


def actual_bus_times(sc: dict) -> list[tuple[str, datetime]]:
    rows = [
        (c["id"], parse_dt(c["departed_at"]))
        for c in sc["actual"]["connection_departures"]
    ]
    return sorted(rows, key=lambda row: row[1])


def choose_connection(
    candidates: list[tuple[str, datetime]], arrival: datetime, buffer_s: int
) -> str | None:
    ready = arrival + timedelta(seconds=buffer_s)
    for connection_id, departure in candidates:
        if departure >= ready:
            return connection_id
    return None


def actual_connection(sc: dict, buffer_s: int) -> str | None:
    """실제 정류장 도착시각으로 물리적으로 잡을 수 있었던 가장 이른 후보."""
    arrival = parse_dt(sc["actual"]["stop_arrival"])
    return choose_connection(actual_bus_times(sc), arrival, buffer_s)


def predict(
    sc: dict,
    crosswalks: dict,
    model: str,
    buffer_s: int,
    ratio_override: float | None = None,
    tmap_walk_s: float | None = None,
    kakao_walk_s: float | None = None,
    signal_wait_s: float | None = None,
) -> Prediction:
    walk_start = parse_dt(sc["walk_start"])
    naver_walk_s = float(sc["naver"]["walk_time_s"])
    ratio, speed_mps = profile(sc, ratio_override)

    if model == "Naver":
        walk_s = naver_walk_s
    elif model == "Speed":
        walk_s = naver_walk_s * ratio
    elif model == "Speed+Signal":
        wait_s = (
            signal_wait_s
            if signal_wait_s is not None
            else crossing_wait(sc, crosswalks, "expected", speed_mps)
        )
        walk_s = naver_walk_s * ratio + wait_s
    elif model == "Best":
        walk_s = naver_walk_s * ratio
    elif model == "Worst":
        walk_s = naver_walk_s * ratio + crossing_wait(
            sc, crosswalks, "worst", speed_mps
        )
    elif model == "Oracle":
        walk_s = naver_walk_s * ratio + crossing_wait(
            sc, crosswalks, "oracle", speed_mps
        )
    elif model == "TMAP-Personal":
        if tmap_walk_s is None:
            raise ValueError(f"{sc['id']}: TMAP 보행시간이 없습니다.")
        walk_s = tmap_walk_s
    elif model == "TMAP+Signal":
        if tmap_walk_s is None:
            raise ValueError(f"{sc['id']}: TMAP 보행시간이 없습니다.")
        wait_s = (
            signal_wait_s
            if signal_wait_s is not None
            else crossing_wait(sc, crosswalks, "expected", speed_mps)
        )
        walk_s = tmap_walk_s + wait_s
    elif model == "TMAP-Oracle":
        if tmap_walk_s is None:
            raise ValueError(f"{sc['id']}: TMAP 보행시간이 없습니다.")
        walk_s = tmap_walk_s + crossing_wait(
            sc, crosswalks, "oracle", speed_mps
        )
    elif model == "Kakao-Personal":
        if kakao_walk_s is None:
            raise ValueError(f"{sc['id']}: 카카오 보행시간이 없습니다.")
        walk_s = kakao_walk_s
    elif model == "Kakao+Signal":
        if kakao_walk_s is None:
            raise ValueError(f"{sc['id']}: 카카오 보행시간이 없습니다.")
        wait_s = (
            signal_wait_s
            if signal_wait_s is not None
            else crossing_wait(sc, crosswalks, "expected", speed_mps)
        )
        walk_s = kakao_walk_s + wait_s
    elif model == "Kakao-Oracle":
        if kakao_walk_s is None:
            raise ValueError(f"{sc['id']}: 카카오 보행시간이 없습니다.")
        walk_s = kakao_walk_s + crossing_wait(
            sc, crosswalks, "oracle", speed_mps
        )
    else:
        raise ValueError(model)

    arrival = walk_start + timedelta(seconds=walk_s)
    if model == "Naver" and sc["naver"].get("selected_connection"):
        connection = sc["naver"]["selected_connection"]
    else:
        connection = choose_connection(predicted_bus_times(sc), arrival, buffer_s)

    actual_arrival = parse_dt(sc["actual"]["stop_arrival"])
    truth = actual_connection(sc, buffer_s)
    actual_by_id = dict(actual_bus_times(sc))
    dangerous = None
    if connection in actual_by_id:
        dangerous = actual_by_id[connection] < actual_arrival + timedelta(seconds=buffer_s)

    return Prediction(
        model=model,
        arrival=arrival,
        connection=connection,
        arrival_error_s=(arrival - actual_arrival).total_seconds(),
        connection_ok=connection == truth,
        dangerous=dangerous,
    )


def eligible(sc: dict, selected_split: str) -> bool:
    if sc.get("source") == "demo":
        return False
    if not sc.get("valid", True):
        return False
    return selected_split == "all" or sc.get("split", "test") == selected_split


def evaluate(
    scenarios: list[dict],
    crosswalks: dict,
    selected_split: str = "all",
    buffer_s: int = DEFAULT_BUFFER_S,
    ratio_override: float | None = None,
    tmap_walk_times: dict[str, float] | None = None,
    kakao_walk_times: dict[str, float] | None = None,
    signal_estimates: dict[str, float | None] | None = None,
) -> tuple[list[dict], dict]:
    tmap_walk_times = tmap_walk_times or {}
    kakao_walk_times = kakao_walk_times or {}
    models = list(BASE_MODELS)
    if tmap_walk_times:
        models += TMAP_MODELS
    if kakao_walk_times:
        models += KAKAO_MODELS
    rows = []
    for sc in scenarios:
        if not eligible(sc, selected_split):
            continue
        strict_signal = signal_estimates is not None
        signal_wait_s = (
            signal_estimates.get(sc["id"]) if strict_signal else None
        )
        signal_available = not strict_signal or signal_wait_s is not None
        event_models = ["Naver", "Speed", "Oracle"]
        if signal_available:
            event_models.insert(2, "Speed+Signal")
        tmap_walk_s = tmap_walk_times.get(sc["id"])
        kakao_walk_s = kakao_walk_times.get(sc["id"])
        if tmap_walk_s is not None:
            event_models += ["TMAP-Personal", "TMAP-Oracle"]
            if signal_available:
                event_models.insert(
                    event_models.index("TMAP-Oracle"), "TMAP+Signal"
                )
        if kakao_walk_s is not None:
            event_models += ["Kakao-Personal", "Kakao-Oracle"]
            if signal_available:
                event_models.insert(
                    event_models.index("Kakao-Oracle"), "Kakao+Signal"
                )
        preds = {
            name: predict(
                sc,
                crosswalks,
                name,
                buffer_s,
                ratio_override,
                tmap_walk_s,
                kakao_walk_s,
                signal_wait_s,
            )
            for name in event_models
        }
        rows.append(
            {
                "id": sc["id"],
                "split": sc.get("split", "test"),
                "truth": actual_connection(sc, buffer_s),
                "boarded": sc["actual"].get("boarded"),
                "predictions": preds,
            }
        )

    metrics = {}
    for model in models:
        ps = [
            row["predictions"][model]
            for row in rows
            if model in row["predictions"]
        ]
        if not ps:
            metrics[model] = {
                "n": 0,
                "mae_s": math.nan,
                "median_ae_s": math.nan,
                "connection_accuracy": math.nan,
                "dangerous": 0,
            }
            continue
        abs_errors = [abs(p.arrival_error_s) for p in ps]
        metrics[model] = {
            "n": len(ps),
            "mae_s": mean(abs_errors),
            "median_ae_s": median(abs_errors),
            "connection_accuracy": sum(p.connection_ok for p in ps) / len(ps),
            "dangerous": sum(p.dangerous is True for p in ps),
        }
    return rows, metrics


def verdict(
    scenarios: list[dict],
    crosswalks: dict,
    buffer_s: int,
    ratio_override: float | None,
    tmap_walk_times: dict[str, float] | None = None,
    kakao_walk_times: dict[str, float] | None = None,
    signal_estimates: dict[str, float | None] | None = None,
) -> dict:
    rows, metrics = evaluate(
        scenarios,
        crosswalks,
        "test",
        buffer_s,
        ratio_override,
        tmap_walk_times,
        kakao_walk_times,
        signal_estimates,
    )
    n = len(rows)
    use_tmap = n > 0 and all(
        "TMAP+Signal" in row["predictions"] for row in rows
    )
    use_kakao = n > 0 and all(
        "Kakao+Signal" in row["predictions"] for row in rows
    )
    use_speed = n > 0 and all(
        "Speed+Signal" in row["predictions"] for row in rows
    )
    if signal_estimates is not None and not (use_tmap or use_kakao or use_speed):
        return {
            "status": "insufficient",
            "n": n,
            "uplift": 0,
            "product_model": "Signal unavailable",
            "reason": "일부 표본에서 실시간 위상도 주기 기반 기대대기도 계산할 수 없음",
        }
    product_model = (
        "TMAP+Signal"
        if use_tmap
        else "Kakao+Signal"
        if use_kakao
        else "Speed+Signal"
        if use_speed
        else "Speed+Signal"
    )
    uplift = sum(
        (not row["predictions"]["Naver"].connection_ok)
        and row["predictions"][product_model].connection_ok
        for row in rows
    )
    if n < MIN_TEST_EVENTS:
        return {
            "status": "insufficient",
            "n": n,
            "uplift": uplift,
            "product_model": product_model,
            "reason": f"새 test 표본 {MIN_TEST_EVENTS}건 필요",
        }

    naver = metrics["Naver"]
    product = metrics[product_model]
    mae_improvement = (
        1 - product["mae_s"] / naver["mae_s"]
        if naver["mae_s"] > 0
        else float("-inf")
    )
    passed = (
        mae_improvement >= 0.30
        and product["connection_accuracy"] >= 2 / 3
        and product["dangerous"] == 0
        and uplift >= 1
    )
    return {
        "status": "pass" if passed else "fail",
        "n": n,
        "uplift": uplift,
        "product_model": product_model,
        "mae_improvement": mae_improvement,
        "connection_accuracy": product["connection_accuracy"],
        "dangerous": product["dangerous"],
    }


def print_report(rows: list[dict], metrics: dict, result: dict, buffer_s: int) -> None:
    print(f"\n=== 개인화 연결 평가 (안전버퍼 {buffer_s}초) ===")
    if not rows:
        print("선택한 split에 유효한 표본이 없습니다.")
    for row in rows:
        print(f"\n[{row['id']}] split={row['split']}  실제 earliest={row['truth']} "
              f"실제탑승={row['boarded']}")
        print(f'{"모델":<16}{"정류장 도착":<20}{"오차":>9}  {"선택 연결":<18}판정')
        for model, p in row["predictions"].items():
            danger = " 위험" if p.dangerous else ""
            print(
                f"{model:<16}{fmt_dt(p.arrival):<20}{p.arrival_error_s:>+8.1f}s  "
                f"{str(p.connection):<18}{'적중' if p.connection_ok else '오답'}{danger}"
            )

    print("\n--- 선택 표본 요약 ---")
    for model, m in metrics.items():
        if not m["n"]:
            continue
        print(
            f"{model:<16} n={m['n']}  MAE={m['mae_s']:.1f}s  "
            f"중앙AE={m['median_ae_s']:.1f}s  연결정확도={m['connection_accuracy']:.0%}  "
            f"위험오답={m['dangerous']}"
        )

    print("\n--- 검증 판정 (source=real, split=test만) ---")
    if result["status"] == "insufficient":
        print(
            f"판정 보류: test {result['n']}건 — {result['reason']} "
            f"(제품모델={result['product_model']})"
        )
    else:
        print(
            f"{result['status'].upper()} [{result['product_model']}]: "
            f"MAE 개선 {result['mae_improvement']:.0%}, "
            f"연결정확도 {result['connection_accuracy']:.0%}, "
            f"위험오답 {result['dangerous']}, 순증분 {result['uplift']}건"
        )


def sweep_flip(crosswalks: dict, slow: float = 1.0, fast: float = 1.66) -> None:
    print(f"\n=== 속도별 유효 녹색 뒤집힘 ({slow} vs {fast}m/s) ===")
    found = False
    for cwid, c in crosswalks.items():
        if cwid.startswith("_") or c.get("source") == "demo":
            continue
        found = True
        slow_green = max(0.0, c["green_s"] - c["dist_m"] / slow)
        fast_green = max(0.0, c["green_s"] - c["dist_m"] / fast)
        flip = max(0.0, fast_green - slow_green) / c["cycle_s"]
        print(
            f"{cwid}: 유효창 {slow_green:.1f}s→{fast_green:.1f}s, "
            f"전체 위상의 {flip:.1%}에서 판정 뒤집힘"
        )
    if not found:
        print("실측 횡단보도 데이터가 없습니다.")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--split", choices=["all", "calibration", "test"], default="all")
    ap.add_argument("--buffer", type=int, default=DEFAULT_BUFFER_S)
    ap.add_argument("--ratio", type=float, help="모든 표본의 개인 시간비율 임시 덮어쓰기")
    ap.add_argument("--sweep", action="store_true")
    ap.add_argument(
        "--tmap",
        action="store_true",
        help="tmap.start/end 좌표가 있는 표본을 TMAP API로 조회",
    )
    ap.add_argument(
        "--kakao",
        action="store_true",
        help="kakao.start/end 장소를 좌표화하고 도보 경로 조회",
    )
    ap.add_argument(
        "--hybrid",
        action="store_true",
        help="카카오로 장소·도보 경로를 얻고 같은 좌표를 TMAP으로 보강",
    )
    ap.add_argument(
        "--signals",
        action="store_true",
        help="TMAP 횡단보도에 실시간 신호를 매칭하고 불가하면 기대대기 사용",
    )
    args = ap.parse_args()
    if args.signals:
        args.tmap = True

    with open(os.path.join(HERE, "ground_truth.json"), encoding="utf-8") as f:
        scenarios = json.load(f)
    with open(os.path.join(HERE, "crosswalks.json"), encoding="utf-8") as f:
        crosswalks = json.load(f)

    kakao_walk_times = {}
    resolved_tmap_configs = {}
    if args.kakao or args.hybrid:
        from kakao_client import event_walk, load_key as load_kakao_key

        configured = [sc for sc in scenarios if sc.get("kakao")]
        if configured:
            key = load_kakao_key()
            for sc in configured:
                seconds, summary, tmap_config = event_walk(sc, key)
                kakao_walk_times[sc["id"]] = seconds
                resolved_tmap_configs[sc["id"]] = tmap_config
                print(
                    f"Kakao {sc['id']}: {summary['start']['name']} → "
                    f"{summary['end']['name']}, {summary['distance_m']:.0f}m, "
                    f"{summary['steps']}단계/{len(summary['points'])}점 → "
                    f"{seconds:.1f}초"
                )
        else:
            print("카카오 장소가 입력된 표본이 없어 API를 호출하지 않았습니다.")

    tmap_walk_times = {}
    tmap_summaries = {}
    if args.tmap or args.hybrid:
        from tmap_client import event_walk_seconds, load_key

        configured = [
            sc
            for sc in scenarios
            if sc.get("tmap") or sc["id"] in resolved_tmap_configs
        ]
        if configured:
            key = load_key()
            for sc in configured:
                tmap_sc = dict(sc)
                if sc["id"] in resolved_tmap_configs:
                    tmap_sc["tmap"] = resolved_tmap_configs[sc["id"]]
                seconds, summary = event_walk_seconds(tmap_sc, key)
                tmap_walk_times[sc["id"]] = seconds
                tmap_summaries[sc["id"]] = summary
                print(
                    f"TMAP {sc['id']}: {summary['distance_m']:.0f}m, "
                    f"{summary['segments']}개 구간, 횡단보도 "
                    f"{summary['crosswalk_segments']}개 → {seconds:.1f}초"
                )
        else:
            print("TMAP 좌표가 입력된 표본이 없어 API를 호출하지 않았습니다.")

    signal_estimates = None
    if args.signals:
        from signal_client import (
            estimate_route_wait,
            fetch_all,
            load_key as load_signal_key,
        )

        signal_estimates = {}
        if not tmap_summaries:
            print("신호 평가에는 TMAP 횡단보도 경로가 필요합니다.")
        else:
            signal_key = load_signal_key()
            intersections = fetch_all("crsrd_map_info", signal_key)
            signal_rows = fetch_all("tl_drct_info", signal_key)
            fetched_at = datetime.now()
            for sc in scenarios:
                summary = tmap_summaries.get(sc["id"])
                if summary is None:
                    signal_estimates[sc["id"]] = None
                    continue
                _, speed_mps = profile(sc, args.ratio)
                result = estimate_route_wait(
                    summary["crosswalks"],
                    crosswalks,
                    parse_dt(sc["walk_start"]),
                    bool((sc.get("signal") or {}).get("is_first_walk", False)),
                    speed_mps,
                    intersections,
                    signal_rows,
                    fetched_at,
                )
                signal_estimates[sc["id"]] = result["wait_s"]
                methods = ",".join(
                    detail["method"] for detail in result["details"]
                ) or "no-crossing"
                wait_text = (
                    f"{result['wait_s']:.1f}초"
                    if result["wait_s"] is not None
                    else "계산 불가"
                )
                print(
                    f"Signal {sc['id']}: {result['crossings']}개 "
                    f"[{methods}] → {wait_text}"
                )

    rows, metrics = evaluate(
        scenarios,
        crosswalks,
        args.split,
        args.buffer,
        args.ratio,
        tmap_walk_times,
        kakao_walk_times,
        signal_estimates,
    )
    result = verdict(
        scenarios,
        crosswalks,
        args.buffer,
        args.ratio,
        tmap_walk_times,
        kakao_walk_times,
        signal_estimates,
    )
    print_report(rows, metrics, result, args.buffer)
    if args.sweep:
        sweep_flip(crosswalks)


if __name__ == "__main__":
    main()
