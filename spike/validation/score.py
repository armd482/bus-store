#!/usr/bin/env python3
"""개인화 길찾기 해결책 평가기.

★ 판정 대상은 **'어느 모델을 써야 하나'가 아니라, 이런 식으로 도보 속도 + 보행신호
   대기를 반영한 접근이 기존 지도보다 나은가(적합성)**이다. 여러 추정 방법(보수·유도)이
   있고, 그 방법들로도 지도보다 개선되는가를 본다. 그래서 '제품'은 단일 모델이 아니라
   **추정기**로 둔다: 절벽·배차 모두 **유도(유효녹색 평균)를 기본**으로 쓰고, 절벽 안전은
   RoadUpper 과대예측이 아니라 **§6.6 안전마진(buffer)**으로 확보한다. 보수(RoadUpper)는
   실측 전부에서 오차가 더 커(과대예측) **유도를 못 만들 때만 최후수단**으로 쓴다.
   ⚠️ 유도+마진의 절벽 안전은 실측 cliff=true 표본에서 위험오답이 없어야 검증된다.
   표본이 없거나 위험오답이 있으면 verdict의 `safety_validated=false`로 표시하고,
   절벽 표본이 없는 pass는 연결(속도) 축 한정으로 읽는다.

검증 대상 가설: **도보 속도와 보행신호 대기를 반영하면, 기존 지도(네이버·카카오)
보다 더 정확한 연결 안내를 준다.** 여기서 '정확'은 도착시각 정밀도가 아니라
**안내한 연결이 실제와 맞는가**로 정의한다 — 지도가 명시한 것과 다른 버스를 타거나
(different_bus), 지도보다 더 대기하거나(more_wait), 그 버스를 놓치는(miss) 경우를
지도의 부정확으로 보고, 그때 우리 모델이 실제를 맞히는지를 잰다 (classify_map_outcome).

같은 연결 이벤트에서 아래를 실제 관측과 비교한다.

  Naver          네이버 표시 도보시간 + 네이버 선택 연결  ← 기존 지도(기준선)
  Speed          네이버 도보시간 × 개인 시간비율          ← 도보 속도만 반영
  Speed+Signal   Speed + 횡단보도별 예측 신호대기         ← 도보 속도 + 신호 (제품)
  TMAP-Personal  TMAP 시설별 거리 / 개인 물리속도
  TMAP+Signal    TMAP-Personal + 예측 신호대기
  Kakao-Personal 카카오 도보거리 / 개인 물리속도

  ※ '신호를 완벽히 알면 얼마나 정확해지나'(개선 천장)는 별도 모델로 두지 않는다 —
     그 값은 관측된 실제 신호대기(`actual.signal_wait_s`)를 Speed 도착에 더해
     한 줄로 읽는다. 예전엔 이를 `Oracle`/`+RealSignal` 모델로 뒀으나 '실측'이
     실시간 신호 API 로 오해돼 없앴다 (게이트엔 원래 안 들어갔다).

★ 통과 게이트는 **연결 안내 정확도 + 위험오답 0 + 지도 대비 순증분**이다.
  도착시각 MAE 는 진단값으로만 남긴다(게이트 아님) — 신호를 보수적으로 과대예측하면
  MAE 는 나빠지지만 연결 안내는 정확할 수 있고, 그 경우를 MAE 게이트가 잘못 탈락시켰다.

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
BASE_MODELS = ["Naver", "Speed", "Speed+Signal"]
TMAP_MODELS = ["TMAP-Personal", "TMAP+Signal"]
KAKAO_MODELS = ["Kakao-Personal", "Kakao+Signal"]


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
    """균일 위상에서 보행신호 기대대기 (설계 문서 §7.1·§7.4).

    E[wait] = (cycle − effective_green)² / (2·cycle),
    effective_green = green − 횡단시간(dist/speed).

    ⚠️ 유효 녹색 규칙(§7.4: 통과 ⟺ 녹색 잔여 ≥ 횡단시간)과 **일치시킨다.** 녹색이라도
    남은 시간이 횡단시간보다 짧으면 못 건너므로, 실제로 '출발 가능한' 창은 green 이
    아니라 effective_green 이다. 그래서 대기 발생 구간은 red 가 아니라 (red + 횡단시간)이고
    기대대기는 red²/(2·cycle) 이 아니라 (cycle − effective_green)²/(2·cycle) 이다.
    이 형태라야 **빠른 사용자일수록 effective_green 이 커져 기대대기가 준다** — 속도
    개인화가 기대대기에 반영된다(옛 red²/(2·cycle) 은 속도 무관이라 개인화가 빠졌다).
    횡단 이동시간 자체는 도보시간에 이미 포함되므로 여기(대기창)에만 반영하고 따로 더하지 않는다.
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
    effective_green = green_s - crossing_s
    return (cycle_s - effective_green) ** 2 / (2 * cycle_s)


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
    """crosswalks.json 에 cycle/green 이 있는 횡단보도의 기대대기 합 (mode='expected')."""
    total = 0.0
    for cwid in sc.get("crossings", []):
        if cwid not in crosswalks:
            raise KeyError(f"{sc['id']}: unknown crosswalk {cwid}")
        c = crosswalks[cwid]
        if mode == "expected":
            total += expected_wait(c["green_s"], c["cycle_s"], c["dist_m"], speed_mps)
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


def actual_connection(sc: dict) -> str | None:
    """정답 = 실측만으로 정한 **물리적으로 잡을 수 있었던 가장 이른 후보** (안전버퍼 미적용).

    ⚠️ 정답은 관측이 정한다 — `--buffer`(제품 안전마진)와 무관해야 한다. 버퍼를 정답에
    섞으면 같은 관측인데도 버퍼값에 따라 정답이 달라진다(이전 버그). 제품 버퍼는 **예측**
    (choose_connection)과 **위험 판정**(dangerous)에만 쓴다. §9.3: 실제 탑승은 정의상
    catchable 이고 도착 이후 출발이라 이 buffer=0 최초값 이상이므로, 이 값이 정답이다.
    별도로 실제 탑승(`actual.boarded`)·제품 버퍼 예측은 각 모델 예측으로 따로 보고한다.
    """
    arrival = parse_dt(sc["actual"]["stop_arrival"])
    return choose_connection(actual_bus_times(sc), arrival, 0)


MAP_FAILURE_LABELS = {
    "different_bus": "다른 버스",
    "more_wait": "더 대기",
    "miss": "놓침",
}


def classify_map_outcome(sc: dict, buffer_s: int) -> list[str]:
    """지도(네이버) 안내가 실제와 어긋난 방식 — 검증의 축.

    도보 속도·신호를 반영하지 않은 기존 지도의 안내가 실제와 어떻게 벌어지는가를
    사용자가 정의한 '부정확' 세 유형으로 분류한다. 이 셋 중 하나라도 걸리면 지도
    안내가 실제와 달랐다는 뜻이고, 그때 우리 모델이 실제를 맞히는지가 검증 대상이다.

      different_bus: 지도가 명시한 버스와 실제로 타는 가장 이른 버스가 다르다
      miss:          지도가 명시한 버스가 실제 도착 시점엔 이미 떠났다 (놓침)
      more_wait:     실제 정류장 도착이 지도 예측보다 늦어 대기가 안내보다 늘었다

    빈 리스트면 지도 안내가 실제와 일치(정확)한 것이다.
    """
    named = sc["naver"].get("selected_connection")
    truth = actual_connection(sc)
    actual_arrival = parse_dt(sc["actual"]["stop_arrival"])
    departures = dict(actual_bus_times(sc))
    naver_arrival = parse_dt(sc["walk_start"]) + timedelta(
        seconds=float(sc["naver"]["walk_time_s"])
    )
    modes = []
    if named and truth and named != truth:
        modes.append("different_bus")
    # 놓침 = 지도 버스가 **실제 도착 전에** 이미 떠남(물리적 사실, 버퍼 무관). arrival+buffer 로
    # 재면 도착 뒤 출발한 잡을 수 있는 버스를 놓침으로 오검출한다.
    if named in departures and departures[named] < actual_arrival:
        modes.append("miss")
    # 더 대기 = 실제 도착이 지도 예측보다 늦음. 여기 buffer_s 는 분 단위 ETA 반올림 허용오차다.
    if (actual_arrival - naver_arrival).total_seconds() > buffer_s:
        modes.append("more_wait")
    return modes


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

    def expected_or_strict() -> float:
        return (
            signal_wait_s
            if signal_wait_s is not None
            else crossing_wait(sc, crosswalks, "expected", speed_mps)
        )

    # base_walk_s = 순수 이동(도보)시간, signal_component = 신호 추가대기.
    # 둘을 분리해야 §6.3의 절벽 판정을 신호와 독립적으로 적용할 수 있다.
    signal_component = 0.0
    # 신호 추정 소스 접미사 제거 — "Speed+Signal(유도)" → base "Speed+Signal".
    # 어느 소스(보수/유도)의 값을 넣었는지는 signal_wait_s 로 이미 들어와 있다.
    base = model.split("(", 1)[0]
    if base == "Naver":
        base_walk_s = naver_walk_s
    elif base == "Speed":
        base_walk_s = naver_walk_s * ratio
    elif base == "Speed+Signal":
        base_walk_s = naver_walk_s * ratio
        signal_component = expected_or_strict()
    elif base in ("TMAP-Personal", "TMAP+Signal"):
        if tmap_walk_s is None:
            raise ValueError(f"{sc['id']}: TMAP 보행시간이 없습니다.")
        base_walk_s = tmap_walk_s
        if base == "TMAP+Signal":
            signal_component = expected_or_strict()
    elif base in ("Kakao-Personal", "Kakao+Signal"):
        if kakao_walk_s is None:
            raise ValueError(f"{sc['id']}: 카카오 보행시간이 없습니다.")
        base_walk_s = kakao_walk_s
        if base == "Kakao+Signal":
            signal_component = expected_or_strict()
    else:
        raise ValueError(model)

    walk_s = base_walk_s + signal_component
    arrival = walk_start + timedelta(seconds=walk_s)
    # §6.3: 시각표를 지키는 수단(지하철·기점 광역)에만 '놓침 절벽'이 있다. 배차버스
    # (cliff=false)는 신호 Δ가 총시간엔 +Δ로 더해지지만(arrival/MAE), **연결 선택
    # 게이트에선 신호를 뺀 순수 이동 도착으로 판정**한다.
    # ⚠️ 연결은 차량 단위로 유지한다(6211_early vs 6211_next 는 다르다 — 이른 차를 잡으면
    #   그만큼 일찍 출발한다. 이 early/next 구분이 순증분의 핵심이라 노선으로 묶지 않는다).
    #   그럼에도 게이트에서 신호를 빼는 건 [[cliff-aware-connection]] 의 의도적 결정이다:
    #   배차버스는 놓쳐도 배차간격 안에 다음 차가 오므로(하드 절벽 없음), 신호 **과대예측**이
    #   연결을 next 로 잘못 뒤집는 위험이 이득보다 크다. 신호의 효과는 MAE(+Δ)로 남는다.
    cliff = bool(sc.get("cliff", True))
    gate_arrival = (
        arrival if cliff else walk_start + timedelta(seconds=base_walk_s)
    )
    if model == "Naver" and sc["naver"].get("selected_connection"):
        connection = sc["naver"]["selected_connection"]
    else:
        connection = choose_connection(
            predicted_bus_times(sc), gate_arrival, buffer_s
        )

    actual_arrival = parse_dt(sc["actual"]["stop_arrival"])
    truth = actual_connection(sc)
    connection_ok = connection == truth
    actual_by_id = dict(actual_bus_times(sc))
    dangerous = None
    if connection in actual_by_id:
        dangerous = actual_by_id[connection] < actual_arrival + timedelta(seconds=buffer_s)

    return Prediction(
        model=model,
        arrival=arrival,
        connection=connection,
        arrival_error_s=(arrival - actual_arrival).total_seconds(),
        connection_ok=connection_ok,
        dangerous=dangerous,
    )


def eligible(sc: dict, selected_split: str) -> bool:
    if sc.get("source") == "demo":
        return False
    if not sc.get("valid", True):
        return False
    # 공식 검증은 명시적인 실측 자료만 허용한다. source 누락·오타가 조용히
    # test 판정에 들어가면 demo/calibration 제외 규칙이 무력화된다.
    if selected_split == "test" and sc.get("source") != "real":
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
    rows = []
    for sc in scenarios:
        if not eligible(sc, selected_split):
            continue
        # 신호 추정 소스 정규화 → [(label, value)].
        #   None(비-strict)  → [("", None)]  : crosswalks.json 기대대기로 계산(라벨 없음)
        #   float            → [("", float)]  : 단일 소스(라벨 없음) — 하위호환
        #   dict{보수:x,유도:y} → 소스별 모델 변형 (Speed+Signal(보수)·(유도) …)
        strict_signal = signal_estimates is not None
        raw = signal_estimates.get(sc["id"]) if strict_signal else None
        if not strict_signal:
            signal_sources = [("", None)]
        elif raw is None:
            signal_sources = []                      # 이 이벤트는 신호 계산 불가
        elif isinstance(raw, dict):
            signal_sources = [(lb, float(v)) for lb, v in raw.items()]
        else:
            signal_sources = [("", float(raw))]

        tmap_walk_s = tmap_walk_times.get(sc["id"])
        kakao_walk_s = kakao_walk_times.get(sc["id"])

        def sig_models(base):   # base + 소스 접미사
            return [(base + (f"({lb})" if lb else ""), val) for lb, val in signal_sources]

        jobs = [("Naver", None), ("Speed", None)]
        jobs += sig_models("Speed+Signal")
        if tmap_walk_s is not None:
            jobs.append(("TMAP-Personal", None))
            jobs += sig_models("TMAP+Signal")
        if kakao_walk_s is not None:
            jobs.append(("Kakao-Personal", None))
            jobs += sig_models("Kakao+Signal")
        preds = {
            name: predict(
                sc, crosswalks, name, buffer_s, ratio_override,
                tmap_walk_s, kakao_walk_s, sig_val,
            )
            for name, sig_val in jobs
        }
        rows.append(
            {
                "id": sc["id"],
                "split": sc.get("split", "test"),
                "cliff": bool(sc.get("cliff", True)),
                "truth": actual_connection(sc),
                "boarded": sc["actual"].get("boarded"),
                "map_failure": classify_map_outcome(sc, buffer_s),
                "predictions": preds,
            }
        )

    # 모델 목록은 실제 예측 키에서 동적으로 — 신호 소스 접미사(보수/유도)가 붙어
    # 이름이 가변이기 때문. 등장 순서를 보존한다.
    models = []
    for row in rows:
        for k in row["predictions"]:
            if k not in models:
                models.append(k)
    metrics = {}
    for model in models:
        # 세 정답 개념을 분리해 각각 집계한다 (같은 관측, 다른 질문):
        #   connection_accuracy — 예측 == **물리적 최초 연결**(truth, 버퍼 무관 관측)
        #   boarded_accuracy    — 예측 == **실제 탑승**(actual.boarded), boarded 있는 행만
        #   dangerous           — 예측 버스가 실제로 도착+버퍼 전에 떠남(제품 안전, 버퍼 적용)
        pr = [(row["predictions"][model], row) for row in rows if model in row["predictions"]]
        if not pr:
            metrics[model] = {"n": 0, "mae_s": math.nan, "median_ae_s": math.nan,
                              "connection_accuracy": math.nan, "boarded_accuracy": math.nan,
                              "boarded_n": 0, "dangerous": 0}
            continue
        ps = [p for p, _ in pr]
        abs_errors = [abs(p.arrival_error_s) for p in ps]
        boarded_hits = [p.connection == r["boarded"] for p, r in pr if r.get("boarded")]
        metrics[model] = {
            "n": len(ps),
            "mae_s": mean(abs_errors),
            "median_ae_s": median(abs_errors),
            "connection_accuracy": sum(p.connection_ok for p in ps) / len(ps),
            "boarded_accuracy": (sum(boarded_hits) / len(boarded_hits)) if boarded_hits else math.nan,
            "boarded_n": len(boarded_hits),
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
    # ★ 판정 대상은 '어느 모델이 이기나'가 아니라, **이 접근(도보 속도 + 신호,
    #   맥락별 추정기)이 지도보다 나은가**의 적합성이다. 그래서 제품을 단일 모델이
    #   아니라 **이벤트 맥락별 판정 규칙**으로 둔다:
    #     · 절벽(cliff=true, 지하철)  → 유도(derived) + §6.6 안전마진(buffer_s)
    #     · 배차(cliff=false)         → 유도(derived), 연결 게이트에는 신호를 넣지 않음
    #   유도를 만들 수 없을 때만 보수(RoadUpper)를 최후 fallback으로 쓴다.
    def base_present(base):
        return n > 0 and all(
            any(k == base or k.startswith(base + "(") for k in row["predictions"])
            for row in rows
        )
    product_base = next(
        (b for b in ("TMAP+Signal", "Kakao+Signal", "Speed+Signal") if base_present(b)),
        None,
    )
    if signal_estimates is not None and product_base is None:
        return {
            "status": "insufficient", "n": n, "uplift": 0,
            "product_model": "Signal unavailable",
            "reason": "일부 표본에서 실시간 위상도 주기 기반 기대대기도 계산할 수 없음",
        }
    if product_base is None:
        product_base = "Speed+Signal"

    def product_pred(row):
        preds = row["predictions"]
        # 실측 주기 기반 정확값이 있으면 최우선, 없으면 유도 평균을 쓴다.
        # 보수(RoadUpper)는 유도도 만들 수 없을 때만 최후수단이다.
        return (preds.get(f"{product_base}(정확)")
                or preds.get(f"{product_base}(유도)")
                or preds.get(product_base)
                or preds.get(f"{product_base}(보수)"))
    pp = [product_pred(row) for row in rows]
    used = {p.model for p in pp}
    product_model = (next(iter(used)) if len(used) == 1
                     else f"{product_base}(정확/유도, 보수 최후수단)")

    # 절벽 표본의 존재와 안전 결과를 분리한다. 표본이 있다는 이유만으로 위험오답을
    # 낸 모델까지 "안전 검증됨"으로 표시하면 안 된다.
    cliff_pp = [p for row, p in zip(rows, pp) if row["cliff"]]
    cliff_scored = len(cliff_pp)
    cliff_dangerous = sum(p.dangerous is True for p in cliff_pp)
    cliff_observed = cliff_scored > 0
    safety_validated = cliff_observed and cliff_dangerous == 0

    uplift = sum(
        (not row["predictions"]["Naver"].connection_ok) and p.connection_ok
        for row, p in zip(rows, pp)
    )
    if n < MIN_TEST_EVENTS:
        return {
            "status": "insufficient", "n": n, "uplift": uplift,
            "product_model": product_model,
            "safety_validated": safety_validated,
            "cliff_observed": cliff_observed,
            "cliff_scored": cliff_scored,
            "cliff_dangerous": cliff_dangerous,
            "reason": f"새 test 표본 {MIN_TEST_EVENTS}건 필요",
        }

    naver = metrics["Naver"]
    product_accuracy = sum(p.connection_ok for p in pp) / n
    product_dangerous = sum(p.dangerous is True for p in pp)
    product_mae = mean(abs(p.arrival_error_s) for p in pp)
    # MAE(도착시각 정밀도)는 진단값 — 게이트 아님. 검증 대상은 '어느 버스를 타는가'다.
    mae_improvement = (
        1 - product_mae / naver["mae_s"] if naver["mae_s"] > 0 else float("-inf")
    )
    map_wrong = sum(1 for row in rows if not row["predictions"]["Naver"].connection_ok)
    # ★ 통과 = 안내(연결) 정확도 + 안전(위험오답 0) + 지도 대비 순증분.
    passed = (
        product_accuracy >= 2 / 3 and product_dangerous == 0 and uplift >= 1
    )
    return {
        # ⚠️ status 는 **연결(어느 차를 타나) 축의 통과**다. 절벽 안전은 별도 —
        #   safety_validated=false 면 이 pass 를 '제품 안전성 통과'로 읽지 말 것.
        "status": "pass" if passed else "fail",
        "n": n,
        "uplift": uplift,
        "product_model": product_model,
        "safety_validated": safety_validated,
        "cliff_observed": cliff_observed,
        "cliff_scored": cliff_scored,
        "cliff_dangerous": cliff_dangerous,
        "map_accuracy": naver["connection_accuracy"],
        "product_accuracy": product_accuracy,
        "map_wrong": map_wrong,
        "corrected": uplift,
        "mae_improvement": mae_improvement,   # 진단용 — 게이트 아님
        "connection_accuracy": product_accuracy,
        "dangerous": product_dangerous,
    }


def print_report(rows: list[dict], metrics: dict, result: dict, buffer_s: int) -> None:
    print(f"\n=== 개인화 연결 평가 (안전버퍼 {buffer_s}초) ===")
    if not rows:
        print("선택한 split에 유효한 표본이 없습니다.")
    for row in rows:
        cliff_tag = "절벽O" if row.get("cliff", True) else "절벽X(배차·신호 연결게이트 제외)"
        modes = row.get("map_failure", [])
        map_tag = ("지도 부정확: " + "·".join(MAP_FAILURE_LABELS.get(m, m) for m in modes)
                   if modes else "지도 정확")
        print(f"\n[{row['id']}] split={row['split']} {cliff_tag}  "
              f"실제 earliest={row['truth']} 실제탑승={row['boarded']}  [{map_tag}]")
        print(f'{"모델":<18}{"정류장 도착":<20}{"오차":>9}  {"선택 연결":<18}판정')
        for model, p in row["predictions"].items():
            danger = " 위험" if p.dangerous else ""
            print(
                f"{model:<18}{fmt_dt(p.arrival):<20}{p.arrival_error_s:>+8.1f}s  "
                f"{str(p.connection):<18}{'적중' if p.connection_ok else '오답'}{danger}"
            )

    print("\n--- 선택 표본 요약 ---")
    for model, m in metrics.items():
        if not m["n"]:
            continue
        print(
            f"{model:<18} n={m['n']}  MAE={m['mae_s']:.1f}s  "
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
            f"연결 안내 정확도 지도 {result['map_accuracy']:.0%} → 제품 "
            f"{result['product_accuracy']:.0%}, "
            f"지도 부정확 {result['map_wrong']}건 중 제품이 {result['corrected']}건 교정, "
            f"위험오답 {result['dangerous']}"
        )
        print(
            f"  (진단) 도착시각 MAE 개선 {result['mae_improvement']:+.0%} — "
            f"신호 보수 반영 시 음수일 수 있으나 게이트 아님"
        )
    # ★ 절벽 안전 검증 여부 — status(연결 축)와 분리해 항상 명시한다.
    if result.get("safety_validated"):
        print(f"  절벽 안전: ✅ 관측 표본 내 위험오답 없음 "
              f"(실측 cliff=true {result.get('cliff_scored', 0)}건)")
    elif result.get("cliff_observed"):
        print(f"  절벽 안전: ❌ 위험오답 {result.get('cliff_dangerous', 0)}건 "
              f"(실측 cliff=true {result.get('cliff_scored', 0)}건)")
    else:
        print("  절벽 안전: ⚠️ 미검증 — 채점 표본이 전부 배차버스(cliff=false)라 "
              "유도+§6.6 안전마진의 절벽 안전이 실측된 적 없다(마진 크기 미교정). "
              "위 판정은 연결(속도) 축 한정이다.")


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


def resolve_tmap_endpoints(tmap_config: dict, key: str | None = None) -> dict:
    """tmap start/end 가 정류장 참조(stop{city_code,ars})면 실측 좌표로 바꾼다.

    lon/lat 가 이미 있으면 그대로 둔다 (오프라인 재현 유지). 정류장 좌표를 못
    찾을 때 사용자가 준 ID(ARS)로 TAGO 에서 가져오는 경로다 — Case 1 E1 처럼
    TMAP 이 종점을 건물 좌표로 잡아 도보가 부풀려지는 것을 막는다.
    """
    if not tmap_config:
        return tmap_config
    out = dict(tmap_config)
    resolver = None
    for side in ("start", "end"):
        point = out.get(side)
        if (
            isinstance(point, dict)
            and "stop" in point
            and not ("lon" in point and "lat" in point)
        ):
            if resolver is None:
                from stop_client import resolve_stop as resolver  # 지연 import
            out[side] = resolver(point, key)
    return out


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
        help="TMAP 횡단보도에 신호 API를 매칭하고 불가하면 거리별 보수 사용",
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
                # 종점이 정류장 참조(stop{city_code,ars})면 TMAP 건물 좌표 대신
                # 실측 정류장 좌표로 해석한다 (Case 1 E1 종점 버그 방지).
                tmap_sc["tmap"] = resolve_tmap_endpoints(tmap_sc.get("tmap") or {})
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
            fallback_derived_total,
            fallback_signal_sensitivity,
            fetch_all,
            load_key as load_signal_key,
        )

        signal_estimates = {}
        if not tmap_summaries:
            print("신호 평가에는 TMAP 횡단보도 경로가 필요합니다.")
        else:
            try:
                signal_key = load_signal_key()
                intersections = fetch_all("crsrd_map_info", signal_key)
                signal_rows = fetch_all("tl_drct_info", signal_key)
            except RuntimeError as exc:
                intersections = []
                signal_rows = []
                print(
                    f"신호 API 사용 불가({exc}) → "
                    "TMAP 횡단보도마다 거리별 보수 fallback"
                )
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
                # ★ 보수(RoadUpper)과 유도(derived) 둘 다 계산해서 소스별로 넣는다.
                #   crosswalks.json 에 실측 주기가 있으면 estimate_route_wait 의
                #   wait_s(정확)도 함께 넣어 세 소스가 된다.
                mc = summary["crosswalks"]
                sources = {}
                if mc:
                    sources["보수"] = fallback_signal_sensitivity(mc)["road_upper_s"]
                    derived = fallback_derived_total(mc, speed_mps)
                    if derived is not None:      # None = 느린 사용자·긴 횡단이라 단일주기 불가
                        sources["유도"] = derived
                if result["wait_s"] is not None and result.get("complete"):
                    sources["정확"] = result["wait_s"]   # 실측 주기 기반(있을 때만)
                signal_estimates[sc["id"]] = sources or (result["wait_s"])
                methods = ",".join(
                    detail["method"] for detail in result["details"]
                ) or "no-crossing"
                src_text = " · ".join(f"{k} {v:.0f}s" for k, v in sources.items()) or (
                    f"{result['wait_s']:.1f}초" if result["wait_s"] is not None else "계산 불가")
                print(
                    f"Signal {sc['id']}: {result['crossings']}개 [{methods}] → {src_text}"
                )
                if result.get("used_fallback"):
                    fallback = result["fallback"]
                    raw = ", ".join(
                        f"{name}={seconds:.0f}s"
                        for name, seconds in fallback["raw_waits_s"].items()
                    )
                    bounds = ", ".join(
                        f"{row['distance_m']:.0f}m→{row['upper_s']:.0f}s"
                        for row in fallback["crossing_upper_bounds"]
                    )
                    print(
                        f"  TMAP-only fallback (거리별 보수 공식 평가 반영): "
                        f"횡단보도 {fallback['raw_count']}개 [{bounds}], "
                        f"합계={fallback['road_upper_s']:.0f}s [{raw}]"
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
