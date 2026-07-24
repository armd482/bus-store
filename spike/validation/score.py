#!/usr/bin/env python3
"""§9 5·6 최소 구현 — 정답지(S1~S5) 백테스트 스코어러 (docs §7.1·§7.4).

"지금 어디에도 없는 계산"(문서 line 1170)을 엔진 없이 ~한 파일로 만든 것.
OTP·GTFS·그래프빌드 전부 불필요. 정답지 + 횡단보도 데이터만으로 순수 산수.

각 정답지에서 세 예측을 실측과 대면한다:
  - S15   : OTP 기본 (횡단보도마다 15초, 사용자 속도 무시 → OTP 기본 속도)
  - Sm     : 제품 (사용자 실속도 + 횡단보도별 기대대기 + 속도별 유효녹색창)
  - (검증) : Sm 을 기대값/최악값 양쪽으로 — §7.4 가 요구하는 인공물 점검

  판정: Sm 이 실측 연결을 맞히고 S15 가 틀리면 winner=Sm.

사용:
  python3 score.py                       # 기대값 모델로 백테스트
  python3 score.py --worst               # 최악값(적색 전체)로도 — 둘 다 갈려야 진짜
  python3 score.py --sweep               # 속도별 catch/miss 뒤집기 위상 스윕(§7.4)
"""
import argparse
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))

# ── 모델 정의 ────────────────────────────────────────────────────────────────
# speed: 도보 속도(m/s). S15 는 사용자 속도를 무시하고 OTP 기본을 쓴다.
#        Sm 의 speed 는 정답지 실측 라이드에서 잰 "그 사람의 실제 속도"로 바꿀 것.
# OTP 2.x 기본 보행속도 = 1.33 m/s(4.8km/h). 설계속도 1.0 은 신호 산정용(§7.4)이지
# 사용자 속도가 아니다 — 혼동 금지.
OTP_DEFAULT_SPEED = 1.33
USER_SPEED = 1.45          # ← 정답지 실측으로 교체

MODELS = {
    "S15":    {"speed": OTP_DEFAULT_SPEED, "wait": "const15"},  # 진짜 베이스라인
    "Sm":     {"speed": USER_SPEED,        "wait": "expected"}, # 제품(기대값)
    # 참고·검증용 (기본 리포트엔 S15/Sm 만; 아래는 플래그로 켠다)
    "S0":     {"speed": OTP_DEFAULT_SPEED, "wait": "zero"},     # 신호 0 (부풀림, 참고만)
    "Sm_wc":  {"speed": USER_SPEED,        "wait": "worst"},    # 제품(최악값) — 인공물 점검
}


# ── §7.1·§7.4 물리 ──────────────────────────────────────────────────────────
def expected_wait(green, cycle, dist, speed):
    """속도-인지 기대대기(초). 임의 위상 도착 가정의 기대값.

    유도: 유효녹색 창 = green − dist/speed(§7.4). 이 창 밖(적색 + 횡단소요)에
    도착하면 다음 녹색까지 기다린다. 균일 위상에 대해 적분하면
        E[wait] = (red + dist/speed)² / (2·cycle),   red = cycle − green
    dist/speed(횡단소요) → 0 이면 문서 §7.1 의 red²/(2·cycle) 로 환원된다.
    즉 이 식은 §7.1(기대대기)과 §7.4(속도가 창을 가른다)의 통합이다.
    """
    red = cycle - green
    t_cross = dist / speed
    return (red + t_cross) ** 2 / (2 * cycle)


def wait_seconds(model_wait, c, speed):
    """한 횡단보도의 대기 비용(초)."""
    if model_wait == "zero":
        return 0.0
    if model_wait == "const15":       # OTP 뭉뚱그린 상수
        return 15.0
    if model_wait == "worst":         # 적색 전체 (bus-test 모델, 최악값)
        return c["cycle"] - c["green"]
    if model_wait == "expected":      # Sm
        return expected_wait(c["green"], c["cycle"], c["dist_m"], speed)
    raise ValueError(model_wait)


def effective_green(c, speed):
    """유효 녹색 창(초) = green − 횡단거리/속도 (§7.4). 음수면 이 속도론 못 건넌다."""
    return c["green"] - c["dist_m"] / speed


# ── 시각 유틸 ────────────────────────────────────────────────────────────────
def to_sec(hhmm):
    p = [int(x) for x in hhmm.split(":")]
    return p[0] * 3600 + p[1] * 60 + (p[2] if len(p) > 2 else 0)


def to_hhmm(sec):
    sec = int(round(sec))
    return f"{sec // 3600:02d}:{(sec % 3600) // 60:02d}:{sec % 60:02d}"


# ── 핵심: 정류장 도착 시각과 잡는 연결 ──────────────────────────────────────
def arrival_at_stop(sc, cw, model):
    """정답지 sc 의 도보 경로를 model 로 걸어 정류장 도착 시각(초, 하루 기준)."""
    speed, mwait = model["speed"], model["wait"]
    t = to_sec(sc["depart"]) + sc.get("fixed_s", 0)   # 출발 + 고정시간(개찰 등)
    for seg in sc["walk"]["segments"]:
        t += seg["dist_m"] / speed                    # 구간 도보
        for cwid in seg.get("crossings", []):
            t += wait_seconds(mwait, cw[cwid], speed)  # 횡단 대기
    return t


def catch_departure(timetable, arrival_sec, buffer_s=0):
    """도착시각 이후(+버퍼) 첫 출발. 시각표는 "HH:MM" 리스트."""
    for dep in timetable:
        if to_sec(dep) >= arrival_sec + buffer_s:
            return dep
    return None


# ── 백테스트 ────────────────────────────────────────────────────────────────
def backtest(gt, cw, baseline, product, buffer_s=0):
    """baseline(예: S15) vs product(예: Sm) 두 모델을 실측에 대면."""
    tally = {"product": 0, "baseline": 0, "neither": 0, "both": 0}
    rows = []
    for sc in gt:
        if not sc.get("observed_connection"):
            rows.append((sc["id"], "— 실측 미입력 —", "", "", ""))
            continue
        a_b = arrival_at_stop(sc, cw, MODELS[baseline])
        a_p = arrival_at_stop(sc, cw, MODELS[product])
        conn_b = catch_departure(sc["timetable"], a_b, buffer_s)
        conn_p = catch_departure(sc["timetable"], a_p, buffer_s)
        obs = sc["observed_connection"]
        p_ok, b_ok = conn_p == obs, conn_b == obs
        winner = "both" if p_ok and b_ok else "product" if p_ok \
            else "baseline" if b_ok else "neither"
        tally[winner] += 1
        rows.append((sc["id"], f"{to_hhmm(a_b)}→{conn_b}",
                     f"{to_hhmm(a_p)}→{conn_p}", obs, winner))
    return rows, tally


def print_report(title, rows, tally, baseline="S15", product="Sm"):
    print(f"\n=== {title} ===")
    print(f'{"ID":<5}{baseline+" 예측":<24}{product+" 예측":<24}{"실측":<12}{"승자"}')
    print("-" * 78)
    for r in rows:
        w = {"product": product, "baseline": baseline}.get(r[4], r[4])
        print(f"{r[0]:<5}{r[1]:<24}{r[2]:<24}{r[3]:<12}{w}")
    scored = tally["product"] + tally["baseline"]   # 분화한(예측이 갈린) 사례만
    print("-" * 78)
    print(f'분화(예측 갈림): {scored}건  |  {product} 승 {tally["product"]} · '
          f'{baseline} 승 {tally["baseline"]}  |  둘다맞음 {tally["both"]} · '
          f'둘다틀림 {tally["neither"]}')
    if scored:
        ok = tally["product"] > tally["baseline"]
        print(f'▶ 지표2(정확도): {product} {tally["product"]} vs {baseline} '
              f'{tally["baseline"]} → {"통과 (제품 승 > 패)" if ok else "실패"}')
    else:
        print("▶ 이 표본에선 예측이 안 갈림 — OD·시간대를 늘리거나 분화 사례를 찾을 것")


def sweep_flip(gt, cw, slow=1.0, fast=USER_SPEED, step=1):
    """§7.4 물리 직접 검증 — 각 횡단보도에서 위상 phase 를 0..cycle 로 훑어
    느린 속도와 빠른 속도의 '이번 녹색 통과(catch)' 여부가 뒤집히는 구간 비율."""
    print(f"\n=== 속도별 catch/miss 뒤집기 (slow {slow} vs fast {fast} m/s) ===")
    seen = set()
    for sc in gt:
        for seg in sc["walk"]["segments"]:
            for cwid in seg.get("crossings", []):
                if cwid in seen:
                    continue
                seen.add(cwid)
                c = cw[cwid]
                eg_slow = effective_green(c, slow)
                eg_fast = effective_green(c, fast)
                flip = 0
                for ph in range(0, int(c["cycle"]), step):
                    catch_slow = ph <= eg_slow
                    catch_fast = ph <= eg_fast
                    if catch_slow != catch_fast:
                        flip += step
                frac = flip / c["cycle"]
                print(f'{cwid:<8} dist {c["dist_m"]}m cycle {c["cycle"]}s green {c["green"]}s '
                      f'| 유효창 느림 {eg_slow:.0f}s / 빠름 {eg_fast:.0f}s '
                      f'| 뒤집힘 {frac*100:.0f}% 위상')
    print("→ 뒤집힘 %가 클수록 속도가 신호 통과를 가르는 힘이 큼 (0이면 그 횡단보도는 무력)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--worst", action="store_true", help="최악값(60s) 모델로도 돌린다")
    ap.add_argument("--sweep", action="store_true", help="속도별 catch/miss 위상 스윕")
    ap.add_argument("--buffer", type=int, default=0, help="탑승 안전버퍼(초)")
    args = ap.parse_args()

    gt = json.load(open(os.path.join(HERE, "ground_truth.json"), encoding="utf-8"))
    cw = json.load(open(os.path.join(HERE, "crosswalks.json"), encoding="utf-8"))

    rows, tally = backtest(gt, cw, "S15", "Sm", args.buffer)
    print_report("기대값 모델 (Sm = red²/2cycle 통합식)", rows, tally)

    if args.worst:
        rows_w, tally_w = backtest(gt, cw, "S15", "Sm_wc", args.buffer)
        print_report("최악값 모델 (Sm = 적색 전체) — 인공물 점검",
                     rows_w, tally_w, "S15", "Sm_wc")
        print("\n※ §7.4 교정: 기대값에서도 Sm 이 이겨야 진짜. 최악값에서만 갈리면 인공물.")

    if args.sweep:
        sweep_flip(gt, cw)


if __name__ == "__main__":
    main()
