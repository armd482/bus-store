#!/usr/bin/env python3
"""
지하철 정시성 판정 — 계획 시각표 vs 실측 (docs §8.1 · §8.2 · §8 #1)

§8 #1(지하철 절벽 = B+ 핵심 차별점)의 정식 판정기. §8.2 는 '셀 내 재현성 σ'로
쟀지만(계획표 없이 같은 (열차,역)의 날짜 간 분산), 이건 **공식 계획 시각표와의
편차**를 직접 잰다 — 진짜 정시성이다.

방법 (§8.1 못박은 규칙 그대로):
  ① 시각근접매칭 — 열차번호 조인 불가(신분당선 무 trainNo·1~9호선 코드형식 불일치)라
     각 실측 도착을 가장 가까운 계획에 붙인다. |Δ|≥배차/2 는 미매칭(그 자체가 신호).
  ② 실측 시각은 `recptnDt`(BIS 수신, recptn_of 로 자정보정) — 우리 폴링 t 아님.
     stale(죽은 레코드) 제외. 도착(arvlCd=1)만.
  ③ 임계값은 배차 H 대비: σ≤0.05H 절벽신뢰 · σ≥0.3H 무의미. 노선별 H 와 함께 본다.
  · 공휴일 제외(장부와 같은 기준). 요일종 매칭: 평일↔평일 · 토↔토요일 · 일/공휴일↔공휴일.

계획 소스:
  · 신분당선  → 운영사 PDF (subway_timetable.parse_sinbundang) — 방면=상/하행
  · 1~9호선   → 15098251 CSV (parse_stcs) — UP/DOWN/OUT/IN(2호선 순환)
  그 외(수인분당·경의중앙·경춘·경강·서해·GTX-A 등)는 공식 계획표가 없어 판정 불가(§3.3.1).

⚠️ 신분당선 주의(§8.1 ⑤ 가): recptnDt 가 미래(예측성)일 수 있다. 편차가 비정상적으로
   작으면(σ→0) 예측이 계획을 복사한 순환일 수 있으니 audit-subway 와 함께 읽을 것.

사용:
  python3 judge_subway.py --stcs <15098251.csv> --sinbundang <평일.pdf> <공휴일.pdf> \
                          --obs '/path/subway-2026-07-*.jsonl.gz'
  python3 judge_subway.py --stcs ... --obs '...' --line 2호선   # 역별 상세
"""

import argparse
import glob
import gzip
import json
import re
import statistics
import sys
from datetime import datetime

import orchestrator as O
import subway_timetable as T


# 괄호 제거로도 안 맞는 별칭 — 양쪽(jsonl 실측·CSV 계획)을 한 canonical 로 수렴시킨다.
# ✅ 전 노선 대조로 찾은 잔여 불일치: 실측 realtime 이 옛/기본명을, CSV 가 개정/병기명을
#    (또는 그 반대) 쓰는 경우. 각 역은 노선 내 유일해 오병합 위험 없다.
_STATION_ALIAS = {
    "서울": "서울역",          # 1·4호선 realtime '서울' ↔ CSV '서울역'
    "평택지제": "지제",        # 1호선 CSV '평택지제' ↔ realtime '지제'
    "이수": "총신대입구",      # 7호선 CSV '이수' ↔ realtime '총신대입구'(4호선 병기명과 동일역)
    "자양": "뚝섬유원지",      # 7호선 CSV '자양(뚝섬한강공원)' ↔ realtime '뚝섬유원지'(구명)
    "응암순환": "응암",        # 6호선 루프 회차 라벨 ↔ CSV '응암'
}


def norm_station(s):
    """역명 정규화 — 조인 키. ① 병기역명 괄호 제거(대흥(서강대앞)→대흥) ② 별칭 수렴.
    실측 jsonl 과 15098251 CSV 의 역명 표기가 달라(괄호·구명·병기명) 안 맞추면 그 역
    도착이 통째로 미매칭된다 — ✅ 6호선은 이 때문에 미매칭 24%였다(정규화 후 1%)."""
    if not s:
        return s
    s = re.sub(r"\(.*?\)", "", s).strip()
    return _STATION_ALIAS.get(s, s)


# 계획 방향 라벨 → 정규 방향. 실측 updnLine 도 같은 정규값으로 맞춘다.
DIR_CANON = {
    # 15098251 CSV
    "UP": "up", "DOWN": "dn", "OUT": "out", "IN": "in",
    # 신분당선 PDF
    "신사방면": "up", "광교방면": "dn",
    # 실측 jsonl updnLine
    "상행": "up", "하행": "dn", "외선": "out", "내선": "in",
}

# CSV 호선("2") ↔ 실측 line("2호선")
def csv_line_to_obs(ln):
    return f"{ln}호선"


def absmin(h, m, sec=0):
    """운행일 절대분 — 자정 이후(h<4)는 +24h. 계획·실측 같은 규약을 써야 매칭된다.
    CSV 가 25:10 처럼 24 초과로 저장해도 그대로 절대분이 되어 00:xx 실측과 만난다."""
    return (h + (24 if h < 4 else 0)) * 60 + m + sec / 60.0


def obs_daytype_to_plan(dt, has_saturday):
    """실측 요일(mon~sun) → 계획 요일종. 신분당선 PDF 는 토요일이 없어 공휴일로 폴백(§8 #3)."""
    if dt == "sat":
        return "토요일" if has_saturday else "공휴일"
    if dt == "sun":
        return "공휴일"
    return "평일"


def build_plan(stcs_csv, sinbundang_pdfs):
    """계획 인덱스 — plan[(line,station,daytype,dir)] = 정렬된 절대분 리스트."""
    plan = {}
    daytypes_with_sat = set()
    recs = []
    if stcs_csv:
        recs += [dict(r, line=csv_line_to_obs(r["line"])) for r in T.parse_stcs(stcs_csv)]
    for p in (sinbundang_pdfs or []):
        dt = "공휴일" if "공휴일" in p else "토요일" if "토요일" in p else "평일"
        recs += T.parse_sinbundang(p, dt)
    for r in recs:
        d = DIR_CANON.get(r["direction"])
        if d is None:
            continue
        if r["daytype"] == "토요일":
            daytypes_with_sat.add(r["line"])
        key = (r["line"], norm_station(r["station"]), r["daytype"], d)
        plan.setdefault(key, []).append(absmin(r["h"], r["m"]))
    for k in plan:
        plan[k].sort()
    return plan, daytypes_with_sat


def headway(times):
    """정렬된 계획분 리스트의 중앙 배차(분). 매칭 창(H/2)·임계값(§8.1 ③)에 쓴다."""
    if len(times) < 2:
        return None
    gaps = [b - a for a, b in zip(times, times[1:]) if 0 < b - a < 60]
    return statistics.median(gaps) if gaps else None


# §8.3 도착 대용 — 폴링이 성겨(특히 2·9호선) 도착(arvlCd=1)을 놓친 방문을
# 진입(0)/출발(2)로 채운다. 실측 시차 (2026-07-23, 5일치): 진입→도착 +47s · 도착→출발 34s.
ENTER_TO_ARRIVE = 47.0 / 60.0    # 진입(0) 관측 → 도착 추정: +47초 (분)
DEPART_TO_ARRIVE = -34.0 / 60.0  # 출발(2) 관측 → 도착 추정: −34초 (분)
VISIT_GAP_MIN = 3.0              # 같은 (열차,역) 이벤트가 이 안이면 한 '방문'


def load_obs(obs_glob, plan_lines, substitute=True):
    """실측 도착 인덱스 — obs[(line,station,daytype,dir)] = [(절대분, 대용여부)...].

    §8.3 도착 대용: (line,trainNo,statnId,운행일) 이벤트를 시각순으로 모아 3분 이내를
    한 방문으로 묶고, 방문마다 도착(1)이 있으면 그 시각, 없으면 진입(0)+47s 또는
    출발(2)−34s 로 **도착을 추정**한다. jsonl·수집기는 안 건드리고 여기서만 (jsonl=진리).
    substitute=False 면 예전처럼 도착(1)만 쓴다(정밀화 전후 비교용).

    recptn_of(자정보정)·stale 제외·공휴일 제외. 계획이 있는 노선만.
    """
    files = sorted(glob.glob(obs_glob))
    if not files:
        sys.exit(f"실측 파일이 없다: {obs_glob}")
    hols = O.holiday_set(offline=True)   # 캐시만 — 네트워크 안 건드림
    # (line,trainNo,statnId,운행일) -> [(절대분, arvlCd, statnNm, dir, daytype)]
    units = {}
    skipped_stale = skipped_dir = 0
    for f in files:
        opener = gzip.open if f.endswith(".gz") else open
        for line in opener(f, mode="rt", encoding="utf-8"):
            try:
                r = json.loads(line)
            except ValueError:
                continue
            cd = str(r.get("arvlCd"))
            if cd not in ("0", "1", "2"):
                continue
            ln = r.get("line")
            if ln not in plan_lines:
                continue
            b = O.recptn_of(r)
            if b is None or O.stale(r):
                skipped_stale += 1
                continue
            sday = O.service_day_of(b)
            sd = sday.strftime("%Y-%m-%d")
            if sd in hols:
                continue
            d = DIR_CANON.get(r.get("updnLine"))
            if d is None:
                skipped_dir += 1
                continue
            am = absmin(b.hour, b.minute, b.second)
            units.setdefault((ln, r.get("trainNo"), r.get("statnId"), sd), []).append(
                (am, cd, norm_station(r.get("statnNm")), d, O.day_type(b)))

    obs = {}
    kept = subbed = 0
    for (ln, _tn, _sid, _sd), events in units.items():
        events.sort()
        # 3분 이내를 한 방문으로 클러스터 (신분당선처럼 번호가 종일 재사용돼도
        # 같은 역 재방문은 수십 분 간격이라 서로 다른 방문으로 갈린다)
        visit = []
        for ev in events + [None]:
            if visit and (ev is None or ev[0] - visit[-1][0] > VISIT_GAP_MIN):
                arr = _arrival_of(visit, substitute)
                if arr is not None:
                    am, was_sub = arr
                    _, _, stn, d, dtw = visit[0]
                    dt = obs_daytype_to_plan(dtw, True)
                    obs.setdefault((ln, stn, dt, d), []).append((am, was_sub))
                    kept += 1
                    subbed += int(was_sub)
                visit = []
            if ev is not None:
                visit.append(ev)
    return obs, kept, subbed, skipped_stale, skipped_dir


def _arrival_of(visit, substitute):
    """한 방문 → (도착 절대분, 대용여부). 도착(1) 우선, 없으면 진입(0)+47s/출발(2)−34s."""
    times = {cd: am for am, cd, *_ in visit}
    if "1" in times:
        return times["1"], False
    if not substitute:
        return None
    if "0" in times:
        return times["0"] + ENTER_TO_ARRIVE, True
    if "2" in times:
        return times["2"] + DEPART_TO_ARRIVE, True
    return None


def match_deltas(obs_times, plan_times, half):
    """각 실측을 가장 가까운 계획에 붙인다. 반환 (매칭 Δ초 리스트, 미매칭 수).
    Δ = 실측 − 계획 (양수 = 지연). |Δ| > half(분) 는 미매칭."""
    deltas, unmatched = [], 0
    for am in obs_times:
        near = min(plan_times, key=lambda p: abs(p - am))
        d = am - near
        if abs(d) <= half:
            deltas.append(d * 60.0)
        else:
            unmatched += 1
    return deltas, unmatched


def judge(plan, sat_lines, obs, focus_line=None):
    # (line,station,daytype,dir) 단위로 매칭 → 노선(또는 역)별 집계
    # ⚠️ 방향 인코딩이 소스마다 어긋나는 경우가 있다 — 2호선 지선은 실측 외선/내선(out/in)
    #    인데 15098251 계획은 UP/DOWN(up/dn)이다. 정확 방향 계획이 없으면 그 역의 전
    #    방향 계획을 합쳐(pooled) 매칭한다 (본선처럼 방향이 맞는 경우엔 폴백이 안 탄다).
    station_plan = {}   # (line,station,daytype) -> 합친 정렬 계획분
    for (ln, st, dt, d), times in plan.items():
        station_plan.setdefault((ln, st, dt), []).extend(times)
    for k in station_plan:
        station_plan[k].sort()

    per_line = {}     # line -> {deltas, unmatched, matched, H들}
    per_station = {}  # (line,station) -> deltas  (focus_line 일 때)
    for (ln, st, dt, d), items in obs.items():
        ptimes = None
        for plan_dt in (dt, "공휴일" if dt == "토요일" else None):
            if plan_dt is None:
                continue
            # 신분당선은 토요일 계획이 없으면 공휴일로 폴백 (§8 #3)
            if plan_dt == "토요일" and ln not in sat_lines:
                continue
            ptimes = plan.get((ln, st, plan_dt, d)) or station_plan.get((ln, st, plan_dt))
            if ptimes:
                break
        pl = per_line.setdefault(ln, {"deltas": [], "resid": [], "unmatched": 0,
                                      "matched": 0, "H": []})
        if not ptimes:
            pl["unmatched"] += len(items)     # 계획이 없는 (역,요일,방향)
            continue
        H = headway(ptimes) or 5.0
        pl["H"].append(H)
        deltas, un = match_deltas([am for am, _ in items], ptimes, H / 2.0)
        pl["deltas"] += deltas                # 원시 Δ (역별 bias 포함)
        pl["unmatched"] += un
        pl["matched"] += len(deltas)
        # ★ 역별 디트렌드 — 이 (역,요일,방향)의 median 을 빼 residual 로 모은다.
        #   노선 σ 를 역 내 편차로만 재므로, 역마다 다른 수신지연 offset(신분당선
        #   −88~+167s)이 노선 σ 를 부풀리던 것을 걷어낸다 (진짜 시각표 준수도).
        if len(deltas) >= 3:
            gmed = statistics.median(deltas)
            pl["resid"] += [x - gmed for x in deltas]
        if focus_line and ln == focus_line:
            per_station.setdefault((ln, st, d), []).extend(deltas)
    return per_line, per_station


SIGNAL_MIN = 1.5   # 신호 대기 기준(분) — §8.1 임계 맥락(첨두 90초). σ 를 이것과 비교.


def line_summary(per_line):
    """per_line → 노선별 요약 dict 리스트 (report·emit 공용). σ 는 분 단위."""
    out = []
    for ln, v in per_line.items():
        if not v["deltas"]:
            continue
        med = statistics.median(v["deltas"])
        sd = statistics.pstdev(v["deltas"]) if len(v["deltas"]) > 1 else 0
        sd_in = statistics.pstdev(v["resid"]) if len(v["resid"]) > 1 else sd
        H = statistics.median(v["H"]) if v["H"] else 0
        tot = v["matched"] + v["unmatched"]
        ratio = (sd_in / 60) / H if H else 0
        out.append({
            "name": ln, "matched": v["matched"],
            "unmatchedPct": round(v["unmatched"] / tot, 3) if tot else 0,
            "biasSec": round(med), "sigmaRawSec": round(sd), "sigmaInSec": round(sd_in),
            "sigmaInMin": round(sd_in / 60, 2), "headwayMin": round(H, 1),
            "ratio": round(ratio, 3),
            "verdict": ("cliff" if ratio <= 0.05 else "buffer" if ratio < 0.3 else "none"),
        })
    return sorted(out, key=lambda x: -x["matched"])


def build_snapshot(per_line, span, days):
    """대시보드 배너용 판정 스냅샷 (오프라인 계산 결과. server.py 가 읽는다)."""
    lines = line_summary(per_line)
    ratios = [l["sigmaInMin"] / l["headwayMin"] for l in lines if l["headwayMin"]]
    below = sum(1 for l in lines if l["sigmaInMin"] < SIGNAL_MIN)
    return {
        "done": True, "method": "공식 시각표 시각근접매칭 + §8.3 도착대용 + 역별 디트렌드",
        "span": span, "days": days, "signalMin": SIGNAL_MIN,
        "linesJudged": len(lines),
        "belowSignalLines": below,
        "medRatio": round(statistics.median(ratios), 3) if ratios else None,
        "sigmaMinMed": round(statistics.median([l["sigmaInMin"] for l in lines]), 2) if lines else None,
        "lines": lines,
    }


def report(per_line, per_station, focus_line):
    # σ_raw = 원시(역별 bias 포함) · σ_in = 역별 디트렌드(역 내 준수도). 판정은 σ_in/H.
    print(f"\n{'노선':<10}{'매칭':>8}{'미매칭%':>7}{'중앙Δ':>7}{'σ원시':>7}{'σ역내':>7}"
          f"{'H':>6}{'σ역내/H':>8}  판정")
    for ln in sorted(per_line, key=lambda x: -per_line[x]["matched"]):
        v = per_line[ln]
        tot = v["matched"] + v["unmatched"]
        if not v["deltas"]:
            print(f"{ln:<10}{v['matched']:>8}{'—':>7}  계획 매칭 0 (계획표 없거나 방향/역명 불일치)")
            continue
        med = statistics.median(v["deltas"])
        sd = statistics.pstdev(v["deltas"]) if len(v["deltas"]) > 1 else 0
        sd_in = statistics.pstdev(v["resid"]) if len(v["resid"]) > 1 else sd
        H = statistics.median(v["H"]) if v["H"] else 0
        unpct = v["unmatched"] / tot if tot else 0
        ratio = (sd_in / 60) / H if H else 0
        verdict = ("✅ 절벽신뢰" if ratio <= 0.05 else
                   "⚠️ 완충필요" if ratio < 0.3 else "❌ 절벽무의미")
        print(f"{ln:<10}{v['matched']:>8}{unpct:>6.0%}{med:>6.0f}s{sd:>6.0f}s{sd_in:>6.0f}s"
              f"{H:>5.1f}분{ratio:>8.2f}  {verdict}")
    print("\nΔ = 실측도착(§8.3 도착대용 포함) − 계획(초). 양수=지연.")
    print("σ원시 = 역별 offset(수신지연) 포함 · σ역내 = 역별 median 뺀 준수도. 판정은 σ역내/H.")
    print("σ역내/H ≤0.05 절벽신뢰 · <0.3 완충필요 · ≥0.3 무의미 (§8.1 ③). 미매칭%=|Δ|>배차/2.")

    if focus_line and per_station:
        print(f"\n=== {focus_line} 역·방향별 ===")
        print(f"{'역':<12}{'방향':<5}{'매칭':>6}{'중앙Δ':>8}{'σ':>8}")
        for (ln, st, d), ds in sorted(per_station.items(), key=lambda x: -len(x[1])):
            if not ds:
                continue
            print(f"{st:<12}{d:<5}{len(ds):>6}{statistics.median(ds):>7.0f}s"
                  f"{statistics.pstdev(ds) if len(ds)>1 else 0:>7.0f}s")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stcs", help="15098251 CSV (1~9호선)")
    ap.add_argument("--sinbundang", nargs="*", default=[], help="신분당선 PDF(평일/공휴일)")
    ap.add_argument("--obs", required=True, help="실측 glob (예 'data/subway-*.jsonl*')")
    ap.add_argument("--line", help="이 노선 역별 상세도 출력")
    ap.add_argument("--emit", help="판정 스냅샷 JSON 경로 (대시보드 배너용, 오프라인 계산)")
    args = ap.parse_args()
    if not args.stcs and not args.sinbundang:
        sys.exit("계획 소스가 없다 — --stcs 또는 --sinbundang 중 하나는 필요.")

    plan, sat_lines = build_plan(args.stcs, args.sinbundang)
    plan_lines = {k[0] for k in plan}
    print(f"계획: {len(plan):,} (노선,역,요일,방향) 슬롯 · 노선 {sorted(plan_lines)}")
    obs, kept, subbed, stale, nodir = load_obs(args.obs, plan_lines)
    print(f"실측 도착 {kept:,}건 채택 (§8.3 도착대용 {subbed:,} = {subbed/kept*100 if kept else 0:.0f}%) "
          f"· stale 제외 {stale:,} · 방향불명 {nodir:,}")
    per_line, per_station = judge(plan, sat_lines, obs, args.line)
    report(per_line, per_station, args.line)

    if args.emit:
        # 실측 파일명(subway-YYYY-MM-DD)에서 관측 구간을 뽑아 스냅샷에 남긴다
        import re
        days = sorted(set(re.findall(r"subway-(\d{4}-\d{2}-\d{2})", " ".join(glob.glob(args.obs)))))
        span = f"{days[0]}~{days[-1]}" if days else ""
        snap = build_snapshot(per_line, span, len(days))
        snap["date"] = datetime.now().strftime("%Y-%m-%d")
        with open(args.emit, "w", encoding="utf-8") as f:
            json.dump(snap, f, ensure_ascii=False, indent=1)
        print(f"\n→ 스냅샷 {args.emit} (노선 {snap['linesJudged']} · "
              f"σ중앙 {snap['sigmaMinMed']}분 · σ/H중앙 {snap['medRatio']})")


if __name__ == "__main__":
    main()
