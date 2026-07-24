#!/usr/bin/env python3
"""
수집 서버 — 수집기 + 대시보드를 한 프로세스로 (docs/collector-design.md)

다른 컴퓨터에서 24시간 무인으로 돌리고, 브라우저로 상태를 본다.
표준 라이브러리만 쓴다 (설치할 것 없음).

  python3 server.py                 # 수집 + 대시보드 (기본 877 포트)
  python3 server.py --port 9000
  python3 server.py --no-collect    # 대시보드만 (조회용)

⚠️ 대시보드는 인증이 없다. 같은 기계에 API 키(.env)가 있으므로
   외부에 열려면 그 앞에 인증을 두거나 방화벽으로 막을 것.
"""

import argparse
import json
import os
import sys
import threading
import time
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import holidays as H
import orchestrator as O

try:
    import bus_collector
except Exception:
    bus_collector = None

try:
    import subway_collector          # 별도 프로세스 — 대시보드는 그 디스크 산출물만 읽는다
except Exception:
    subway_collector = None

try:
    import seoul_collector           # 서울 버스 스냅샷 — 역시 별도 프로세스
except Exception:
    seoul_collector = None

# 수집기가 갱신하는 최근 상태 (대시보드가 읽는다)
STATE = {
    "started": None, "cycles": 0, "lastObs": None, "lastCycleSec": None,
    "picked": 0, "moving": 0, "errors": {}, "written": 0, "night": False,
    "fetching": False,  # 지금 API 요청 사이클이 도는 중인가 (대시보드 표시용)
    "errLog": [],       # 최근 잔여 실패 이력 (대시보드 오류 리스트)
}
LOCK = threading.Lock()


AVG_ROW_BYTES = 222  # 슬림 형식 행 평균 (✅ 실측) — 행 수 추정용

DAYS7 = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")  # 버스·지하철 공통 요일 7종

# 지하철 jsonl 증분 리더 상태 — /api 마다 풀스캔하지 않기 위한 캐시 (subway_snapshot).
# ⚠️ ThreadingHTTPServer 라 /api 가 동시에 여러 스레드에서 온다 — 락 없이 갱신하면
#    offset·카운트가 이중 가산된다. 표시용이라 치명적이진 않지만 락이 한 줄이다.
_SUB_TAIL = {}
_SUB_LOCK = threading.Lock()

# 서울 스냅샷도 같은 이유로 증분 리더 (하루 50만 행이라 풀스캔이면 /api 가 늘어진다)
_SEOUL_TAIL = {}
_SEOUL_LOCK = threading.Lock()

# 대시보드용 sqlite 연결 하나를 공유한다 — /api(5초)마다 connect() 하면 매번
# CREATE TABLE·PRAGMA 가 돌고 연결이 닫히지 않은 채 GC 로 넘어간다.
# ThreadingHTTPServer 는 요청마다 새 스레드라 thread-local 로는 재사용이 안 되므로,
# check_same_thread=False 로 하나를 만들고 _DB_LOCK 으로 직렬화한다.
# 읽기 전용이고 WAL 이라 수집기의 쓰기를 막지 않는다.
_DB = None
_DB_LOCK = threading.Lock()


def db():
    global _DB
    if _DB is None:
        _DB = O.connect(check_same_thread=False)
    return _DB


def _obs_rate_per_day():
    """(하루 관측률 행/일, 측정 창 일수) — 최근 1~2 운행일 jsonl 크기 ÷ 경과 시간.

    창 일수도 돌려준다 — ETA 가 측정 창이 얇을 때(방금 시작) 요동치므로,
    호출부가 이 값으로 '측정 중'을 판단한다. 행 수를 직접 세지 않고
    크기 ÷ 평균 행 크기로 추정한다 (수백 MB 를 5초마다 세지 않기 위함).
    """
    now = datetime.now(O.KST)
    sd = O.service_day_of(now)
    anchor = now.replace(hour=4, minute=0, second=0, microsecond=0)
    if now.hour < 4:
        anchor -= timedelta(days=1)          # 운행일 시작(04시) 앵커
    elapsed = (now - anchor).total_seconds()

    window, size = 0.0, 0
    p_today = os.path.join(O.DATA, f"bus-{sd:%Y-%m-%d}.jsonl")
    p_prev = os.path.join(O.DATA, f"bus-{sd - timedelta(days=1):%Y-%m-%d}.jsonl")
    if os.path.exists(p_today) and elapsed > 3600:
        size += os.path.getsize(p_today)
        window += elapsed
    if os.path.exists(p_prev):
        size += os.path.getsize(p_prev)
        window += 86400
    if window < 3600 or size == 0:
        return None, 0.0                     # 표본 부족
    return (size / AVG_ROW_BYTES) / (window / 86400), window / 86400


def seoul_snapshot():
    """서울 버스 도착정보 스냅샷 현황 — 밴드별 진행과 쿼터.

    지하철과 지표가 다르다. 서울은 셀을 쌓는 게 아니라 **밴드마다 노선당 1회**
    찍어 CV(§6.4)를 재는 것이라, 볼 것은 '오늘 이 밴드에서 702노선 중 몇 개를
    찍었나' 와 '쿼터가 남았나' 다. 진행은 매일 0 에서 다시 시작한다.
    """
    if seoul_collector is None:
        return {"present": False}
    now = datetime.now(O.KST)
    day = seoul_collector.service_day(now)
    qday = seoul_collector.quota_day(now)
    cap = seoul_collector.DAILY_CAP
    try:
        with open(seoul_collector.ROUTES_FILE, encoding="utf-8") as f:
            R = json.load(f)
    except (OSError, ValueError):
        R = {}
    done = seoul_collector.load_done(day)
    bands = O.cfg()["timebands"]
    per = {}
    for b, _ in done:
        per[b] = per.get(b, 0) + 1
    cur = O.band_of(now, bands)

    # 오늘 jsonl — 행 수와 마지막 관측 (지하철과 같은 증분 리더)
    path = os.path.join(O.DATA, f"{seoul_collector.PREFIX}-{day}.jsonl")
    cache = _SEOUL_TAIL
    with _SEOUL_LOCK:
        if cache.get("path") != path:
            cache.update(path=path, offset=0, total=0, last_t=None, last_no=None, last_stn=None)
        try:
            size = os.path.getsize(path)
            if size < cache["offset"]:
                cache.update(offset=0, total=0)
            if size > cache["offset"]:
                with open(path, "rb") as fh:
                    fh.seek(cache["offset"])
                    for raw in fh:
                        if not raw.endswith(b"\n"):
                            break
                        cache["offset"] += len(raw)
                        cache["total"] += 1
                        try:
                            r = json.loads(raw.decode("utf-8"))
                        except (ValueError, UnicodeDecodeError):
                            continue
                        cache["last_t"], cache["last_no"], cache["last_stn"] = \
                            r.get("t"), r.get("no"), r.get("stNm")
        except OSError:
            pass
        written, last_t = cache["total"], cache["last_t"]
        last_no, last_stn = cache["last_no"], cache["last_stn"]
    last_epoch = None
    if last_t:
        try:
            last_epoch = datetime.fromisoformat(last_t).timestamp()
        except ValueError:
            pass

    calls = seoul_collector.read_calls(qday)
    return {
        "present": True, "started": bool(R),
        "routes": len(R), "calls": calls, "cap": cap,
        "need": len(R) * len(bands),          # 하루에 필요한 콜 (밴드마다 노선당 1회)
        "written": written, "lastObs": last_epoch,
        "lastNo": last_no, "lastStn": last_stn,
        "curBand": cur,
        "bands": [{"i": i, "from": a, "to": b if b <= 24 else b - 24, "wrap": b > 24,
                   "done": per.get(i, 0), "total": len(R),
                   "pct": per.get(i, 0) / len(R) if R else 0,
                   "cur": i == cur} for i, (a, b) in enumerate(bands)],
    }


def subway_snapshot():
    """지하철 수집 현황 (B안 셀) — 노선별로 집계한다. 셀은 coverage.sqlite 의 subway_cell,
    실시간 현황(마지막 관측)은 오늘 노선별 jsonl 에서. 쿼터는 키(env명) 단위 카운터.
    """
    if subway_collector is None:
        return {"present": False}
    now = datetime.now(O.KST)
    day = subway_collector.service_day(now)
    qday = subway_collector.quota_day(now)
    tgt = O.cfg().get("subwayTarget", 3)   # ⚠️ 버스(7)와 다르다 — §8.1 ④: 쌍당 3 관측이면 σ
    cap = subway_collector.DAILY_CAP
    c = db()

    # 노선은 관측된 것에서 자동 발견된다 — 일괄(ALL) 수집이라 config 에 노선 목록이 없다
    # 판정 준비도 (§8.1 ④) — (열차,역) 쌍당 평균 관측 일수, **요일 무관**.
    # 셀 충족(7주)과 다른 신호다: 정시성 σ 를 재는 데는 쌍당 3~5 관측이면 되므로
    # 이걸 봐야 판정 시점을 안 놓친다. Σn ÷ 서로 다른 (열차,역) 쌍 수.
    judge = {}
    with _DB_LOCK:                # 연결을 공유하므로 커서 소진까지 락을 쥔다
        rows = c.execute(
            """SELECT line, COALESCE(SUM(n),0), COUNT(DISTINCT trainNo || '|' || statnId)
               FROM subway_cell GROUP BY line""").fetchall()
    for name, sn, pairs in rows:
        judge[name] = round(sn / pairs, 1) if pairs else 0

    # fillN = Σmin(n, 목표) — 진행률의 분자다.
    # ⚠️ filled(n>=목표)를 진행률로 쓰면 7주차 전까지 **수학적으로 0%** 다:
    #    지하철 셀은 요일별로 주 1회씩만 차서 6주차에도 전 셀이 n<7 → 0%,
    #    그러다 7주차에 한꺼번에 100% 로 점프한다 (✅ 실측: 하루 뒤 3,671셀
    #    전부 n=1 → 0.0%). "멈춘 것"과 "차는 중"이 구분되지 않는다.
    #    min 으로 자르는 건 초과 관측이 빈 셀을 가리지 못하게 하려는 것.
    agg = {}
    with _DB_LOCK:
        rows = c.execute(
            """SELECT line, daytype, COUNT(*), COALESCE(SUM(n>=?),0),
                      COUNT(DISTINCT trainNo), COUNT(DISTINCT statnId), COALESCE(SUM(n),0),
                      COALESCE(SUM(MIN(n,?)),0)
               FROM subway_cell GROUP BY line, daytype""", (tgt, tgt)).fetchall()
    for name, dtp, s, f2, tr, st, sn, fn in rows:
        a = agg.setdefault(name, {"seen": 0, "filled": 0, "sumN": 0, "fillN": 0, "by": {},
                                  "trains": 0, "stations": 0})
        a["by"][dtp] = {"seen": s, "filled": f2, "sumN": sn, "fillN": fn,
                        "days": round(sn / s, 1) if s else 0}   # 셀당 평균 관측 일수
        a["seen"] += s
        a["filled"] += f2
        a["sumN"] += sn
        a["fillN"] += fn
        a["trains"] = max(a["trains"], tr)
        a["stations"] = max(a["stations"], st)

    # 오늘 jsonl — 노선별 기록 수와 마지막 관측 (일괄이라 파일 하나).
    # ⚠️ 매 /api(5초)마다 전체를 다시 읽지 않는다 — 일괄 수집은 하루 수만 행이라
    #    풀스캔이면 t4g.micro 에서 /api 가 초 단위로 늘어진다. 지난 호출 이후
    #    추가된 바이트만 이어 읽어 누적한다 (파일 교체/축소 시엔 처음부터).
    path = os.path.join(O.DATA, f"{subway_collector.PREFIX}-{day}.jsonl")
    #    /api 는 동시에 여러 스레드에서 오므로 이어읽기 전체를 락으로 감싼다 —
    #    안 그러면 두 스레드가 같은 구간을 읽어 카운트가 이중 가산된다.
    cache = _SUB_TAIL
    with _SUB_LOCK:
        if cache.get("path") != path:
            cache.update(path=path, offset=0, per_line={}, total=0,
                         last_t=None, last_stn=None, last_line=None)
        try:
            size = os.path.getsize(path)
            if size < cache["offset"]:
                cache.update(offset=0, per_line={}, total=0)
            if size > cache["offset"]:
                with open(path, "rb") as fh:
                    fh.seek(cache["offset"])
                    for raw in fh:
                        if not raw.endswith(b"\n"):
                            break          # 쓰다 만 마지막 줄 — 다음 호출에서 마저 읽는다
                        cache["offset"] += len(raw)
                        cache["total"] += 1
                        try:
                            r = json.loads(raw.decode("utf-8"))
                        except (ValueError, UnicodeDecodeError):
                            continue
                        ln = r.get("line")
                        cache["per_line"][ln] = cache["per_line"].get(ln, 0) + 1
                        cache["last_t"], cache["last_stn"], cache["last_line"] = \
                            r.get("t"), r.get("statnNm"), ln
        except OSError:
            pass
        # 락 안에서 스냅샷을 뜬다 — per_line 딕트를 그대로 넘기면 다음 호출이
        # 같은 객체를 갱신하는 중에 직렬화될 수 있다
        per_line_written, total_written = dict(cache["per_line"]), cache["total"]
        last_stn, last_line, last_t = cache["last_stn"], cache["last_line"], cache["last_t"]
    last_epoch = None
    if last_t:
        try:
            last_epoch = datetime.fromisoformat(last_t).timestamp()
        except ValueError:
            pass

    keys = [{"id": kid, "calls": subway_collector.read_calls(kid, qday), "cap": cap}
            for kid in (O.cfg().get("subwayKeys") or ["SEOUL_SUBWAY_KEY"])]
    lines = sorted(
        ({"name": n, "trains": a["trains"], "stations": a["stations"],
          "seen": a["seen"], "filled": a["filled"], "sumN": a["sumN"],
          "fillN": a["fillN"], "target": tgt,
          "judgeDays": judge.get(n, 0),
          # 셀 모델이 성립하지 않는 노선 — 준비도를 계산해도 의미가 없다 (config 주석 참조)
          "judgeSkip": n in (O.cfg().get("subwayJudgeExclude") or []),
          "byDay": {d: a["by"].get(d, {"seen": 0, "filled": 0, "sumN": 0, "fillN": 0, "days": 0})
                    for d in DAYS7},
          "written": per_line_written.get(n, 0)}
         for n, a in agg.items()),
        key=lambda x: -x["seen"])
    return {
        "present": True,
        "started": total_written > 0 or bool(agg) or any(k["calls"] for k in keys),
        "inService": subway_collector.in_service(now),
        "keys": keys, "lines": lines, "written": total_written,
        "lastObs": last_epoch, "lastStn": last_stn, "lastLine": last_line,
        "judgeTarget": 3,   # §8.1 ④ — 쌍당 3 관측이면 σ 를 잴 수 있다
        "target": tgt,      # 요일 분리 재검토 목표 (셀당 3 관측 = 3주)
        # §8 #1 은 이미 판정됨 (§8.2) — 대시보드가 '진행 중 목표'로 오인하지 않게 결과를 실어 보낸다
        "judged": {"done": True, "sigmaMed": 0.75, "signal": 1.5, "belowSignal": 0.75,
                   "date": "2026-07-23", "cells": 99082},
    }


# ── 커버리지 집계 ──────────────────────────────────────────────────
def snapshot():
    k = O.cfg()
    c = db()
    bands = k["timebands"]
    nb, tgt = len(bands), k["targetSamples"]

    # 구간수는 노선별 max(nstops-1, 0) 의 합 — SUM(nstops)-COUNT(*) 로 하면 조회
    # 실패로 nstops=0 인 노선이 분모를 -1 씩 깎는다 (fetch_routes 의 산식과 동일하게).
    with _DB_LOCK:
        routes = c.execute(
            "SELECT COUNT(*), COALESCE(SUM(MAX(nstops-1,0)),0) FROM route").fetchone()
    nroute, nseg = routes
    # 요일 7종을 전부 분리한 뒤로 토·일은 더 이상 '병목'이 아니다 — 월~일 모두
    # 주 1회씩만 채워지는 동등한 처지다. 그래서 완성률 분모도 7요일 전체로 본다.
    ndays = 7
    day_goal = nseg * nb           # 요일 하나 기준 — 요일별 진행률의 분모
    goal = day_goal * ndays        # 전체 목표 셀 (구간 × 밴드 × 7요일)

    # 한 번의 스캔으로 — 백그라운드 캐시가 계산하므로 셀 수백만이어도 괜찮다.
    # ★ 완료 = n>=target AND n_days>=minDays (같은 날 완주 방지 — 외부 리뷰)
    md = k.get("minDays", 2)
    with _DB_LOCK:
        seen, total, done = c.execute(
            "SELECT COUNT(*), COALESCE(SUM(n),0), COALESCE(SUM(n >= ? AND n_days >= ?),0) FROM cell",
            (tgt, md)).fetchone()

    # 밴드별 — 오늘(운행일 기준) 요일만 본다. 자정~04시엔 전날 요일이 '오늘'이다
    # (01:30 관측은 전날 막차 — day_type 이 운행일 경계로 처리한다).
    now = datetime.now(O.KST)
    today = O.day_type(now)
    nowband = O.band_of(now, bands)
    with _DB_LOCK:
        byband = {b: (n, cells) for b, n, cells in c.execute(
            "SELECT band, SUM(n), COUNT(*) FROM cell WHERE daytype=? GROUP BY band",
            (today,)).fetchall()}
    band_rows = []
    band_need = nseg * tgt  # 이 요일·밴드에서 채워야 할 관측 수 = 구간수 × 목표샘플
    for i, (a, b) in enumerate(bands):
        n, cells = byband.get(i, (0, 0))
        band_rows.append({
            "i": i, "from": a, "to": b if b <= 24 else b - 24, "wrap": b > 24,
            "obs": n, "cells": cells, "need": band_need,
            "pct": n / band_need if band_need else 0,
            "peak": (a, b) in ((7, 9), (17, 20)),
        })

    # 요일별 — 분모는 요일 하나 기준(day_goal). 단일 GROUP BY 로 한 번에
    # (요일마다 별도 COUNT 쿼리를 치면 스캔 7회 — 셀이 커지면 /api 가 느려진다).
    byday = {}
    with _DB_LOCK:
        day_rows = c.execute(
            """SELECT daytype, SUM(n), COUNT(*),
                      SUM(n >= ? AND n_days >= ?),
                      COALESCE(SUM(MIN(n,?)),0),
                      COALESCE(SUM(MIN(n_days,?)),0),
                      COALESCE(SUM(n >= ?),0),
                      COALESCE(SUM(n_days >= ?),0)
               FROM cell GROUP BY daytype""",
            (tgt, md, tgt, md, tgt, md)).fetchall()
    for d, n, cells, full, fill_n, day_fill_n, sample_ready, date_ready in day_rows:
        byday[d] = {
            "obs": n, "cells": cells, "done": full,
            "pct": full / day_goal if day_goal else 0,
            # 이진 완료율은 2주차 전까지 0에 가까워 고장처럼 보인다. 표본 건수와
            # 날짜 다양성이 각각 얼마나 차는지 연속 진행률도 함께 제공한다.
            "fillN": fill_n,
            "fillPct": fill_n / (day_goal * tgt) if day_goal else 0,
            "dayFillN": day_fill_n,
            "dayFillPct": day_fill_n / (day_goal * md) if day_goal else 0,
            "sampleReady": sample_ready,
            "dateReady": date_ready,
        }

    # 쿼터는 달력일 키다 (data.go.kr 자정 리셋) — 운행일(service_day)이 아니다
    calls = bus_collector.read_calls(bus_collector.quota_day(datetime.now(O.KST))) if bus_collector else 0

    with LOCK:
        st = dict(STATE)
        st["errors"] = dict(STATE["errors"])
        st["errLog"] = list(STATE.get("errLog", []))  # 락 밖 직렬화 중 수집기 append 와 경합 방지

    # 남은 기간 — 밴드별 천장 도달 시점의 [최소, 최대] 범위.
    #
    # 속도: 최근 1~2 운행일 jsonl 크기 기반 (⚠️ 이전 판 버그 — total(역대 전체) ÷
    # elapsed(재시작 후)라서 재시작마다 뻥튀기, 실제 ~100일이 24.9일로 표시됐다).
    #
    # 구조: (밴드, 요일) 셀은 그 요일(주 1회)에만 채워지고, 운행 구조상 천장이 있다
    # (새벽·심야 ~82%, 낮 ~90% — 운행 안 하는 시간대의 셀은 영원히 빈다).
    # 밴드마다 "천장까지 몇 주"를 계산해 범위를 만든다. 단 안정화 두 겹:
    #  ① 측정 창이 1일 미만이면(방금 시작) '측정 중' — per_day 가 몇 시간치라 요동친다.
    #  ② 상한은 중앙값의 3배 이내 밴드의 최대치 — 데이터가 거의 없는 밴드 하나가
    #     max 를 305일처럼 튀게 하던 것을 자른다 (밴드 7개라 퍼센타일은 부정확).
    eta = eta_hi = None
    eta_measuring = False
    per_day, win_days = _obs_rate_per_day()
    if per_day and win_days < 1.0:
        eta_measuring = True                     # 표본은 있으나 창이 얇다
    elif per_day:
        with _DB_LOCK:
            band_tot = dict(c.execute(
                "SELECT band, SUM(n) FROM cell GROUP BY band").fetchall())
        tot_all = sum(band_tot.values())
        need_bd = nseg * tgt                     # (밴드, 요일) 하나의 필요 관측 수
        CEIL = {0: 0.82, nb - 1: 0.82}           # 04-07·20-04시 천장 — 나머지 0.90
        weeks = []
        for i in range(nb):
            share = band_tot.get(i, 0) / tot_all if tot_all else 0  # 하루 관측 중 이 밴드 비중(실측 근사)
            avg_pct = band_tot.get(i, 0) / (need_bd * 7)            # 7요일 평균 진행률
            ceil = CEIL.get(i, 0.90)
            if share > 0.02 and avg_pct < ceil:  # share≤2% 밴드는 표본 부족 — 제외
                weekly_gain = per_day * share / need_bd             # 요일 하나 기준 주당 증가
                weeks.append((ceil - avg_pct) / weekly_gain)
        if weeks:
            weeks.sort()
            med = weeks[len(weeks) // 2]
            cap = med * 3                                          # 이상치 컷 — 중앙값 3배
            hi = max([w for w in weeks if w <= cap], default=weeks[0])
            eta, eta_hi = weeks[0] * 7, hi * 7                     # 일 단위
            # 날짜 다양성 완료조건의 물리적 하한. 같은 daytype은 주 1회만 오므로
            # minDays개의 서로 다른 날짜를 채우는 데 최소 minDays주가 필요하다.
            # 관측량 기반 ETA가 이보다 작게 나와도 달력상 달성할 수 없다.
            date_floor = md * 7
            eta, eta_hi = max(eta, date_floor), max(eta_hi, date_floor)

    # 공휴일 — 오늘이 공휴일이면 관측은 쌓이되 장부엔 안 들어간다. 그 사실이
    # 화면에 없으면 "수집은 도는데 진행률이 안 오른다"로 보여 고장으로 오인한다.
    # ⚠️ 캐시만 읽는다(offline) — /api 는 5초마다 오므로 여기서 갱신을 시도하면
    #    그 한 번이 대시보드를 수십 초 멈춘 것처럼 보이게 한다. 갱신은 수집 루프가 한다.
    try:
        hinfo = H.info()                    # network=False 기본
        sday = O.service_day_of(datetime.now(O.KST)).strftime("%Y-%m-%d")
        hset = O.holiday_set(offline=True)
        hol = {"count": len(hset), "source": hinfo["source"], "updated": hinfo["updated"],
               "today": sday in hset,
               "next": next((d for d in sorted(hset) if d >= sday), None)}
    except Exception:
        hol = {"count": 0, "source": None, "updated": None, "today": False, "next": None}

    return {
        "routes": nroute, "segments": nseg, "goal": goal, "dayGoal": day_goal,
        "dayNeed": nseg * nb * tgt,  # 요일 하나의 필요 관측 건수 = 밴드별 need × 밴드 수
        "target": tgt, "minDays": md,
        "done": done, "seen": seen, "total": total,
        "pct": done / goal if goal else 0,
        "bands": band_rows, "days": byday, "today": today, "nowBand": nowband,
        "calls": calls, "quota": k["dailyQuota"], "holidays": hol,
        "maxRoutes": k.get("maxRoutes", 170),
        "busKeys": len([n for n in (k.get("busKeys") or ["GBIS_BUS_KEY"])
                        if bus_collector and bus_collector._load_key(n)]) if bus_collector else 1,
        "state": st, "etaDays": eta, "etaDaysHi": eta_hi, "etaMeasuring": eta_measuring,
        "subway": subway_snapshot(),
        "seoul": seoul_snapshot(),
        "cfg": {kk: k[kk] for kk in
                ("targetSamples", "maxRoutes", "dispatchRate",
                 "intervalSec", "serviceWindow", "dailyQuota")},
    }


# ── 대시보드 ──────────────────────────────────────────────────────
PAGE = """<!doctype html><meta charset="utf-8"><title>수집 현황</title>
<meta name=viewport content="width=device-width,initial-scale=1">
<style>
 :root{color-scheme:light dark}
 body{font:14px/1.6 -apple-system,BlinkMacSystemFont,"Apple SD Gothic Neo",sans-serif;
      margin:0;padding:24px;max-width:900px;margin:0 auto}
 h1{font-size:18px;margin:0 0 4px} h2{font-size:14px;margin:28px 0 10px;opacity:.7}
 .sub{opacity:.55;font-size:12px;margin-bottom:20px}
 .big{font-size:34px;font-weight:700;letter-spacing:-.02em}
 .row{display:flex;align-items:center;gap:10px;margin:5px 0}
 .lbl{width:88px;font-variant-numeric:tabular-nums;opacity:.75;font-size:13px}
 .bar{flex:1;height:14px;background:#8883;border-radius:3px;overflow:hidden}
 .fill{height:100%;background:#3b82f6;transition:width .4s}
 .fill.ok{background:#22c55e} .fill.warn{background:#f59e0b} .fill.bad{background:#ef4444}
 .val{width:200px;text-align:right;font-variant-numeric:tabular-nums;font-size:12px;opacity:.7;white-space:nowrap}
 .tag{font-size:10px;padding:1px 5px;border-radius:3px;background:#8882;margin-left:4px}
 .warn{color:#f59e0b} .bad{color:#ef4444} .ok{color:#22c55e}
 table{border-collapse:collapse;width:100%;font-size:13px}
 td{padding:4px 8px 4px 0} td.n{text-align:right;font-variant-numeric:tabular-nums}
 .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:14px;margin:14px 0}
 .card{background:#8881;border-radius:8px;padding:12px}
 .card .k{font-size:11px;opacity:.6} .card .v{font-size:20px;font-weight:600;margin-top:2px}
 code{background:#8882;padding:1px 4px;border-radius:3px;font-size:12px}
</style>
<h1>수집 현황</h1>
<div class=sub id=sub>…</div>
<div id=app>불러오는 중…</div>
<script>
const pct = x => (x*100).toFixed(1)+'%';
// 초기 엄격 완료율은 0.01%보다 작을 수 있다. 0이 아닌 값을 0.0%로 숨기지 않는다.
const pctFine = x => x>0 && x<0.001 ? (x*100).toFixed(3)+'%' : pct(x);
const num = x => (x||0).toLocaleString();
function bar(p, cls){ return `<div class=bar><div class="fill ${cls||''}" style="width:${Math.min(100,p*100)}%"></div></div>`; }

let S = null, tab = 'bus', lineTab = '__all__';  // S: 최근 /api · tab: 상단 탭 · lineTab: 지하철 노선 탭
window.setTab = (t) => { tab = t; render(); };
window.setLineTab = (t) => { lineTab = t; render(); };

async function tick(){
  // ⚠️ 요청 완료 후 다음을 예약한다 (setInterval 금지) — /api 가 느릴 때
  //    앞 요청이 끝나기 전에 다음이 겹쳐 서버 스레드가 쌓이는 걸 막는다.
  try {
    S = await (await fetch('/api')).json();
    if(!S.warming) render();
  } catch(e) { /* 일시적 실패는 다음 tick 에 회복 */ }
  setTimeout(tick, 5000);
}

function render(){
  const d = S; if(!d) return;
  // 헤더는 **세 수집기 공통** — 어느 하나가 멈추면 탭을 안 열어도 보이게.
  // 경기 전용 규모(노선·구간·목표셀)는 그 탭 안으로 옮겼다. 탭이 셋이 된 뒤로
  // 전역 헤더가 경기 것만 이고 있으면 다른 탭에서 틀린 맥락을 준다.
  const now = Date.now()/1000;
  const dot = (ok, label) => `<span class=${ok?'ok':'bad'}>●</span> ${label}`;
  const sub = d.subway || {}, seo = d.seoul || {};
  const parts = [dot(d.state.lastObs && now - d.state.lastObs < 180, '경기')];
  if(sub.present) parts.push(dot(sub.lastObs && now - sub.lastObs < 300, '지하철'));
  if(seo.present) parts.push(dot(seo.lastObs && now - seo.lastObs < 600, '서울'));
  document.getElementById('sub').innerHTML = parts.join(' · ');

  const tb = (id,label) => `<span onclick="setTab('${id}')" style="cursor:pointer;padding:6px 16px;`
    + `border-bottom:2px solid ${tab===id?'#3b82f6':'transparent'};${tab===id?'font-weight:700':'opacity:.5'}">${label}</span>`;
  let h = `<div style="display:flex;gap:8px;border-bottom:1px solid #8883;margin:0 0 18px">${tb('bus','경기 버스')}${tb('subway','지하철')}${tb('seoul','서울 버스')}</div>`;
  h += (tab === 'subway') ? renderSubway(d) : (tab === 'seoul') ? renderSeoul(d) : renderBus(d);
  document.getElementById('app').innerHTML = h;
  paintObs();
}

function renderBus(d){
  const s = d.state;
  let h = '';
  // 규모 — 전역 헤더에 있던 것을 이 탭으로 옮겼다 (경기 전용 정보라서)
  const nkey = d.busKeys || 1;
  h += `<div class=sub>TAGO 위치정보 30초 폴링 · 경기 <b>${num(d.routes)}</b>노선 · `
     + `${num(d.segments)}구간 · 목표 ${num(d.goal)}셀 × ${d.target}샘플 (구간 × 밴드 7 × 요일 7)`
     + `<br>키 <b>${nkey}개</b>(계정 독립 세션·쿼터) · 사이클당 최대 <b>${num(d.maxRoutes||170)}</b>노선 폴링`
     + (nkey>=2 ? ' — 세션·rate 가 키 단위라 커버 속도 ×'+nkey : '') + `</div>`;
  // 완성률
  h += `<div class=big>${pctFine(d.pct)}</div>`;
  const etaTxt = d.etaMeasuring ? ' · 남은 기간 <b>측정 중</b> (관측 하루치 쌓이면 표시)'
    : d.etaDays!=null ? ` · 남은 기간 약 <b>${Math.round(d.etaDays)}~${Math.round(d.etaDaysHi)}일</b>`
       + ` (${(d.etaDays/7).toFixed(0)}~${(d.etaDaysHi/7).toFixed(0)}주 · 밴드별 천장 도달 기준)` : '';
  h += `<div class=sub>${num(d.done)} / ${num(d.goal)} 셀 충족${etaTxt}</div>`;
  h += bar(d.pct);

  // 건강
  const q = d.calls/d.quota;
  const errN = Object.values(s.errors).reduce((a,b)=>a+b,0);
  const errRate = s.picked ? errN/s.picked : 0;
  // 쿼터는 **계정 단위**(각 50만 하드리밋). 합산 카운터라 균등 분배(fetch_all)를
  // 전제로 계정당 = 합산/키수 로 환산해 보여준다. 계정 하나라도 50만에 부딪히면
  // 그 계정 몫이 멈추므로, 계정당 값이 진짜 벽에 가까운지가 중요하다.
  const perAcct = nkey>=2 ? d.calls/nkey : d.calls;
  const perQ = nkey>=2 ? d.quota/nkey : d.quota;
  const qa = perAcct/500000;   // 계정당 하드리밋 50만 대비
  h += '<h2>건강 상태</h2><div class=grid>';
  h += `<div class=card><div class=k>쿼터 ${nkey>=2?'(계정당)':''}</div>
        <div class="v ${qa>.97?'bad':qa>.9?'warn':''}">${num(Math.round(perAcct))}</div>
        <div class=k>/ ${num(Math.round(perQ))} 사용상한 · 하드 500k${nkey>=2?` · 합산 ${num(d.calls)}/${num(d.quota)}`:''}</div></div>`;
  h += `<div class=card><div class=k>실패율</div><div class="v ${errRate>.05?'bad':errRate>.02?'warn':'ok'}">${pct(errRate)}</div>
        <div class=k>${Object.entries(s.errors).map(([k,v])=>k+'×'+v).join(' ')||'없음'}</div></div>`;
  h += `<div class=card><div class=k>마지막 관측</div><div class=v id=lastobs>—</div>
        <div class=k>사이클 ${s.lastCycleSec?s.lastCycleSec.toFixed(0)+'s':'—'} · ${s.picked}노선${s.night?' (심야)':''}</div></div>`;
  h += `<div class=card><div class=k>총 관측</div><div class=v>${num(d.total)}</div>
        <div class=k>운행 ${num(s.moving)}대</div></div>`;
  // 공휴일 — 오늘이면 관측은 쌓이되 **장부엔 안 들어간다**. 이걸 안 보이게 두면
  // "수집은 도는데 진행률이 안 오른다"가 고장으로 오인된다.
  const HOL = d.holidays || {};
  h += `<div class=card><div class=k>공휴일 (장부 제외)</div>
        <div class="v ${HOL.today?'warn':''}">${HOL.today?'오늘 해당':'평상일'}</div>
        <div class=k>${HOL.count||0}일 등록 · 출처 ${HOL.source||'없음'}${HOL.next?' · 다음 '+HOL.next:''}</div></div>`;
  h += '</div>';
  if(HOL.today)
    h += '<div class=sub style="color:#f59e0b">⚠️ 오늘은 공휴일 — 관측은 그대로 jsonl 에 쌓이지만 '
       + '장부(셀)에는 넣지 않는다. 휴일 다이어라 요일 표본을 오염시키기 때문. '
       + '진행률이 안 오르는 것이 정상이다.</div>';

  // 밴드 — 지금 채워지는 요일의 **누적** (모든 주의 해당 요일 합).
  const KO = {mon:'월',tue:'화',wed:'수',thu:'목',fri:'금',sat:'토',sun:'일'};
  h += `<h2>밴드별 — <b>${KO[d.today]||'?'}요일</b> 누적 (모든 주 합산 · 04시에 다음 요일로 전환)</h2>`;
  for(const b of d.bands){
    const label = `${String(b.from).padStart(2,'0')}-${String(b.to).padStart(2,'0')}시`;
    const cur = d.nowBand!=null && b.i===d.nowBand;
    h += `<div class=row><div class=lbl>${label}${b.peak?'<span class=tag>첨두</span>':''}${b.wrap?'<span class=tag>익일</span>':''}${cur?'<span class=tag style="color:#3b82f6">진행 중</span>':''}</div>`
       + bar(b.pct, b.pct>=1?'ok':'')
       + `<div class=val>${num(b.obs)} / ${num(b.need)} · <b>${pct(b.pct)}</b></div></div>`;
  }

  // 요일 — 7종 전부 분리
  h += `<h2>요일별 완성률 (전부 분리)</h2>
        <div class=sub>엄격 완료 = 셀당 관측 ${d.target}건 + 서로 다른 같은 요일 ${d.minDays||2}일.
        첫 주에는 표본이 쌓여도 날짜 조건 때문에 충족 셀이 0인 것이 정상이다.
        아래 표본·날짜 충전률로 실제 진행을 함께 본다.</div><table>`;
  for(const [k,label] of [['mon','월'],['tue','화'],['wed','수'],['thu','목'],['fri','금'],['sat','토'],['sun','일']]){
    const v = d.days[k] || {obs:0,done:0,cells:0,pct:0,fillPct:0,dayFillPct:0};
    h += `<tr><td width=40>${label}</td>
          <td class=n width=72><b>${pctFine(v.pct)}</b></td>
          <td width=250>${bar(v.pct, v.pct>=1?'ok':'')}</td>
          <td class=n style="white-space:nowrap">충족 셀 ${num(v.done)} / ${num(d.dayGoal)}</td>
          <td class=n style="white-space:nowrap;opacity:.75">표본 ${pct(v.fillPct||0)}</td>
          <td class=n style="white-space:nowrap;opacity:.75">날짜 ${pct(v.dayFillPct||0)}</td>
          <td class=n style="white-space:nowrap;opacity:.6">관측 ${num(v.obs)}건</td></tr>`;
  }
  h += '</table>';

  // 오류 로그 — 재시도 후 잔여 실패만 · 매일(운행일 경계) 초기화
  const log = (s.errLog||[]);
  h += `<h2>오류 로그 — 오늘 잔여 실패 (${log.length}건 · 매일 초기화)</h2>`;
  if(!log.length){
    h += '<div class=sub style="color:#22c55e">오늘 실패 없음</div>';
  } else {
    h += '<table>';
    for(const e of log.slice().reverse()){
      const t = new Date(e.t*1000).toLocaleTimeString('ko-KR',{hour12:false});
      const rate = e.picked ? (e.n/e.picked*100).toFixed(1) : '0';
      h += `<tr><td width=80 style="opacity:.6">${t}</td>`
         + `<td width=90 class=bad>실패 ${e.n}/${e.picked}</td>`
         + `<td width=44 class=n>${rate}%</td>`
         + `<td style="opacity:.8">${e.detail}</td></tr>`;
    }
    h += '</table>';
  }

  // 설정
  h += '<h2>설정 (읽기 전용 — config.json)</h2><table>';
  for(const [k,v] of Object.entries(d.cfg))
    h += `<tr><td width=140><code>${k}</code></td><td>${JSON.stringify(v)}</td></tr>`;
  h += '</table>';
  return h;
}

function renderSeoul(d){
  const s = d.seoul;
  if(!s || !s.present)
    return '<div class=sub>이 서버엔 서울 수집기가 없다 (seoul_collector 미탑재).</div>';
  if(!s.started)
    return '<div class=sub>노선 목록이 아직 없다 — <code>python3 seoul_collector.py --routes</code> 로 먼저 받을 것.</div>';
  const q = s.cap ? s.calls/s.cap : 0;
  const needPct = s.need ? s.calls/s.need : 0;
  let h = '<div class=sub><b>도착정보 스냅샷</b> — 경기(TAGO)와 방식이 다르다. 서울은 운영사가 '
    + '구간시간·배차를 <b>이미 계산해서</b> 주므로 30초 폴링이 아니라 <b>밴드마다 노선당 1회</b>만 찍는다 '
    + `(${num(s.routes)}노선 × 밴드 7 = ${num(s.need)}콜/일, 한도의 ${pct(s.need/s.cap)}). `
    + 'arrmsg1/arrmsg2 의 간격이 <b>실측 배차</b>이고 그것이 §6.4 의 CV — 모르면 20분 배차에서 10분 오차다.</div>';

  h += '<h2>건강 상태</h2><div class=grid>';
  h += `<div class=card><div class=k>마지막 관측</div><div class=v id=seoullastobs>—</div>
        <div class=k>${s.lastNo||''} ${s.lastStn||'—'}</div></div>`;
  h += `<div class=card><div class=k>오늘 기록</div><div class=v>${num(s.written)}</div>
        <div class=k>정류장 × 노선 행</div></div>`;
  h += `<div class=card><div class=k>오늘 콜</div><div class="v ${q>.95?'bad':q>.85?'warn':''}">${num(s.calls)}</div>
        <div class=k>/ ${num(s.cap)} 상한 · 필요 ${num(s.need)}</div></div>`;
  h += `<div class=card><div class=k>하루 진행</div><div class="v ${needPct>=1?'ok':''}">${pct(Math.min(needPct,1))}</div>
        <div class=k>밴드 7종 × ${num(s.routes)}노선</div></div>`;
  h += '</div>';

  h += '<h2>밴드별 진행 (매일 0에서 시작)</h2><table>';
  for(const b of s.bands){
    h += `<tr><td width=90>${b.from}-${b.to}시${b.wrap?'<span style="opacity:.5">익일</span>':''}
          ${b.cur?'<span style="color:#3b82f6;font-size:11px"> 진행 중</span>':''}</td>
          <td class=n width=54><b>${pct(b.pct)}</b></td>
          <td width=240>${bar(b.pct, b.pct>=1?'ok':'')}</td>
          <td class=n style="white-space:nowrap">${num(b.done)} / ${num(b.total)}</td></tr>`;
  }
  h += '</table>';
  h += '<div class=sub style="margin-top:6px">밴드마다 전 노선을 한 번씩 찍으면 100%. '
     + '⚠️ 이건 <b>관측이 아니라 운영사 예측</b>이라(신분당선 recptnDt 와 같은 성격, §8.1 ⑤ 가) '
     + '정시성 판정엔 쓰지 않는다. CV 는 다음 2대의 현재 위치 기반이라 예측 오염이 상대적으로 작다.</div>';
  return h;
}

function renderSubway(d){
  const sub = d.subway;
  if(!sub || !sub.present)
    return '<div class=sub>이 서버엔 지하철 수집기가 없다 (subway_collector 미탑재).</div>';
  if(!sub.started)
    return '<div class=sub>지하철 수집기가 아직 안 돎 — <code>.env</code> 에 config.subwayKeys의 '
      + 'SEOUL_SUBWAY_KEY~KEY5를 넣고 '
      + 'systemd 유닛(findpath-subway)을 걸 것. §8 #1(지하철 정시성)이 프로젝트 존폐 항목이라 우선순위가 높다.</div>';
  const KOD = {mon:'월',tue:'화',wed:'수',thu:'목',fri:'금',sat:'토',sun:'일'};
  const D7 = ['mon','tue','wed','thu','fri','sat','sun'];
  const alive = sub.lastObs && (Date.now()/1000 - sub.lastObs) < 300;
  const tot = sub.lines.reduce((a,L)=>({seen:a.seen+L.seen, filled:a.filled+L.filled,
    sumN:a.sumN+(L.sumN||0), fillN:a.fillN+(L.fillN||0)}), {seen:0,filled:0,sumN:0,fillN:0});
  // ★ 목표가 바뀌었다. §8 #1(정시성)은 **이미 판정됨**(§8.2, σ 0.75분<신호).
  //   그래서 남은 목표는 '7주 셀 충족'이 아니라 **요일 분리 재검토** — 같은 요일이
  //   3번(=3주) 와서 (노선,열차,역,요일)당 3관측이면 요일별 σ 를 잴 수 있다(§8.1 ④).
  //   목표를 7→3 으로 내렸다(subwayTarget). 진행률 = Σmin(n,3)/(셀 수 × 3),
  //   각 요일이 세 번씩 오면 100% (지금은 대개 1관측 → 33%).
  const tgtAll = sub.lines.length ? sub.lines[0].target : 3;
  const rate = tot.seen ? tot.fillN/(tot.seen*tgtAll) : 0;
  const prog = L => L.seen ? (L.fillN||0)/(L.seen*L.target) : 0;
  const days = L => L.seen ? (L.sumN||0)/L.seen : 0;      // 셀당 평균 관측 일수
  let h = '<div class=sub><b>전 노선 일괄(ALL)</b> 도착정보 — 1콜에 19노선·555역 (✅ 경기·인천 포함). '
    + '<b>B안 셀</b> (노선,열차,역,요일)별 관측 일수 — trainNo 가 매일 반복이라 요일별 며칠이면 '
    + '각 열차 정시성 σ 가 나온다 (docs §8.2).</div>';

  // §8 #1 판정 결과 — 존폐 항목이었고 통과했다. 진행 중 목표가 아니라 완료다.
  const jg = sub.judged;
  if(jg && jg.done){
    h += `<div class=card style="margin:12px 0;border-left:3px solid #22c55e;background:#22c55e11">
      <div class=k>§8 #1 지하철 정시성 — <b class=ok>✅ 판정됨</b> (${jg.date})</div>
      <div style="margin-top:4px">σ 중앙 <b>${jg.sigmaMed}분</b> &lt; 신호 ${jg.signal}분 · 신호보다 σ 작은 셀 <b>${pct(jg.belowSignal)}</b>
      <span class=sub>(${num(jg.cells)}셀 · 5일치)</span></div>
      <div class=sub style="margin-top:2px">§6.2가 버스를 기각한 논리가 지하철엔 적용 안 됨 → <b>절벽 성립, 차별점 유효.</b>
      아래는 <b>요일 분리 재검토</b>(3주+)의 진행률이다.</div></div>`;
  }

  h += '<h2>건강 상태</h2><div class=grid>';
  h += `<div class=card><div class=k>마지막 관측</div><div class=v id=sublastobs>—</div>
        <div class=k>${sub.lastLine||''} ${sub.lastStn||'—'}${sub.inService?'':' · 운행 밖'}</div></div>`;
  h += `<div class=card><div class=k>오늘 기록</div><div class=v>${num(sub.written)}</div>
        <div class=k>도착·출발 관측</div></div>`;
  h += `<div class=card><div class=k>노선</div><div class=v>${num(sub.lines.length)}</div>
        <div class=k>관측된 노선 수</div></div>`;
  const wk = (tot.seen ? tot.sumN/tot.seen : 0);   // 셀당 평균 관측 일수 = 대략 몇 주째
  h += `<div class=card><div class=k>요일 분리 재검토</div><div class="v ${rate>=1?'ok':''}">${pct(rate)}</div>
        <div class=k>셀당 ${wk.toFixed(1)}/${tgtAll}관측 · ${Math.ceil(Math.max(0,tgtAll-wk))}주 남음</div></div>`;
  const jt = sub.judgeTarget || 3;
  h += '</div>';

  // 키별 쿼터 — 라운드로빈이라 고르게 소진돼야 정상
  h += '<h2>키별 쿼터 (달력일 · 라운드로빈)</h2><table>';
  for(const K of (sub.keys||[])){
    const q = K.calls/K.cap;
    h += `<tr><td width=190><code>${K.id}</code></td>
          <td class=n width=54><b>${pct(q)}</b></td>
          <td width=240>${bar(q, q>.95?'bad':q>.85?'warn':'')}</td>
          <td class=n style="white-space:nowrap">${num(K.calls)} / ${num(K.cap)}</td></tr>`;
  }
  h += '</table>';

  // 노선 탭 — 전체 요약 / 노선 하나 상세 (요일 7종)
  const tgt = sub.lines.length ? sub.lines[0].target : 7;
  const lt = (id,label,extra) => `<span onclick="setLineTab('${id.replace(/'/g,"\\'")}')" `
    + `style="cursor:pointer;padding:3px 9px;margin:0 3px 4px 0;border-radius:5px;display:inline-block;`
    + `${lineTab===id?'background:#3b82f6;color:#fff;font-weight:600':'background:#8882'}">${label}${extra||''}</span>`;
  // ★ 노선별 지표는 **판정 준비도**다 — 충전율이 아니다.
  //   전 노선이 1콜(ALL)에 함께 수집되므로 '수집률'은 노선마다 같을 수밖에 없다.
  //   ✅ 실측: 19개 노선 전부 정확히 14.3%(=1/7). 아직 각 요일이 한 번씩만 왔고
  //   전 셀이 n=1 이라 fillN/(seen×7) = 1/7 로 고정된다 — 정보량이 0 이다.
  //   (다음 월요일에 mon 셀이 n=2 가 되면 오르지만, 그때도 19개가 나란히 오른다.)
  //   반면 준비도는 1.6~4.5일로 갈린다: 쌍당 관측 수라 trainNo 파편화가 드러난다.
  const jprog = L => Math.min(1, (L.judgeDays||0)/jt);
  h += `<h2>노선별 쌍당 관측 <span class=sub style="font-weight:400">— §8 #1 은 판정 완료(§8.2). 이 표는 노선별 데이터 두께·trainNo 파편화 점검용</span></h2>`;
  h += '<div style="margin-bottom:10px">' + lt('__all__','전체')
     + sub.lines.map(L=>lt(L.name, L.name, `<span style="opacity:.6;font-size:11px"> ${
         L.judgeSkip ? '—' : (L.judgeDays||0).toFixed(1)+'일'}</span>`)).join('') + '</div>';

  if(!sub.lines.length){
    h += '<div class=sub>아직 셀 없음 — 수집이 돌면 노선이 자동으로 나타난다. '
       + '(오늘 기록이 늘고 있는데 셀이 0이면 공휴일이거나 첫 사이클 전이다.)</div>';
  } else if(lineTab === '__all__'){
    h += '<table><tr style="opacity:.5"><td>노선</td><td class=n>쌍당 관측</td><td></td>'
       + '<td class=n>셀</td><td class=n>열차·역</td><td class=n>오늘</td></tr>';
    for(const L of sub.lines){
      const j = L.judgeDays||0, jp = jprog(L);
      h += `<tr><td width=90>${L.name}</td>
            <td class="n ${L.judgeSkip?'':(j>=jt?'ok':'')}" width=64 style="white-space:nowrap">${
              L.judgeSkip ? '<span style="opacity:.5">제외</span>' : '<b>'+j.toFixed(1)+'</b>/'+jt+'일'}</td>
            <td width=170>${L.judgeSkip
              ? '<span class=sub style="font-size:11px" title="trainNo 가 종일 재사용돼 셀이 성립하지 않는다">시각표 대조 필요 (§8.1 ⑤)</span>'
              : bar(jp, jp>=1?'ok':'')}</td>
            <td class=n style="white-space:nowrap;opacity:.6">${num(L.seen)}</td>
            <td class=n style="white-space:nowrap;opacity:.6">${L.trains}·${L.stations}</td>
            <td class=n style="white-space:nowrap;opacity:.6">${num(L.written)}</td></tr>`;
    }
    h += '</table>';
    h += `<div class=sub style="margin-top:6px"><b>쌍당 관측</b> = Σ관측일 ÷ 서로 다른 (열차,역) 쌍 수 `
       + `(<b>요일 무관</b>). ${jt}일이면 정시성 σ 를 잴 수 있어 <b>${jt}일째부터 §8 #1 판정이 가능</b>하다 `
       + `— §8 #1 은 이미 이 방식으로 판정됐다(§8.2).<br>`
       + `⚠️ <b>노선별 '수집률'은 여기 없다.</b> 전 노선이 1콜(ALL)에 함께 수집되므로 수집 노력이 노선마다 `
       + `같고, 요일 분리 재검토 진행률은 19개 노선 전부 같은 값이다(각 요일이 아직 한 번씩만 와서 전 셀 n=1). `
       + `노선을 가르는 것은 <b>쌍당 관측</b>이고, 낮은 노선은 trainNo 가 파편화돼 쌍이 부풀려진 `
       + `것이다 — 2호선은 한 번호가 노선의 8% 구간에서만 잡힌다(§8.1 ⑤ 다).</div>`;
  } else {
    const L = sub.lines.find(x=>x.name===lineTab);
    if(!L){ h += '<div class=sub>그 노선은 아직 관측되지 않았다.</div>'; }
    else {
      h += `<div class=sub><b>${L.name}</b> — 열차 ${num(L.trains)}대 · 역 ${num(L.stations)}개 · `
         + `오늘 기록 ${num(L.written)}건 · 요일 분리 재검토 <b>${pct(prog(L))}</b> `
         + `(${num(L.seen)}셀 · 3관측 완료 ${num(L.filled)}) · 셀당 ${days(L).toFixed(1)}/${L.target}관측</div>`;
      h += '<table><tr style="opacity:.5"><td>요일</td><td class=n>재검토%</td><td></td>'
         + '<td class=n>셀(3관측)</td><td class=n>관측 수</td></tr>';
      for(const dk of D7){
        const v = L.byDay[dk] || {seen:0,filled:0,fillN:0,days:0};
        const dp = v.seen ? (v.fillN||0)/(v.seen*L.target) : 0;
        h += `<tr><td width=40>${KOD[dk]}</td>
              <td class=n width=54><b>${pct(dp)}</b></td>
              <td width=180>${bar(dp, dp>=1?'ok':(dk==='sat'||dk==='sun')?'warn':'')}</td>
              <td class=n style="white-space:nowrap">${num(v.seen)} <span style="opacity:.55">(${num(v.filled)})</span></td>
              <td class=n style="white-space:nowrap;opacity:.75">${(v.days||0).toFixed(1)} / ${L.target}일</td></tr>`;
      }
      h += '</table>';
      h += `<div class=sub style="margin-top:6px">셀 = (열차, 역) 조합 · <b>관측 수</b> = 그 요일에 며칠 봤나 `
         + `(셀당 평균). 요일마다 주 1회씩이라 목표 ${tgt}관측 = ${tgt}주. 셀이 0인 요일은 아직 그 요일이 안 온 것. `
         + `이게 100% 면 요일별 σ 를 재검토할 표본이 찼다는 뜻(§8.2).</div>`;
    }
  }

  h += '<h2>판정 규칙 (docs §8.1)</h2>';
  h += '<div class=sub>① <b>시각 근접 매칭</b>으로 대조한다 — ✅ 실측상 실시간 <code>btrainNo</code>(숫자)와 '
    + '시각표 <code>열차코드</code>(S902/K802) 형식이 어긋나고 <b>신분당선 PDF엔 열차번호가 아예 없어</b> ID 조인이 불가능하다. '
    + '미매칭 비율도 같이 본다(그 자체가 "시각표를 안 지킨다"의 증거).<br>'
    + '② 시각은 <code>t</code>(동적 폴링, 보통 30~50초)가 아니라 <b><code>recptnDt</code></b>(BIS 수신, ~20초)를 쓴다 — '
    + '해상도가 신호보다 거칠면 못 잰다.<br>'
    + '③ 임계값은 배차 H 대비다: <b>오판율 ≈ 2σ/H</b> → σ≤0.05H 면 절벽 신뢰, σ≥0.3H 면 무의미. '
    + '"±30초"는 H=10분에서 나온 값이라 <b>노선별 H와 함께</b> 판정한다.<br>'
    + '⚠️ 측정 바닥 σ≈20초 > 첨두 기준선(15초) — H=5분 노선에선 <b>기각만 가능하고 증명은 불가</b>하다.</div>';
  return h;
}

// 마지막 관측 타이머 — /api 폴링(5초)과 별개로 1초마다 다시 그린다.
// 수집기가 요청 사이클 중이면 '데이터 요청 중' 태그를 함께 띄운다.
// (사이클의 대부분이 요청 시간이라, 타이머를 통째로 바꾸면 거의 항상 가려진다)
function paintObs(){
  if(!S) return;
  // 버스 — 사이클 30~45s 라 180s 넘으면 멈춘 것
  const el = document.getElementById('lastobs');
  if(el){
    const s = S.state;
    const secs = s.lastObs ? Math.round(Date.now()/1000 - s.lastObs) : null;
    el.className = 'v ' + (secs != null && secs < 180 ? 'ok' : 'bad');
    el.innerHTML = (secs != null ? secs + '초 전' : '—')
      + (s.fetching ? ' <span class=tag style="background:#3b82f622;color:#3b82f6">데이터 요청 중</span>' : '');
  }
  // 지하철 — 동적 폴링(현재 보통 30~50s)이므로 300s를 정지 임계로 둔다. 운행 시간 밖이면 회색
  const se = document.getElementById('sublastobs');
  if(se && S.subway){
    const sub = S.subway;
    const secs = sub.lastObs ? Math.round(Date.now()/1000 - sub.lastObs) : null;
    const alive = secs != null && secs < 300;
    se.className = 'v ' + (alive ? 'ok' : (sub.inService ? 'bad' : ''));
    se.textContent = secs != null ? secs + '초 전' : '—';
  }
  // 서울은 밴드당 노선 1회라 간격이 최대 30초 — 임계값을 넉넉히 잡는다
  // (버스 180s · 지하철 300s 와 다른 이유. 밴드를 다 찍으면 다음 밴드까지 쉰다).
  const qe = document.getElementById('seoullastobs');
  if(qe && S.seoul && S.seoul.present){
    const secs = S.seoul.lastObs ? Math.round(Date.now()/1000 - S.seoul.lastObs) : null;
    qe.className = 'v ' + (secs != null && secs < 600 ? 'ok' : '');
    qe.textContent = secs != null ? secs + '초 전' : '—';
  }
}
tick(); setInterval(paintObs, 1000);   // tick 은 스스로 setTimeout 으로 재예약
</script>
"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/api"):
            with _SNAP_LOCK:                 # 캐시만 즉시 반환 — 계산은 백그라운드
                body = _SNAP["data"]
            ct = "application/json; charset=utf-8"
        elif self.path == "/":
            body = PAGE.encode()
            ct = "text/html; charset=utf-8"
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", ct)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass  # 접근 로그 끔 — 수집 로그만 본다


class Tee:
    """콘솔에는 전부, 파일에는 중요한 줄만 — 윈도우 bat 용 (--log).

    셸 리다이렉트(>>)로 하면 스케줄러가 띄우는 cmd 창이 텅 비어 버린다.
    창에는 그대로 흐르고, 파일은 **사망 사인 확인용**이므로 오류·경고와
    시작/종료 표식만 남긴다 — 사이클 로그로 채우면 정작 필요한 줄이 묻힌다.
    stderr 는 전체 보존한다 (트레이스백은 여러 줄이고 표식이 없다).
    """
    KEEP = ("⚠️", "❌", "Traceback", "Error", "수집 시작", "대시보드", "종료 감지", "일 상한")

    def __init__(self, stream, f, keep_all=False):
        self.s, self.f, self.keep_all, self.buf = stream, f, keep_all, ""

    def write(self, x):
        self.s.write(x)
        self.buf += x
        while "\n" in self.buf:
            line, self.buf = self.buf.split("\n", 1)
            if self.keep_all or any(k in line for k in self.KEEP):
                self.f.write(line + "\n")

    def flush(self):
        self.s.flush()
        self.f.flush()


# ── 스냅샷 캐시 ────────────────────────────────────────────────────
# ⚠️ snapshot() 은 셀 수백만 규모 sqlite 집계 + jsonl 이어읽기라 **~28초** 걸린다
#    (✅ 실측). 이걸 /api 마다 동기로 돌리면: 프런트가 5초마다 요청 → 앞 요청이
#    끝나기 전에 다음이 겹침 → ThreadingHTTPServer 가 스레드를 계속 만들고 _DB_LOCK
#    에 줄서서 대기 스레드·메모리가 쌓인다. 그래서 **백그라운드에서 주기 계산해
#    메모리에 캐시**하고 /api 는 캐시만 즉시 반환한다.
_SNAP = {"data": b'{"warming":true}', "at": 0.0}
_SNAP_LOCK = threading.Lock()


def _snapshot_loop(period=15):
    """주기적으로 snapshot() 을 계산해 직렬화된 바이트로 캐시한다 (단일 스레드)."""
    while True:
        t0 = time.time()
        try:
            body = json.dumps(snapshot(), ensure_ascii=False).encode()
            with _SNAP_LOCK:
                _SNAP["data"], _SNAP["at"] = body, time.time()
        except Exception as e:
            print(f"⚠️ 스냅샷 계산 실패: {type(e).__name__}: {e}", flush=True)
        # 계산이 오래 걸려도 다음 주기까지는 쉰다 (최소 3초는 양보)
        time.sleep(max(3.0, period - (time.time() - t0)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=877)
    ap.add_argument("--no-collect", action="store_true", help="대시보드만")
    ap.add_argument("--log", help="출력을 이 파일에도 복사 (콘솔에는 그대로)")
    args = ap.parse_args()

    if args.log:
        os.makedirs(os.path.dirname(args.log) or ".", exist_ok=True)
        _f = open(args.log, "a", encoding="utf-8", buffering=1, errors="replace")
        sys.stdout = Tee(sys.stdout, _f)                 # 오류·경고·표식만
        sys.stderr = Tee(sys.stderr, _f, keep_all=True)  # 트레이스백은 전체

    if not args.no_collect:
        if bus_collector is None:
            raise SystemExit("bus_collector 를 못 불러왔다")
        bus_collector.STATE = STATE
        bus_collector.LOCK = LOCK
        t = threading.Thread(target=bus_collector.main, daemon=True)
        t.start()
        with LOCK:
            STATE["started"] = time.time()

        # ⚠️ 수집 스레드 감시 — 스레드만 죽으면 대시보드는 멀쩡히 떠 있어서
        #    launchd/systemd/배치 루프의 자동 재시작이 발동하지 않는 좀비가 된다
        #    (✅ 실전: 마지막 관측만 하염없이 늘어나는 채로 발견됨).
        #    스레드가 죽으면 프로세스째 내려서 재시작 장치가 되살리게 한다.
        def watchdog():
            t.join()  # 수집 스레드가 죽어야 리턴한다
            print("수집 스레드 종료 감지 — 프로세스를 내린다 (자동 재시작 장치가 되살린다)", flush=True)
            os._exit(1)
        threading.Thread(target=watchdog, daemon=True).start()

    # 스냅샷 캐시 워커 — /api 가 즉시 응답하도록 백그라운드에서 미리 계산
    threading.Thread(target=_snapshot_loop, daemon=True).start()

    srv = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    print(f"대시보드 http://localhost:{args.port}  (수집 {'끔' if args.no_collect else '켬'})", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        # Ctrl-C 정상 종료. 수집 스레드는 daemon 이라 함께 죽는다 — 사이클 중간이어도
        # 안전하다: sqlite 는 WAL + 사이클 단위 commit, jsonl 은 줄 단위 append,
        # 콜 카운터는 os.replace 원자 쓰기.
        # ⚠️ os._exit 인 이유: daemon 스레드가 print 중(stdout 락 보유)에 인터프리터가
        #    finalize 되면 "_enter_buffered_busy ... could not acquire lock" 으로 abort 한다.
        #    finalize 를 건너뛰면 그 경합 자체가 없다.
        print("\n종료", flush=True)
        os._exit(0)


if __name__ == "__main__":
    main()
