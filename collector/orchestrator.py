#!/usr/bin/env python3
"""
수집 조율 — 커버리지 추적 + 노선 선택 + 진행률 (docs §4.4)

문제: 지금까지는 노선 목록이 고정이라 이미 충분히 모은 구간을 계속 재폴링했다.
      그 콜로 아직 한 번도 안 본 노선을 봐야 한다.

해법: (노선, 구간, 시간대, 요일) 셀별 샘플 수를 세고, 셀이 목표를 채운 노선은
      폴링 목록에서 빼고 미커버 노선을 넣는다. 그러면 총 소요가 달력이 아니라
      커버리지에 묶인다.

⚠️ 커버리지 산수 (✅ 성남 실측 기반):
     구간당 관측 51.5회/일  ÷ 시간대 수 = 셀당/일
     시간대 7개  → 7.4회/일 → 목표 10샘플에 1.4일
     시간대 19개 → 2.7회/일 → 목표 10샘플에 3.7일
   → timebands 가 수집 기간을 지배한다. config.json 참조.

⚠️ 요일이 병목: 토·일은 주 1일씩만 얻는다. 목표 10샘플이면 10주.
   평일부터 채우고 주말은 뒤에 붙일 것.

사용:
  python3 orchestrator.py status           진행률
  python3 orchestrator.py routes           다음 사이클에 폴링할 노선
  python3 orchestrator.py reset --yes      관측 카운트 전체 초기화 (아래 주의)
  python3 orchestrator.py rebuild --yes    장부를 jsonl 데이터에서 재계산

⚠️ 데이터(jsonl)와 장부(cell)는 따로다 — 아무도 jsonl 을 다시 읽지 않으므로
   파일이나 행을 지워도 장부는 모른다. 장부만 남으면 "이미 채웠다"고 믿어
   그 구간을 다시 안 찍는다: 영구 구멍. 데이터를 지웠으면:
     전부 지웠다  → reset --yes  (장부도 0으로)
     일부만 지웠다 → rebuild --yes (남은 jsonl 로 장부 재계산 — 행에 band/daytype
                     이 박혀 있어 정확히 재현된다)
   반대로 장부만 지우면 재수집할 뿐이라 안전하다 (중복 데이터, 쿼터 낭비).
"""

import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone

# ⚠️ 윈도우 방어. 모든 스크립트가 이 모듈을 import 하므로 여기서 한 번만 한다.
#    윈도우의 기본 stdout 인코딩은 cp949 라 로그의 한글·이모지에서 UnicodeEncodeError 로 죽는다.
#    ✅ 재현: PYTHONIOENCODING=cp949 → "⚠️ 실패" 출력에서 크래시.
#    하필 그 줄은 API 실패 시에만 타는 경로다 → 잘 돌다가 첫 실패에 수집기가 통째로 죽는다.
#    errors="replace" 까지 두는 건, 로그 한 글자 때문에 수집이 멈추는 일은 없어야 하기 때문.
for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass  # 파이프로 감싸였거나 3.7 미만

KST = timezone(timedelta(hours=9))
HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "data")
DB = os.path.join(DATA, "coverage.sqlite")
CONFIG = os.path.join(HERE, "config.json")


def cfg():
    return json.load(open(CONFIG, encoding="utf-8"))


def service_day_of(t):
    """운행일. 04시 기준으로 하루를 가른다 — 01:30 에 잡힌 버스는 전날 운행분이다."""
    return t - timedelta(hours=4)


def day_type(t):
    """⚠️ 벽시계가 아니라 운행일 기준이다.

    토요일 01:30 에 도는 버스는 금요일 막차다. t.weekday() 를 그대로 쓰면
    금요일 막차가 '토요일' 표본으로 잘못 들어간다.
    """
    wd = service_day_of(t).weekday()
    return "sun" if wd == 6 else "sat" if wd == 5 else "weekday"


def band_of(t, bands):
    """시각 → 밴드 인덱스. 어느 밴드에도 없으면 None(커버리지에 안 넣음).

    ⚠️ 24를 넘는 끝값은 익일이다: [20, 27] = 20:00~03:00.
    이걸 처리 안 하면 00:30 관측이 어느 밴드에도 안 걸려 영원히 안 채워진다.
    """
    h = t.hour
    for i, (a, b) in enumerate(bands):
        if b <= 24:
            if a <= h < b:
                return i
        elif h >= a or h < b - 24:   # 자정 넘김
            return i
    return None


def in_window(t, window):
    """운행 창 안인가. window 도 24 초과 = 익일. [4, 27] = 04:00~03:00."""
    a, b = window
    h = t.hour
    return (a <= h < b) if b <= 24 else (h >= a or h < b - 24)


def connect():
    os.makedirs(DATA, exist_ok=True)
    c = sqlite3.connect(DB, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")  # 수집기가 쓰는 중에도 status 가 읽히도록
    c.execute("""
      CREATE TABLE IF NOT EXISTS cell (
        routeid  TEXT NOT NULL,
        from_ord INTEGER NOT NULL,
        to_ord   INTEGER NOT NULL,
        band     INTEGER NOT NULL,
        daytype  TEXT NOT NULL,
        n        INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (routeid, from_ord, to_ord, band, daytype)
      )""")
    c.execute("CREATE INDEX IF NOT EXISTS cell_route ON cell(routeid)")
    # 노선 풀 — fetch_routes.py 가 채운다
    c.execute("""
      CREATE TABLE IF NOT EXISTS route (
        routeid     TEXT PRIMARY KEY,
        cityCode    INTEGER NOT NULL,
        routeno     TEXT,
        routetp     TEXT,
        nstops      INTEGER DEFAULT 0,
        startvt     TEXT,               -- 첫차 'HHMM' — pick_routes 의 운행시간 필터(§2.6)
        endvt       TEXT,               -- 막차 출발 'HHMM'. ⚠️ 도착이 아니다
        emptyStreak INTEGER DEFAULT 0,  -- 연속 0대 반환 횟수 — 관측이 정하는 후순위
        lastSeen    REAL                -- 마지막으로 버스가 보인 시각
      )""")

    # 기존 DB 마이그레이션 — CREATE TABLE IF NOT EXISTS 는 이미 있는 테이블에 컬럼을 안 붙인다.
    # ⚠️ 이걸 빠뜨려 배포본에만 손으로 ALTER 했다가, 새 기계에서 'no such column: startvt' 로 죽었다.
    have = {r[1] for r in c.execute("PRAGMA table_info(route)")}
    for col, decl in (("startvt", "TEXT"), ("endvt", "TEXT"),
                      ("emptyStreak", "INTEGER DEFAULT 0"), ("lastSeen", "REAL")):
        if col not in have:
            c.execute(f"ALTER TABLE route ADD COLUMN {col} {decl}")
    c.commit()
    return c


def bump(conn, routeid, from_ord, to_ord, band, daytype, k=1):
    conn.execute("""
      INSERT INTO cell(routeid,from_ord,to_ord,band,daytype,n) VALUES(?,?,?,?,?,?)
      ON CONFLICT(routeid,from_ord,to_ord,band,daytype) DO UPDATE SET n=n+?
    """, (routeid, from_ord, to_ord, band, daytype, k, k))


def route_progress(conn, target, nbands):
    """노선별 (충족 셀, 목표 셀, 관측된 셀). 목표 셀 = 구간수 × 밴드수 (평일 기준)."""
    rows = conn.execute("""
      SELECT r.routeid, r.routeno, r.routetp, r.cityCode, r.nstops,
             COALESCE(SUM(CASE WHEN c.n >= ? AND c.daytype='weekday' THEN 1 ELSE 0 END), 0),
             COALESCE(SUM(CASE WHEN c.daytype='weekday' THEN 1 ELSE 0 END), 0)
      FROM route r LEFT JOIN cell c ON c.routeid = r.routeid
      GROUP BY r.routeid
    """, (target,)).fetchall()
    out = []
    for rid, no, tp, city, nstops, done, seen in rows:
        goal = max(1, (nstops - 1)) * nbands  # 구간수 × 밴드수
        out.append({
            "routeid": rid, "routeno": no, "routetp": tp, "cityCode": city,
            "goal": goal, "done": done, "seen": seen,
            "pct": done / goal if goal else 0.0,
        })
    return out


def _hm(s):
    """'0450' → 290 (자정 이후 분). 값이 없거나 이상하면 None."""
    if not s or not str(s).isdigit() or len(str(s)) < 4:
        return None
    s = str(s)
    return int(s[:2]) * 60 + int(s[2:4])


def maybe_running(startvt, endvt, t, tail_min=90):
    """지금 이 노선에 버스가 있을 법한가 — **1차 필터일 뿐이다.**

    ⚠️ 확실히 아닌 것만 거른다. 진짜 판정은 관측이 한다(emptyStreak).

    `endvehicletime` 은 막차 **출발** 시각이다. 그 뒤로도 종점까지 달린다.
    ✅ 실측: 36번은 00:50 출발 + 177정류소 → 05:16 도착 추정.
    노선 소요시간은 어느 API 도 주지 않으므로(§4.4 결측) 정확히 계산할 수 없다.
    → tail_min 만큼 넉넉히 열어두고, 실제로 비었는지는 폴링해서 안다.

    운행시간 정보가 없으면 True(폴링해서 확인).
    """
    a, b = _hm(startvt), _hm(endvt)
    if a is None or b is None:
        return True
    now = t.hour * 60 + t.minute
    b = (b + tail_min) % 1440
    return (a <= now <= b) if a <= b else (now >= a or now <= b)


def pick_routes(conn, n, target, nbands, t=None, max_empty=6):
    """미충족이 큰 노선 우선. 단 **지금 안 도는 노선은 뺀다.**

    ⚠️ 이전 판은 커버리지만 보고 골라서, 심야에 '미커버 순 40개'를 찍었다.
    그게 실제로 도는 노선인지는 안 봤다 — 05:16 까지 달리는 36번을 떨어뜨리고
    이미 차고에 들어간 낮 노선을 찍고 있을 수 있었다. **가장 필요한 데이터를 버리는 동작.**

    이제 두 단계로 거른다:
      1. 운행시간 창 (명백히 아닌 것만)
      2. emptyStreak — 연속으로 0대면 후순위. **추정이 아니라 관측이 정한다.**
    """
    prog = route_progress(conn, target, nbands)
    live = [p for p in prog if p["pct"] < 1.0]
    if t is not None:
        meta = {r[0]: r[1:] for r in conn.execute(
            "SELECT routeid, startvt, endvt, emptyStreak FROM route")}
        out = []
        for p in live:
            m = meta.get(p["routeid"])
            if not m:
                out.append(p)
                continue
            s, e, streak = m
            if not maybe_running(s, e, t):
                continue                       # 운행시간 밖
            if (streak or 0) >= max_empty:
                p["cold"] = True               # 계속 비어 있음 — 후순위
            out.append(p)
        live = out
    live.sort(key=lambda p: (p.get("cold", False), p["pct"], -p["goal"]))
    return live[:n]


def mark_empty(conn, routeid, empty):
    """관측 결과로 emptyStreak 갱신. 한 대라도 보이면 리셋."""
    if empty:
        conn.execute("UPDATE route SET emptyStreak = COALESCE(emptyStreak,0)+1 WHERE routeid=?", (routeid,))
    else:
        conn.execute("UPDATE route SET emptyStreak = 0, lastSeen = ? WHERE routeid=?",
                     (time.time(), routeid))


def status():
    c = connect()
    k = cfg()
    nb = len(k["timebands"])
    tgt = k["targetSamples"]
    prog = route_progress(c, tgt, nb)
    if not prog:
        print("노선 풀이 비어 있다. fetch_routes.py 를 먼저 돌릴 것.")
        return

    done_routes = [p for p in prog if p["pct"] >= 1.0]
    goal = sum(p["goal"] for p in prog)
    done = sum(p["done"] for p in prog)
    seen = sum(p["seen"] for p in prog)
    total_obs = c.execute("SELECT COALESCE(SUM(n),0) FROM cell").fetchone()[0]

    print(f"=== 커버리지 (평일 · 목표 {tgt}샘플 · 시간대 {nb}개) ===")
    print(f"  노선      {len(prog):,}개 중 완주 {len(done_routes):,}")
    print(f"  셀        {done:,} / {goal:,} 충족 ({done/goal*100 if goal else 0:.1f}%)")
    print(f"  관측된 셀  {seen:,} ({seen/goal*100 if goal else 0:.1f}%)  ← 한 번이라도 본 것")
    print(f"  총 관측    {total_obs:,}건")

    # 남은 기간 추정 — 셀당 하루 관측률로 역산
    if seen:
        per_cell_day = 51.5 / nb  # ✅ 성남 실측 51.5회/구간/일
        remain_cells = goal - done
        rate = min(k["maxRoutes"], len(prog) - len(done_routes))
        if rate:
            avg_goal = goal / max(1, len(prog))
            rounds = max(1, (len(prog) - len(done_routes)) / rate)
            days_per_round = tgt / per_cell_day
            print(f"\n  추정: 라운드 {rounds:.1f}회 × {days_per_round:.1f}일 = 약 {rounds*days_per_round:.0f}일 (평일 기준)")
            print(f"        ⚠️ 토·일은 주 1일씩만 얻으므로 별도. 목표 {tgt}샘플이면 {tgt}주.")

    print(f"\n=== 진행 중 상위/하위 ===")
    live = sorted([p for p in prog if p["pct"] < 1.0], key=lambda p: -p["pct"])
    for p in live[:3]:
        print(f"  {p['routeno']:<6} {p['routetp'][:4]:<4} {p['pct']*100:5.1f}%  ({p['done']:,}/{p['goal']:,})")
    if len(live) > 6:
        print("  …")
    for p in live[-3:]:
        print(f"  {p['routeno']:<6} {p['routetp'][:4]:<4} {p['pct']*100:5.1f}%  ({p['done']:,}/{p['goal']:,})")


def reset(force):
    """관측 카운트(cell) + 관측 파생 상태(emptyStreak/lastSeen) 초기화.

    노선 풀(route)과 일 콜 카운터(.buscalls-*)는 **유지한다** —
    풀은 다시 받으려면 4,400콜이고, 콜 카운터는 오늘 실제로 쓴 쿼터라
    지우면 50만 상한을 넘겨 그날 수집이 죽을 수 있다.
    """
    c = connect()
    n = c.execute("SELECT COALESCE(SUM(n),0) FROM cell").fetchone()[0]
    if not force:
        sys.exit(f"관측 {n:,}건이 지워진다. jsonl 데이터도 같이 지울 것(rm data/bus-*.jsonl).\n"
                 f"정말이면: python3 orchestrator.py reset --yes")
    c.execute("DELETE FROM cell")
    c.execute("UPDATE route SET emptyStreak = 0, lastSeen = NULL")
    c.commit()
    print(f"관측 {n:,}건 초기화. 노선 풀·일 콜 카운터는 유지 (쿼터는 실사용이라 지우면 안 된다).")
    print("수집기가 돌고 있었다면 재시작할 것 — 메모리의 누적 카운터(written)는 별개다.")


def rebuild(force):
    """장부(cell)를 jsonl 데이터에서 재계산 — 데이터 일부를 지웠거나 장부가 의심될 때.

    행에 band/daytype 이 저장돼 있어 수집 당시와 동일하게 재현된다.
    band 가 없는 행(밴드 밖 03-04시 관측)은 원래도 장부에 안 들어갔으므로 건너뛴다.
    emptyStreak/lastSeen 은 jsonl 에 이력이 없으므로 건드리지 않는다.
    ⚠️ 수집기를 멈추고 돌릴 것 — 재계산 중 들어온 관측은 교체 때 유실된다.
    """
    import glob
    files = sorted(glob.glob(os.path.join(DATA, "bus-*.jsonl")))
    if not force:
        sys.exit(f"jsonl {len(files)}개 파일에서 장부를 다시 계산해 cell 을 통째로 교체한다.\n"
                 f"수집기를 먼저 멈출 것. 정말이면: python3 orchestrator.py rebuild --yes")
    counts = {}
    bad = 0
    for p in files:
        for line in open(p, encoding="utf-8"):
            try:
                r = json.loads(line)
            except ValueError:
                bad += 1  # 강제종료로 잘린 마지막 줄 등 — 한 줄 손상은 한 줄만 버린다
                continue
            b = r.get("band")
            if b is None:
                continue
            try:
                fo, to = int(r["from_ord"]), int(r["to_ord"])
            except (KeyError, TypeError, ValueError):
                continue
            if to <= fo:
                continue  # 회차 아티팩트(168→1 등) — 수집기와 같은 기준으로 거른다
            key = (r["routeid"], fo, to, b, r.get("daytype", "weekday"))
            counts[key] = counts.get(key, 0) + 1
    c = connect()
    old = c.execute("SELECT COALESCE(SUM(n),0) FROM cell").fetchone()[0]
    c.execute("DELETE FROM cell")
    c.executemany("INSERT INTO cell(routeid,from_ord,to_ord,band,daytype,n) VALUES(?,?,?,?,?,?)",
                  [k + (n,) for k, n in counts.items()])
    c.commit()
    print(f"재계산 완료: 파일 {len(files)}개 → 관측 {sum(counts.values()):,}건 · 셀 {len(counts):,}개 "
          f"(이전 장부 {old:,}건" + (f" · 깨진 줄 {bad}" if bad else "") + ")")
    print("수집기가 돌고 있었다면 재시작할 것.")


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"
    if cmd == "status":
        status()
    elif cmd == "routes":
        c = connect()
        k = cfg()
        for p in pick_routes(c, k["maxRoutes"], k["targetSamples"], len(k["timebands"])):
            print(f"{p['routeid']}\t{p['cityCode']}\t{p['routeno']}\t{p['pct']*100:.1f}%")
    elif cmd == "reset":
        reset("--yes" in sys.argv[2:])
    elif cmd == "rebuild":
        rebuild("--yes" in sys.argv[2:])
    else:
        sys.exit(f"모르는 명령: {cmd}  (status | routes | reset | rebuild)")


if __name__ == "__main__":
    main()
