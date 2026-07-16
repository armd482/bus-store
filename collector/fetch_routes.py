#!/usr/bin/env python3
"""
노선 풀 구축 — config.json 의 cityCodes 에 대해 노선 목록 + 정류소 수 + 운행시간을 받아 DB에 넣는다.
정류소 수가 있어야 커버리지의 분모(구간수 × 밴드수)를 알 수 있고,
운행시간(startvt/endvt)이 있어야 심야에 pick_routes 의 운행시간 필터가 동작한다.

한 번만 돌리면 된다. 시군을 늘리려면 config.json 의 cityCodes 를 고치고 다시 실행.
콜 수 = 시군수 + 노선수 × 2 (정류소목록 + 노선정보. 경기 = 31 + 4,400 ≈ 4,431콜, 약 10분).

⚠️ 돌리는 동안 수집기를 멈출 것 — 동시 세션 30 을 둘이 나눠 쓰면 서로 실패한다.
"""

import json
import os
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import orchestrator as O

BASE = "https://apis.data.go.kr/1613000/BusRouteInfoInqireService"

# ⚠️ 25 로 돌렸다가 code99(세션 30/30)로 죽었다. 워커당 순차 2콜이라 in-flight ≤ 워커수인데,
#    서버 쪽 세션이 응답 후 바로 안 빠져 상한에 너무 붙으면 꼬리에서 터진다 (수집기 실측과 동일).
WORKERS = 15


def load_key():
    key = os.environ.get("GBIS_BUS_KEY")
    if key:
        return key
    for p in (os.path.join(O.HERE, ".env"),
              os.path.join(O.HERE, "..", "..", "bus-test", ".env.local")):
        try:
            for line in open(p, encoding="utf-8"):
                if line.strip().startswith("GBIS_BUS_KEY="):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
        except OSError:
            continue
    return None


def get(key, op, retries=3, **kw):
    """⚠️ 실패가 HTTP 200 으로 온다 — resultCode 를 확인하고 아니면 예외를 던진다.
    이걸 안 보면 세션 초과(code99)가 빈 목록으로 둔갑해 nstops=0 이 조용히 저장된다.

    code99(세션 고갈)와 네트워크 오류는 일시적이라 백오프(2·4·8s) 후 재시도한다.
    시군 목록 같은 최상위 호출이 일시 오류 한 방에 전체 실행을 죽이지 않게.
    나머지 코드(키 오류 등)는 재시도해도 같으므로 즉시 던진다."""
    q = urllib.parse.urlencode({"serviceKey": key, "_type": "json", **kw})
    last = None
    for i in range(retries + 1):
        if i:
            time.sleep(2 * 2 ** (i - 1))
        try:
            with urllib.request.urlopen(f"{BASE}/{op}?{q}", timeout=25) as r:
                d = json.loads(r.read().decode())
        except Exception as e:
            last = e
            continue
        h = d.get("response", {}).get("header", {})
        code = str(h.get("resultCode", "?"))
        if code in ("00", "0"):
            body = d["response"].get("body") or {}
            it = ((body.get("items") or {}).get("item") or []) if isinstance(body, dict) else []
            return [it] if isinstance(it, dict) else it
        last = RuntimeError(f"code{code}:{h.get('resultMsg', '')[:40]}")
        if code != "99":
            break
    raise last


def _vt(x):
    """운행시각 정규화: 450/'450' → '0450'. 형식이 아니면 None
    (없는 값은 maybe_running 이 True 로 열어 폴링으로 확인한다)."""
    s = str(x if x is not None else "").strip()
    return s.zfill(4) if s.isdigit() and 3 <= len(s) <= 4 else None


def main():
    key = load_key()
    if not key:
        sys.exit("GBIS_BUS_KEY 없음")
    k = O.cfg()
    conn = O.connect()

    for city in k["cityCodes"]:
        routes = get(key, "getRouteNoList", cityCode=city, numOfRows=500, pageNo=1)
        print(f"[{city}] 노선 {len(routes)}개 — 정류소·운행시간 조회 중…", flush=True)
        fails = []

        def detail(r):
            rid = r["routeid"]
            n = s = e = None
            try:
                info = get(key, "getRouteInfoIem", cityCode=city, routeId=rid)
                if info:
                    s = _vt(info[0].get("startvehicletime"))
                    e = _vt(info[0].get("endvehicletime"))
            except Exception as ex:
                fails.append((rid, "info", str(ex)[:30]))
            try:
                n = len(get(key, "getRouteAcctoThrghSttnList",
                            cityCode=city, routeId=rid, numOfRows=500))
            except Exception as ex:
                fails.append((rid, "stops", str(ex)[:30]))
            return rid, (n, s, e)

        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            details = dict(ex.map(detail, routes))

        for r in routes:
            n, s, e = details.get(r["routeid"], (None, None, None))
            # ⚠️ 실패(None)면 기존 값을 보존한다 — 0 으로 덮으면 커버리지 분모가
            #    조용히 무너지고, 재실행해도 실패하는 노선은 영영 0 으로 남는다.
            conn.execute("""
              INSERT INTO route(routeid,cityCode,routeno,routetp,nstops,startvt,endvt)
              VALUES(?,?,?,?,COALESCE(?,0),?,?)
              ON CONFLICT(routeid) DO UPDATE SET
                cityCode=?, routeno=?, routetp=?,
                nstops=COALESCE(?, nstops),
                startvt=COALESCE(?, startvt),
                endvt=COALESCE(?, endvt)
            """, (r["routeid"], city, str(r["routeno"]), r.get("routetp", ""), n, s, e,
                  city, str(r["routeno"]), r.get("routetp", ""), n, s, e))
        conn.commit()

        got = [v for v in details.values() if v[0] is not None]
        print(f"  정류소 {sum(v[0] for v in got):,}개 · 운행시간 {sum(1 for v in got if v[1]):,}건", flush=True)
        if fails:
            head = " ".join(f"{rid}/{what}" for rid, what, _ in fails[:5])
            print(f"  ⚠️ 조회 실패 {len(fails)}건 (기존 값 보존): {head}"
                  + (" …" if len(fails) > 5 else ""), flush=True)

    nroute = conn.execute("SELECT COUNT(*) FROM route").fetchone()[0]
    nseg = conn.execute("SELECT COALESCE(SUM(MAX(nstops-1,0)),0) FROM route").fetchone()[0]
    novt = conn.execute("SELECT COUNT(*) FROM route WHERE startvt IS NULL").fetchone()[0]
    nb = len(k["timebands"])
    print(f"\n노선 풀: {nroute:,}개 노선 · 구간 {nseg:,}개 · 운행시간 없음 {novt:,}개")
    print(f"평일 목표 셀: {nseg:,} × {nb}밴드 = {nseg*nb:,}개 × {k['targetSamples']}샘플")
    print(f"→ 필요 관측: {nseg*nb*k['targetSamples']:,}건")


if __name__ == "__main__":
    main()
