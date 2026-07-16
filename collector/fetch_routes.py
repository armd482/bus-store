#!/usr/bin/env python3
"""
노선 풀 구축 — config.json 의 cityCodes 에 대해 노선 목록 + 정류소 수를 받아 DB에 넣는다.
정류소 수가 있어야 커버리지의 분모(구간수 × 밴드수)를 알 수 있다.

한 번만 돌리면 된다. 시군을 늘리려면 config.json 의 cityCodes 를 고치고 다시 실행.
콜 수 = 시군수 + 노선수 (성남 = 1 + 80 = 81콜). 쿼터에 티도 안 난다.
"""

import json
import os
import sys
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import orchestrator as O

BASE = "https://apis.data.go.kr/1613000/BusRouteInfoInqireService"


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


def get(key, op, **kw):
    q = urllib.parse.urlencode({"serviceKey": key, "_type": "json", **kw})
    with urllib.request.urlopen(f"{BASE}/{op}?{q}", timeout=25) as r:
        d = json.loads(r.read().decode())
    it = (d["response"]["body"].get("items") or {}).get("item") or []
    return [it] if isinstance(it, dict) else it


def main():
    key = load_key()
    if not key:
        sys.exit("GBIS_BUS_KEY 없음")
    k = O.cfg()
    conn = O.connect()

    total = 0
    for city in k["cityCodes"]:
        routes = get(key, "getRouteNoList", cityCode=city, numOfRows=500, pageNo=1)
        print(f"[{city}] 노선 {len(routes)}개 — 정류소 수 조회 중…", flush=True)

        def nstops(r):
            try:
                return r["routeid"], len(get(key, "getRouteAcctoThrghSttnList",
                                             cityCode=city, routeId=r["routeid"], numOfRows=500))
            except Exception:
                return r["routeid"], 0

        with ThreadPoolExecutor(max_workers=k.get("maxWorkers", 8)) as ex:
            counts = dict(ex.map(nstops, routes))

        for r in routes:
            n = counts.get(r["routeid"], 0)
            conn.execute("""
              INSERT INTO route(routeid,cityCode,routeno,routetp,nstops) VALUES(?,?,?,?,?)
              ON CONFLICT(routeid) DO UPDATE SET cityCode=?,routeno=?,routetp=?,nstops=?
            """, (r["routeid"], city, str(r["routeno"]), r.get("routetp", ""), n,
                  city, str(r["routeno"]), r.get("routetp", ""), n))
        conn.commit()
        seg = sum(max(0, n - 1) for n in counts.values())
        total += seg
        print(f"  정류소 {sum(counts.values()):,}개 · 구간 {seg:,}개", flush=True)

    nb = len(k["timebands"])
    print(f"\n노선 풀: {conn.execute('SELECT COUNT(*) FROM route').fetchone()[0]:,}개 노선 · 구간 {total:,}개")
    print(f"평일 목표 셀: {total:,} × {nb}밴드 = {total*nb:,}개 × {k['targetSamples']}샘플")
    print(f"→ 필요 관측: {total*nb*k['targetSamples']:,}건")


if __name__ == "__main__":
    main()
