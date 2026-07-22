#!/usr/bin/env python3
"""서울 버스 도착정보 스냅샷 수집기 (docs §3.2 · §6.4)

⚠️ **경기(TAGO) 수집기와 방식이 다르다.** 경기는 위치를 30초마다 찍어 구간시간을
   역산한다 — TAGO 가 타임스탬프도 구간시간도 안 주기 때문이다. 서울 도착정보는
   운영사가 **이미 계산한 값**을 준다 (✅ 실측: 753번 1콜에 정류장 104개 × 필드 85개,
   nstnSec·traTime·term·mkTm 전부 채워짐). 그래서 30초 폴링이 필요 없고,
   **밴드당 노선별 1스냅샷**이면 된다.

   문서 §3.2 의 "서울 수집 불가(665×2,280=152만 콜)"는 경기식 위치폴링 전제다.
   스냅샷이면 702노선 × 밴드 7 = **4,914콜/일**로 한도 10,000 의 49% 다.

무엇을 얻나 (§6.4 기대 대기의 CV — 모르면 20분 배차에서 10분 오차):
  arrmsg1/arrmsg2  다음 2대의 도착 예정 → 두 대의 간격 = **실측 순간 배차**
  term             계획 배차 (대조군)
  nstnSec1/2       운영사가 계산한 다음 구간 예정시간
  mkTm             제공시각 — TAGO 에 없는 타임스탬프

⚠️ 이건 **관측이 아니라 운영사 예측**이다. 신분당선 recptnDt 와 같은 성격이라
   (§8.1 ⑤ 가) 시각표 대조 전엔 정시성 판정에 쓰지 말 것. 단 CV(배차 흩어짐)는
   '다음 2대'의 현재 위치 기반이라 예측 오염이 상대적으로 작다.

  python3 seoul_collector.py            # 수집
  python3 seoul_collector.py --routes   # 노선 목록만 갱신하고 종료
"""
import json
import os
import random
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

import orchestrator as O

BASE = "http://ws.bus.go.kr/api/rest"      # ⚠️ https 아님 — 443 이 안 열린다 (§3.2 실측)
PREFIX = "seoul"
OUT_DIR = O.DATA
DAILY_CAP = 9500        # 도착정보 10,000/일에 마진. 위치·노선정보는 별도 쿼터라 무관
ROUTES_FILE = os.path.join(O.DATA, "seoul_routes.json")

# routeType — 7(인천)·8(경기)은 **뺀다**: 경기는 TAGO 로 이미 수집 중이라 중복이고,
# 서울 API 쿼터를 남의 지역에 태울 이유가 없다 (§3.2 '광역버스 중복' 경고 참조).
SEOUL_TYPES = {"1", "2", "3", "4", "5", "6"}
TYPE_NM = {"1": "공항", "2": "마을", "3": "간선", "4": "지선", "5": "순환", "6": "광역"}


def now():
    return datetime.now(O.KST)


def service_day(t):
    return (t - timedelta(hours=4)).strftime("%Y-%m-%d")


def quota_day(t):
    """쿼터는 달력일 — 자정 리셋. 운행일(04시)과 다르다 (지하철 수집기와 같은 이유)."""
    return t.strftime("%Y-%m-%d")


def calls_path(day):
    return os.path.join(O.DATA, f"seoul-calls-{day}.txt")


def read_calls(day):
    try:
        with open(calls_path(day)) as f:
            return int(f.read().strip() or 0)
    except (OSError, ValueError):
        return 0


def add_calls(day, n):
    """디스크에 둔다 — 재시작해도 하루 상한을 넘지 않게 (지하철 수집기와 같은 이유)."""
    v = read_calls(day) + n
    with open(calls_path(day), "w") as f:
        f.write(str(v))
    return v


def load_key():
    """data.go.kr 키는 계정 공통이라 GBIS 키가 서울 데이터셋에도 그대로 통한다
    (✅ 실측: 노선·도착·위치 3종 모두 headerCd=0). 별도 키를 안 만드는 이유."""
    for name in ("SEOUL_BUS_KEY", "GBIS_BUS_KEY"):
        v = os.environ.get(name)
        if v:
            return v
    try:
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")) as f:
            env = dict(l.strip().split("=", 1) for l in f if "=" in l and not l.startswith("#"))
        for name in ("SEOUL_BUS_KEY", "GBIS_BUS_KEY"):
            if env.get(name):
                return env[name]
    except OSError:
        pass
    return None


def fetch(key, op, **params):
    q = urllib.parse.urlencode({"serviceKey": key, **params})
    with urllib.request.urlopen(f"{BASE}/{op}?{q}", timeout=20) as r:
        return ET.fromstring(r.read().decode("utf-8", "replace"))


def refresh_routes(key):
    """서울 소속 노선 목록. getBusRouteList 는 검색어가 필수라 0~9 로 훑는다
    (노선번호에 숫자가 없는 노선은 없다). 노선정보는 **별도 쿼터**(1,000/일)."""
    seen = {}
    for s in "0123456789":
        try:
            root = fetch(key, "busRouteInfo/getBusRouteList", strSrch=s)
        except Exception as e:
            print(f"  노선 조회 실패({s}): {type(e).__name__}", flush=True)
            continue
        for it in root.findall(".//itemList"):
            f = {c.tag: (c.text or "") for c in it}
            if f.get("routeType") in SEOUL_TYPES and f.get("busRouteId"):
                seen[f["busRouteId"]] = {"no": f.get("busRouteNm"), "tp": f.get("routeType"),
                                         "term": f.get("term"), "first": f.get("firstBusTm"),
                                         "last": f.get("lastBusTm")}
        time.sleep(0.3)
    if seen:
        with open(ROUTES_FILE, "w", encoding="utf-8") as f:
            json.dump(seen, f, ensure_ascii=False)
    return seen


def routes(key, force=False):
    if not force and os.path.exists(ROUTES_FILE):
        try:
            with open(ROUTES_FILE, encoding="utf-8") as f:
                r = json.load(f)
            if r:
                return r
        except (OSError, ValueError):
            pass
    print(f"[{now():%H:%M:%S}] 서울 노선 목록 갱신 중…", flush=True)
    r = refresh_routes(key)
    print(f"[{now():%H:%M:%S}] 서울 소속 {len(r):,}개", flush=True)
    return r


def snapshot(key, rid):
    """노선 1개의 전 정류장 도착정보 — 1콜. 슬림 형식으로 줄여 저장한다
    (원본 85필드를 다 남기면 하루 수백 MB. 쓰는 것만 남긴다)."""
    root = fetch(key, "arrive/getArrInfoByRouteAll", busRouteId=rid)
    out = []
    for it in root.findall(".//itemList"):
        f = {c.tag: (c.text or "") for c in it}
        out.append({
            "ord": f.get("staOrd"), "stId": f.get("stId"), "stNm": f.get("stNm"),
            "mkTm": f.get("mkTm"),
            # 다음 2대 — 이 둘의 간격이 실측 순간 배차 (CV 의 재료)
            "arr1": f.get("arrmsg1"), "arr2": f.get("arrmsg2"),
            "veh1": f.get("plainNo1"), "veh2": f.get("plainNo2"),
            "sec1": f.get("nstnSec1"), "sec2": f.get("nstnSec2"),   # 다음 구간 예정시간
            "spd1": f.get("traSpd1"), "term": f.get("term"),
        })
    return out


def main():
    key = load_key()
    if not key:
        sys.exit("키 없음 — .env 에 SEOUL_BUS_KEY 또는 GBIS_BUS_KEY 가 있어야 한다")
    if "--routes" in sys.argv:
        print(f"{len(routes(key, force=True)):,}개 저장: {ROUTES_FILE}")
        return

    os.makedirs(O.DATA, exist_ok=True)
    R = routes(key)
    if not R:
        sys.exit("노선 목록이 비었다 — --routes 로 먼저 받을 것")
    bands = O.cfg()["timebands"]
    print(f"[{now():%H:%M:%S}] 서울 도착정보 스냅샷 · 노선 {len(R):,}개 · 밴드 {len(bands)}개 "
          f"· 상한 {DAILY_CAP:,}/일 (오늘 {read_calls(quota_day(now())):,} 사용)", flush=True)
    tp_n = {}
    for v in R.values():
        tp_n[v["tp"]] = tp_n.get(v["tp"], 0) + 1
    print("  " + " · ".join(f"{TYPE_NM.get(k, k)}{v}" for k, v in sorted(tp_n.items())), flush=True)

    day = service_day(now())
    done = {}          # (band, routeid) -> 이 운행일에 찍었나
    rotated_day = None
    written = 0

    while True:
        t = now()
        d = service_day(t)
        if d != day:
            print(f"[{t:%H:%M:%S}] 운행일 전환 {day} → {d} (전일 {written:,}행)", flush=True)
            day, done, written = d, {}, 0
            continue
        due = O.rotate_due(rotated_day, t)
        if due:
            rotated_day = due
            __import__("threading").Thread(
                target=O.rotate_jsonl, args=(PREFIX,), daemon=True).start()

        b = O.band_of(t, bands)
        if b is None:
            time.sleep(120)
            continue
        qday = quota_day(t)
        todo = [rid for rid in R if (b, rid) not in done]
        if not todo:
            time.sleep(60)              # 이 밴드는 다 찍었다 — 다음 밴드까지 쉰다
            continue

        left = DAILY_CAP - read_calls(qday)
        if left <= 0:
            print(f"[{t:%H:%M:%S}] 일 상한 도달 — 자정 리셋까지 대기", flush=True)
            time.sleep(300)
            continue

        # 밴드가 끝나기 전에 todo 를 고르게 편다. 남은 쿼터도 함께 본다
        # (지하철 pace() 와 같은 원칙 — 캡에 부딪히는 대신 성겨진다).
        band_end = t.replace(hour=bands[b][1] % 24, minute=0, second=0, microsecond=0)
        if bands[b][1] >= 24 or band_end <= t:
            band_end = band_end + timedelta(days=1)
        secs = max(60, (band_end - t).total_seconds())
        # 하한 2초 — 밴드가 얼마 안 남은 채로 시작하면 산식이 0.8초까지 내려가
        # 702콜을 10분에 몰아친다. 서울 API 의 순간 rate 제한은 확인된 바 없어
        # 버스트를 만들지 않는다 (경기에서 버스트가 429·세션99 를 부른 전례 — paced()).
        gap = max(2.0, min(30.0, secs / min(len(todo), left)))

        rid = random.choice(todo)       # 무작위 — 항상 같은 순서면 노선별 위상이 고정된다
        try:
            rows = snapshot(key, rid)
            add_calls(qday, 1)
        except Exception as e:
            add_calls(qday, 1)          # 실패도 쿼터를 먹는다
            print(f"[{t:%H:%M:%S}] 실패 {R[rid]['no']}: {type(e).__name__}", flush=True)
            done[(b, rid)] = True       # 이 밴드에선 재시도 안 한다 (다음 밴드에 다시 온다)
            time.sleep(gap)
            continue
        done[(b, rid)] = True

        if rows:
            path = os.path.join(OUT_DIR, f"{PREFIX}-{day}.jsonl")
            with open(path, "a", encoding="utf-8") as f:
                for r in rows:
                    r["t"] = t.isoformat()
                    r["rid"] = rid
                    r["no"] = R[rid]["no"]
                    r["tp"] = R[rid]["tp"]
                    r["band"] = b
                    r["daytype"] = O.day_type(t)
                    f.write(json.dumps(r, ensure_ascii=False) + "\n")
            written += len(rows)

        if len(done) % 50 == 0:
            mem = O.rss_mb()
            print(f"[{t:%H:%M:%S}] 밴드{b} · 찍은 노선 {len([1 for k in done if k[0] == b]):,}/{len(R):,}"
                  f" · 콜 {read_calls(qday):,}/{DAILY_CAP:,} · 오늘 {written:,}행 · {gap:.1f}s 간격"
                  + (f" · {mem:.0f}MB" if mem else ""), flush=True)
        time.sleep(gap + random.uniform(-0.2, 0.2))


if __name__ == "__main__":
    main()
