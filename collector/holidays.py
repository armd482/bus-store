#!/usr/bin/env python3
"""
공휴일 조회 — 대체공휴일·임시공휴일 포함 (docs/transit-routing-gtfs.md §4.4 "분석 규칙")

**목록을 코드나 config 에 박지 않는다.** 대체공휴일은 그 해 달력에 따라 정해지고
임시공휴일은 몇 주 전에 갑자기 지정되며, 공휴일 지정 자체가 법 개정으로 바뀐다
(✅ 실측 2026-07-19: 조회한 피드에 제헌절 07-17 이 공휴일로 들어 있다 — 손으로
적었으면 틀렸을 항목이다). 손으로 관리하면 15주 수집 중에 반드시 어긋난다.

왜 필요한가: 평일에 낀 공휴일은 평상 다이어가 아니라 휴일 다이어로 돈다.
수요일 추석을 'wed' 표본에 넣으면 요일별 주행시간이 오염된다. 그래서 **장부
(cell/subway_cell)에서만 뺀다** — jsonl 에는 그대로 쌓이므로 나중에 규칙을
바꾸면 rebuild 로 되살아난다 ("jsonl = 진실").

출처 두 곳 — 하나가 막혀도 도는 구조:

  ① 한국천문연구원 특일 정보 (data.go.kr 15012690) — **공식·권위**.
     `isHoliday=Y` 만 주므로 기념일을 거를 필요가 없다.
     ⚠️ 별도 활용신청이 필요하다 (✅ 실측 2026-07-19: 현재 키로 HTTP 403
     `Forbidden` = 미승인 API. docs §3.7 의 Forbidden/Unauthorized 구분).
     신청하면 자동승인 + 게이트웨이 반영 ~1시간이고, 그때부터 자동으로 이쪽을 쓴다.

  ② 구글 대한민국 휴일 iCal — **신청 불요, 지금 바로 된다** (✅ 실측: 2025~2027
     전부 조회됨. 임시공휴일 2025-01-27, 대체공휴일 '쉬는 날 삼일절' 포함).
     ⚠️ 기념일(식목일·스승의날)이 같은 피드에 섞여 있으나 `DESCRIPTION` 이
     `공휴일`/`기념일`로 갈린다 — **이름 목록으로 거르지 않는다** (그것도 하드코딩이다).

  ③ config `holidays` — 위 둘이 놓친 것을 손으로 얹는 **덮어쓰기**용.
     임시공휴일이 지정됐는데 피드 반영이 늦을 때 즉시 대응하는 탈출구.
     비워 두는 게 정상이다.

결과는 `data/holidays.json` 에 캐시한다. 네트워크가 죽어도 캐시로 돌고,
캐시도 없으면 **빈 집합**이다 — 공휴일을 못 가져왔다고 수집이 멈추면 안 된다
(그 손실이 오염보다 크다. 나중에 목록을 채우고 rebuild 하면 재분류된다).

  python3 holidays.py            # 지금 적용 중인 목록
  python3 holidays.py --refresh  # 캐시 무시하고 다시 받는다
"""

import json
import os
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "data")
CACHE = os.path.join(DATA, "holidays.json")

# 특일 정보 (15012690). 신청 전엔 403 — 그때는 조용히 ICS 로 넘어간다.
KASI = "http://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService/getRestDeInfo"
ICS = ("https://calendar.google.com/calendar/ical/"
       "ko.south_korea%23holiday%40group.v.calendar.google.com/public/basic.ics")

REFRESH_SEC = 7 * 86400     # 캐시 수명 — 임시공휴일 지정을 일주일 안에 잡는다
RETRY_SEC = 3600            # 실패 후 재시도 간격. 매 사이클 네트워크를 두드리지 않게
TIMEOUT = 15

_mem = {"days": None, "at": 0.0, "tried": 0.0, "source": None}


def _kasi(key, year):
    """특일 정보 → {YYYY-MM-DD}. isHoliday=Y 만 센다 (기념일 제외는 API 가 해준다)."""
    q = urllib.parse.urlencode({"serviceKey": key, "_type": "json",
                                "solYear": year, "numOfRows": 100})
    with urllib.request.urlopen(f"{KASI}?{q}", timeout=TIMEOUT) as r:
        d = json.loads(r.read().decode())
    body = (d.get("response") or {}).get("body") or {}
    items = (body.get("items") or {}).get("item") or []
    if isinstance(items, dict):
        items = [items]
    out = set()
    for it in items:
        if str(it.get("isHoliday", "Y")).upper() != "Y":
            continue
        s = str(it.get("locdate") or "")
        if len(s) == 8 and s.isdigit():
            out.add(f"{s[:4]}-{s[4:6]}-{s[6:8]}")
    if not out:
        raise RuntimeError("특일정보 응답에 공휴일이 없다")
    return out


def _ics(years):
    """구글 iCal → {YYYY-MM-DD}. **DESCRIPTION 이 '공휴일'인 것만** —
    같은 피드의 기념일(식목일·스승의날)을 이름이 아니라 구조로 가른다.
    대체공휴일은 '쉬는 날 삼일절'처럼 별도 이벤트로 오고 DESCRIPTION 이 공휴일이라
    따로 처리할 게 없다. 어린이날·부처님오신날이 겹친 날처럼 중복은 집합이 흡수한다."""
    req = urllib.request.Request(ICS, headers={"User-Agent": "findpath-collector"})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        txt = r.read().decode("utf-8", errors="replace")
    want = {str(y) for y in years}
    out = set()
    for ev in re.findall(r"BEGIN:VEVENT(.*?)END:VEVENT", txt, re.S):
        m = re.search(r"DTSTART;VALUE=DATE:(\d{8})", ev)
        desc = re.search(r"DESCRIPTION:(.*)", ev)
        if not m or not desc or not desc.group(1).strip().startswith("공휴일"):
            continue
        s = m.group(1)
        if s[:4] in want:
            out.add(f"{s[:4]}-{s[4:6]}-{s[6:8]}")
    if not out:
        raise RuntimeError("iCal 에서 공휴일을 못 찾았다 (형식 변경?)")
    return out


def _read_cache():
    try:
        with open(CACHE, encoding="utf-8") as f:
            d = json.load(f)
        return set(d.get("days") or []), float(d.get("at") or 0), d.get("source")
    except (OSError, ValueError, TypeError):
        return set(), 0.0, None


def _write_cache(days, source):
    os.makedirs(DATA, exist_ok=True)
    tmp = CACHE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"at": time.time(), "source": source,
                   "updated": datetime.now().isoformat(timespec="seconds"),
                   "days": sorted(days)}, f, ensure_ascii=False, indent=1)
    os.replace(tmp, CACHE)   # 원자적 — 읽는 쪽이 반쪽 파일을 보지 않게


def refresh(key=None, years=None, quiet=False):
    """두 출처를 순서대로 시도해 캐시를 갱신한다. 실패하면 (기존 캐시, None)."""
    y = years or _years()
    for name, fn in (("특일정보", (lambda: set().union(*(_kasi(key, yy) for yy in y))) if key else None),
                     ("iCal", lambda: _ics(y))):
        if fn is None:
            continue
        try:
            days = fn()
        except Exception as e:
            if not quiet:
                # 403 은 미승인이라 재시도해도 같다 — 안내만 하고 다음 출처로.
                hint = (" — 활용신청 필요: data.go.kr 15012690 (자동승인·반영 ~1시간)"
                        if "403" in str(e) else "")
                print(f"[공휴일] {name} 실패: {type(e).__name__} {e}{hint}", flush=True)
            continue
        _write_cache(days, name)
        if not quiet:
            print(f"[공휴일] {name} 에서 {len(days)}일 갱신 ({', '.join(str(x) for x in y)})", flush=True)
        return days, name
    cached, _, src = _read_cache()
    if not quiet:
        print(f"[공휴일] ⚠️ 전 출처 실패 — 캐시 {len(cached)}일 유지"
              f"{f' (출처 {src})' if src else ' (캐시 없음 — 공휴일 제외 없이 진행)'}", flush=True)
    return cached, None


def _years():
    """올해와 내년 — 12월에 시작해 1월을 넘겨도 목록이 비지 않게."""
    y = datetime.now().year
    return (y, y + 1)


def data_go_kr_key():
    """특일정보용 data.go.kr 키. bus_collector 를 import 하지 않는다 —
    orchestrator ↔ bus_collector 순환이 생긴다. 같은 .env 를 직접 읽는다."""
    k = os.environ.get("GBIS_BUS_KEY")
    if k:
        return k
    for p in (os.path.join(HERE, ".env"),
              os.path.join(HERE, "..", "..", "bus-test", ".env.local")):
        try:
            for line in open(p, encoding="utf-8"):
                if line.strip().startswith("GBIS_BUS_KEY="):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
        except OSError:
            continue
    return None


def load(key=None, force=False):
    """지금 적용할 공휴일 집합. 캐시 우선, 오래됐으면 갱신 시도.

    ⚠️ 절대 예외를 올리지 않는다 — 수집 루프가 매 사이클 부른다.
    네트워크가 죽어 있으면 캐시(없으면 빈 집합)로 계속 돈다. 실패한 갱신은
    RETRY_SEC(1시간) 뒤에나 다시 시도한다 — 매 사이클 15초씩 물고 있으면
    사이클이 통째로 밀린다.
    """
    now = time.time()
    if _mem["days"] is not None and not force and now - _mem["at"] < 600:
        return _mem["days"]                     # 메모리 캐시 — 사이클마다 디스크도 안 읽는다
    days, at, src = _read_cache()
    stale = force or not days or (now - at) > REFRESH_SEC
    if stale and (force or now - _mem["tried"] > RETRY_SEC):
        _mem["tried"] = now
        days, got = refresh(key=key or data_go_kr_key(), quiet=False)
        src = got or src
    _mem.update(days=days, at=now, source=src)
    return days


def cached():
    """**네트워크를 절대 건드리지 않는** 조회 — 대시보드(/api)용.

    ⚠️ /api 는 5초마다 오는데 load() 는 캐시가 오래되면 갱신을 시도한다(출처 2곳 ×
    15초). 그 한 번이 하필 /api 스레드에 걸리면 대시보드가 30초 멈춘 것처럼 보인다.
    갱신은 수집 루프(사이클마다 holiday_set 호출)에 맡기고 화면은 캐시만 읽는다.
    """
    if _mem["days"] is not None:
        return _mem["days"]
    days, _, _ = _read_cache()
    return days


def info(key=None, network=False):
    """요약. network=False(기본)면 캐시만 본다 — 대시보드가 멈추지 않게."""
    days = load(key) if network else cached()
    _, at, src = _read_cache()
    return {"count": len(days), "source": src,
            "updated": datetime.fromtimestamp(at).isoformat(timespec="seconds") if at else None,
            "days": sorted(days)}


def main():
    import sys
    key = data_go_kr_key()                      # 특일정보는 data.go.kr 키를 쓴다
    if "--refresh" in sys.argv[1:]:
        refresh(key=key)
    d = info(key)
    print(f"공휴일 {d['count']}일 · 출처 {d['source'] or '없음'} · 갱신 {d['updated'] or '—'}")
    today = datetime.now().strftime("%Y-%m-%d")
    for x in d["days"]:
        if x >= today:
            print(f"  {x}" + ("   ← 오늘" if x == today else ""))


if __name__ == "__main__":
    main()
