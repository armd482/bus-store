#!/usr/bin/env python3
"""
지하철 실시간 위치 수집기 — 정시성 검증용 (docs/transit-routing-gtfs.md §3.3.3 · §8 #1)

계획 시각표(신분당선은 운영사 공식 PDF — §3.3.1)와 실측 통과 시각을 대조해
정시성 분포를 잰다. ⚠️ 시각표 "복원"용이 아니다 — 관측으로 복원한 시각표에
그 관측을 대면 지연이 정의상 0 이라 순환이다 (§3.3.3 정정). 복원이 유효한 건
공식 시각표가 없는 노선(수인분당 등)뿐이고, 그 경우에도 부산물로 나온다.

★ 전 노선 일괄 수집 (OA-15799 `realtimeStationArrival/.../ALL`) — ✅ 실측 2026-07-18:
  1콜에 **19개 노선 · 555역 · 약 2,916건**이 전부 온다. 노선별 폴링(노선당 1콜)이면
  전 노선에 19,000콜/일이라 키 19개가 필요했는데, 일괄이면 1회 3콜(1,000건씩 3페이지)로 끝난다.
  ⚠️ **경기·인천이 들어온다** — 판교·광교·수원·기흥·오이도·인천·안산·의정부 실측 확인.
     ("서울시 이외 미제공"은 역별 도착조회(realtimeStationArrival/{역명})에 붙은 제약이고
      이 일괄 엔드포인트엔 해당하지 않는다. 위치 API 만 경기가 된다고 오판했었다.)

핵심 제약 (§3.3.2):
  - 키당 1,000회/일 하드 리밋 → DAILY_CAP(950) 으로 방어. 키를 여러 개 두면 라운드로빈
    (✅ 3키 = 2,850콜/일 ÷ 3콜/회 = 950회/일 → 76초 간격으로 전 노선)
  - 갱신 10~20초. 76초보다 촘촘히 불러도 새 정보가 별로 없다

핵심 설계:
  - 매일 시작 오프셋을 흔든다. 고정하면 매일 같은 위상만 찍혀 해상도가 안 오른다
  - **`arvlCd` 0진입/1도착/2출발만** 기록한다 — 그 역에 실재하는 관측이다.
    3·4·5(전역 상태)·99(운행중)는 다른 역 얘기라 버린다. `barvlDt`(N초 후)는 BIS
    **예측**이라 정시성 판정에 쓰지 않는다 — 우리가 쓰는 건 상태 전이 시각이다.
  - 상태가 바뀔 때만 기록한다 ((열차,역) → arvlCd)
  - JSONL append — 크래시해도 그때까지가 남는다
"""

import json
import os
import random
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import orchestrator as O    # 공용 로테이션(rotate_jsonl) · 지하철 셀(bump_subway) · 공휴일(is_holiday)

KST = timezone(timedelta(hours=9))

BASE = "http://swopenAPI.seoul.go.kr/api/subway"
PREFIX = "subway"       # jsonl 파일 접두사 (rotate_jsonl 이 이 이름으로 백업/삭제)
PAGE = 1000             # 1콜 최대 건수 (API 상한)
ARRIVED = ("0", "1", "2")   # arvlCd — 그 역에 실재: 0진입·1도착·2출발. 3/4/5(전역)·99(운행중) 제외

# 일괄 응답의 subwayNm 이 null 이라 자체 매핑 (✅ 실측 19개 노선)
LINES = {
    "1001": "1호선", "1002": "2호선", "1003": "3호선", "1004": "4호선", "1005": "5호선",
    "1006": "6호선", "1007": "7호선", "1008": "8호선", "1009": "9호선",
    "1032": "GTX-A", "1063": "경의중앙선", "1065": "공항철도", "1067": "경춘선",
    "1075": "수인분당선", "1077": "신분당선", "1081": "경강선",
    "1092": "우이신설선", "1093": "서해선", "1094": "신림선",
}

# 1,000회/일 한도에 마진을 둔다. ⚠️ 운행창 20시간(72,000s) ÷ 950회 = 75.8초 —
# 75로 내림하면 캡이 운행창보다 먼저 차서(950×75s=19.8h) 하필 막차 직전 ~12분이
# 매일 비었다. 76으로 올림해 캡보다 창이 먼저 끝나게 한다 (950×76s=20.06h ≥ 20h).
INTERVAL_SEC = 76
DAILY_CAP = 950

# 운행시간 밖은 열차가 없다 — 콜만 태우므로 건너뛴다
SERVICE_START_H = 5
SERVICE_END_H = 1  # 익일 01시

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, "data")
ENV_FILE = os.path.join(HERE, "..", "..", "bus-test", ".env.local")


def load_key(envname="SEOUL_SUBWAY_KEY"):
    """env 변수 또는 .env 에서 인증키를 읽는다 (노선마다 다른 키를 줄 수 있게 envname 파라미터).

    launchd 로 뜬 프로세스는 ~/Desktop 을 못 읽는다(macOS TCC) → 자기 폴더 .env 를 본다.
    """
    key = os.environ.get(envname)
    if key:
        return key
    for path in (os.path.join(HERE, ".env"), ENV_FILE):
        try:
            with open(path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith(envname + "="):
                        return line.split("=", 1)[1].strip().strip('"').strip("'")
        except OSError:
            continue
    return None


def now():
    return datetime.now(KST)


def in_service(t):
    h = t.hour
    return h >= SERVICE_START_H or h < SERVICE_END_H


def service_day(t):
    """01:30 에 잡힌 열차는 전날 운행분이다. 04시 기준으로 하루를 가른다."""
    return (t - timedelta(hours=4)).strftime("%Y-%m-%d")


def daytype(t):
    """지하철은 3종(weekday/sat/sun) — 시각표가 요일별이라 이 단위로 수렴한다.
    ⚠️ 버스(7종)와 다르다: 지하철은 trainNo 가 매일 같은 시각표라 요일별 며칠이면
    수렴하고, §8 #3(토요일=평일 시각표인가 공휴일인가)을 이 3분리가 답한다."""
    wd = (t - timedelta(hours=4)).weekday()
    return "sun" if wd == 6 else "sat" if wd == 5 else "weekday"


def fetch_page(key, start, end):
    """전 노선 도착정보 일괄 — /{start}/{end}/ALL. ⚠️ 노선명이 아니라 ALL 이고
    start/end 는 rowNum 범위다 (✅ 실측: 0/1000 → 1~1000, 1000/2000 → 1000~2000)."""
    url = f"{BASE}/{key}/json/realtimeStationArrival/{start}/{end}/ALL"
    with urllib.request.urlopen(url, timeout=25) as r:
        return json.loads(r.read().decode())


def rows_of(payload):
    """(행, 총건수, 에러). 총건수로 남은 페이지 수를 정한다 (총 ~2,916건 → 3페이지)."""
    err = (payload.get("errorMessage") or {})
    rows = payload.get("realtimeArrivalList")
    if rows:
        return rows, err.get("total") or 0, None
    return [], 0, f"{err.get('code')} {err.get('message')}"


# ── 일 호출수는 디스크에 남긴다 ─────────────────────────────────────────
# LaunchAgent 가 KeepAlive 로 재시작하면 메모리 카운터는 0으로 리셋된다.
# 맥이 잠들었다 깨거나 크래시가 몇 번 나면 1,000회 상한을 넘겨 그날 수집이 죽는다.
# ⚠️ 키는 운행일(04시)이 아니라 **달력일** — 서울 열린데이터광장 쿼터가 자정에 리셋된다.
#    운행일로 세면 04시 리셋 시점에 API 는 00-01시 콜(~48)을 이미 새 날로 세고 있어
#    950 + 48 = 998/1,000 으로 마진이 사실상 없었다.

def quota_day(t):
    return t.strftime("%Y-%m-%d")


# 카운터는 **키(env 변수명) 단위** — 한 키를 여러 노선이 공유하면 합산돼 상한을 지킨다.
def calls_path(keyid, day):
    return os.path.join(OUT_DIR, f".subwaycalls-{keyid}-{day}")


def read_calls(keyid, day):
    try:
        with open(calls_path(keyid, day)) as f:
            return int(f.read().strip() or 0)
    except (OSError, ValueError):
        return 0


def bump_calls(keyid, day):
    n = read_calls(keyid, day) + 1
    tmp = calls_path(keyid, day) + ".tmp"
    with open(tmp, "w") as f:
        f.write(str(n))
    os.replace(tmp, calls_path(keyid, day))  # 원자적 — 재시작 중 깨진 카운터 방지
    return n


def seed_today(day):
    """재시작 복원 — 오늘 jsonl 에서 마지막 상태와 (열차,역) dedup 집합을 되살린다.

    ⚠️ 이게 없으면 KeepAlive/systemd 재시작마다 메모리 dedup(bumped)이 비어
    같은 날 같은 (열차,역)을 다시 bump → subway_cell.n(관측 '일수')이 부풀어
    §8 #1 수렴 판정이 실제보다 이르게 참이 된다. 콜 카운터를 디스크에 두는 것과
    같은 이유다 — 셀 불변식("하루 1회")도 재시작을 견뎌야 한다.
    last_state 복원은 덤 — 없으면 첫 사이클에 전 열차의 중복 행이 한 벌 남는다.
    (bump 직후·commit 전에 죽은 극소수는 여기서 억제돼 미세 과소계상 쪽으로
    떨어진다 — 부풀리는 쪽보다 안전하다.)
    """
    ls, bset = {}, set()
    try:
        with open(os.path.join(OUT_DIR, f"{PREFIX}-{day}.jsonl"), encoding="utf-8") as f:
            for line in f:
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                tn, sid = r.get("trainNo"), r.get("statnId")
                if tn and sid:
                    ls[(tn, sid)] = r.get("arvlCd")
                    bset.add((r.get("line"), tn, sid))
    except OSError:
        pass
    return ls, bset


def resolve_keys():
    """config.subwayKeys(env 변수명 목록) → [(keyid, key)]. 없는 키는 건너뛴다.

    키가 많을수록 사이클이 짧아진다 — 1회 3콜(3페이지)이므로
    키 N개 = 950N 콜/일 ÷ 3 = 316N 회/일. 3키면 950회 = 76초 간격.
    """
    names = O.cfg().get("subwayKeys") or ["SEOUL_SUBWAY_KEY"]
    out = []
    for kid in names:
        k = load_key(kid)
        if k:
            out.append((kid, k))
        else:
            print(f"⚠️ 키 {kid} 없음 — 건너뜀", flush=True)
    return out


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    keys = resolve_keys()
    if not keys:
        sys.exit("지하철 인증키가 없다 — config.subwayKeys 와 .env 를 확인.")
    conn = O.connect()

    # 매일 위상을 흔든다 (§3.3.3). 고정 간격이면 매일 같은 시각만 샘플링된다.
    time.sleep(random.Random(service_day(now())).uniform(0, INTERVAL_SEC))

    day = service_day(now())
    written = 0
    # (열차,역) -> arvlCd / 오늘 bump 한 (노선,열차,역).
    # 재시작이면 오늘 jsonl 에서 복원한다 — dedup 이 메모리뿐이면 n(관측 일수)이 부푼다.
    last_state, bumped = seed_today(day)
    ki = [0]   # 키 라운드로빈 커서 (리스트 = 클로저에서 갱신)

    def take_key(qday):
        """상한 안 찬 키를 라운드로빈으로 하나 집고 카운터를 올린다. 다 찼으면 (None, None)."""
        for _ in range(len(keys)):
            kid, k = keys[ki[0] % len(keys)]
            ki[0] += 1
            if read_calls(kid, qday) < DAILY_CAP:
                bump_calls(kid, qday)   # 호출 전에 센다 — 죽어도 과다호출로 안 샌다
                return kid, k
        return None, None

    def export():
        """jsonl 2단 로테이션 — 별도 스레드 (rclone 네트워크 호출).
        노선별 파일이던 옛 이름도 같이 정리한다."""
        for p in (PREFIX, "shinbundang", "suinbundang"):
            __import__("threading").Thread(target=O.rotate_jsonl, args=(p,), daemon=True).start()

    export()   # 시작 시 1회

    print(f"[{now():%H:%M:%S}] 지하철 수집 시작 · 전 노선 일괄(ALL) · 키 {len(keys)}개"
          f"({', '.join(k for k, _ in keys)}) · {INTERVAL_SEC}s 간격 · 키당 상한 {DAILY_CAP}회", flush=True)

    while True:
        t = now()
        d = service_day(t)
        if d != day:
            print(f"[{t:%H:%M:%S}] 운행일 전환 {day} → {d} (전일 기록 {written:,}건)", flush=True)
            day, written, last_state, bumped = d, 0, {}, set()
            export()
            time.sleep(random.uniform(0, INTERVAL_SEC))
            continue
        if not in_service(t):
            time.sleep(300)
            continue

        qday = quota_day(t)
        dt = daytype(t)
        # 공휴일(운행일 기준) — 평일 다이어가 아니므로 weekday 표본에 안 섞는다.
        # jsonl 엔 그대로 남는다 (config.holidays 가 틀렸으면 고치고 재집계 가능).
        hol = O.is_holiday(t)
        path = os.path.join(OUT_DIR, f"{PREFIX}-{day}.jsonl")
        start, total, got, did_bump, capped = 0, None, 0, False, False

        while total is None or start < total:
            kid, key = take_key(qday)
            if not key:
                capped = True
                break                                   # 모든 키 상한 — 자정 리셋까지
            try:
                rows, tot, err = rows_of(fetch_page(key, start, start + PAGE))
            except Exception as e:
                print(f"[{t:%H:%M:%S}] 실패({kid}) {start}~: {type(e).__name__}: {e}", flush=True)
                break
            if err:
                print(f"[{t:%H:%M:%S}] 응답없음({kid}) {start}~: {err}", flush=True)
                break
            total = tot or len(rows)
            got += len(rows)
            with open(path, "a", encoding="utf-8") as f:
                for r in rows:
                    cd = r.get("arvlCd")
                    if cd not in ARRIVED:
                        continue                        # 전역 상태·운행중 — 그 역 관측이 아니다
                    tn, sid = r.get("btrainNo"), r.get("statnId")
                    if not tn or not sid:
                        continue
                    if last_state.get((tn, sid)) == cd:
                        continue                        # 상태 안 바뀜 → 같은 관측
                    last_state[(tn, sid)] = cd
                    line = LINES.get(r.get("subwayId"), r.get("subwayId"))
                    f.write(json.dumps({
                        "t": t.isoformat(), "line": line, "recptnDt": r.get("recptnDt"),
                        "trainNo": tn, "statnId": sid, "statnNm": r.get("statnNm"),
                        "updnLine": r.get("updnLine"), "arvlCd": cd,
                        "arvlMsg2": r.get("arvlMsg2"), "bstatnNm": r.get("bstatnNm"),
                        "lstcarAt": r.get("lstcarAt"),
                    }, ensure_ascii=False) + "\n")
                    written += 1
                    # 셀 bump — (노선,열차,역)을 오늘 처음 볼 때만 (n = 관측 일수)
                    if not hol and (line, tn, sid) not in bumped:
                        O.bump_subway(conn, line, tn, sid, dt)
                        bumped.add((line, tn, sid))
                        did_bump = True
            start += PAGE
        if did_bump:
            conn.commit()
        if capped:
            print(f"[{t:%H:%M:%S}] 전 키 일 상한 도달 — 자정 리셋까지 대기", flush=True)
            time.sleep(300)
            continue
        if int(t.timestamp()) // INTERVAL_SEC % 20 == 0:
            cal = " ".join(f"{kid}:{read_calls(kid, qday)}" for kid, _ in keys)
            print(f"[{t:%H:%M:%S}] 콜[{cal}] · 응답 {got}건 · 오늘 기록 {written:,}건 "
                  f"· 셀 {len(bumped):,}", flush=True)

        time.sleep(INTERVAL_SEC + random.uniform(-3, 3))


if __name__ == "__main__":
    main()
