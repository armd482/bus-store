#!/usr/bin/env python3
"""
신분당선 실시간 위치 수집기 — 시각표 복원용 (docs/transit-routing-gtfs.md §3.3.3)

서울교통공사 15098251은 1~9호선만이라 신분당선 시각표가 없다.
realtimePosition 을 누적해 trainNo 별 역 통과 시각을 복원한다.

핵심 제약 (§3.3.2):
  - 1,000회/일 하드 리밋. 초과하면 그날 수집이 죽는다 → DAILY_CAP 으로 방어
  - 1회 1,000건이라 노선 전체 열차를 한 응답으로 받는다 → 노선당 1콜
  - 갱신 10~20초. 72초보다 촘촘히 불러도 새 정보가 별로 없다

핵심 설계:
  - 매일 시작 오프셋을 흔든다. 고정하면 매일 같은 위상만 찍혀 해상도가 안 오른다
  - 열차 상태가 바뀔 때만 기록한다 (trainNo, statnId, trainSttus)
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

import orchestrator as O    # 공용 내보내기(export_old_jsonl) — jsonl gzip → exportDir

KST = timezone(timedelta(hours=9))

LINE = "신분당선"
BASE = "http://swopenAPI.seoul.go.kr/api/subway"

# 1,000회/일 한도에 마진을 둔다. 20시간 운행 ÷ 950회 ≈ 75초
INTERVAL_SEC = 75
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


def fetch(key, line):
    url = f"{BASE}/{key}/json/realtimePosition/0/1000/{urllib.parse.quote(line)}"
    with urllib.request.urlopen(url, timeout=20) as r:
        return json.loads(r.read().decode())


def rows_of(payload):
    rows = payload.get("realtimePositionList")
    if rows:
        return rows, None
    err = (payload.get("errorMessage") or {})
    return [], f"{err.get('code')} {err.get('message')}"


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


def resolve_lines():
    """config.subwayLines → [{name, prefix, keyid, key}]. 키 없는 노선은 건너뛴다."""
    k = O.cfg()
    lines = k.get("subwayLines") or [{"name": LINE, "prefix": "shinbundang", "key": "SEOUL_SUBWAY_KEY"}]
    out = []
    for ln in lines:
        keyid = ln.get("key", "SEOUL_SUBWAY_KEY")
        key = load_key(keyid)
        if not key:
            print(f"⚠️ {ln['name']}: 키 {keyid} 없음 — 건너뜀", flush=True)
            continue
        out.append({"name": ln["name"], "prefix": ln.get("prefix", "shinbundang"),
                    "keyid": keyid, "key": key})
    return out


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    lines = resolve_lines()
    if not lines:
        sys.exit("수집할 지하철 노선이 없다 — config.subwayLines 와 .env 의 키를 확인.")
    conn = O.connect()

    # 매일 위상을 흔든다 (§3.3.3). 고정 간격이면 매일 같은 시각만 샘플링된다.
    time.sleep(random.Random(service_day(now())).uniform(0, INTERVAL_SEC))

    day = service_day(now())
    written = {ln["name"]: 0 for ln in lines}
    last_state = {ln["name"]: {} for ln in lines}      # 노선별 trainNo -> (statnId, trainSttus)
    bumped = {ln["name"]: set() for ln in lines}       # 노선별 오늘 셀 bump 한 (trainNo, statnId) — 하루 1회

    def export():
        """노선별 jsonl 2단 로테이션 — 별도 스레드 (rclone 네트워크 호출)."""
        for ln in lines:
            __import__("threading").Thread(
                target=O.rotate_jsonl, args=(ln["prefix"],), daemon=True).start()

    export()   # 시작 시 1회

    names = " · ".join(f"{ln['name']}({ln['keyid']})" for ln in lines)
    print(f"[{now():%H:%M:%S}] 지하철 수집 시작 · {names} · {INTERVAL_SEC}s 간격 · 노선당 상한 {DAILY_CAP}회", flush=True)

    while True:
        t = now()
        d = service_day(t)
        if d != day:
            print(f"[{t:%H:%M:%S}] 운행일 전환 {day} → {d}", flush=True)
            day = d
            for ln in lines:
                written[ln["name"]] = 0
                last_state[ln["name"]] = {}
                bumped[ln["name"]] = set()
            export()
            time.sleep(random.uniform(0, INTERVAL_SEC))
            continue
        if not in_service(t):
            time.sleep(300)
            continue

        qday = quota_day(t)
        dt = daytype(t)
        did_bump = False
        for ln in lines:
            name, keyid = ln["name"], ln["keyid"]
            if read_calls(keyid, qday) >= DAILY_CAP:
                continue                               # 이 키의 상한 도달 — 다음 노선
            try:
                bump_calls(keyid, qday)                # 호출 전에 센다
                rows, err = rows_of(fetch(ln["key"], name))
                if err:
                    print(f"[{t:%H:%M:%S}] {name} 응답없음: {err}", flush=True)
                    continue
                path = os.path.join(OUT_DIR, f"{ln['prefix']}-{day}.jsonl")
                ls, bset = last_state[name], bumped[name]
                with open(path, "a", encoding="utf-8") as f:
                    for r in rows:
                        tn, sid = r.get("trainNo"), r.get("statnId")
                        state = (sid, r.get("trainSttus"))
                        if ls.get(tn) == state:
                            continue                   # 상태 안 바뀜 → 같은 관측
                        ls[tn] = state
                        f.write(json.dumps({
                            "t": t.isoformat(), "line": name, "recptnDt": r.get("recptnDt"),
                            "trainNo": tn, "statnId": sid, "statnNm": r.get("statnNm"),
                            "statnTid": r.get("statnTid"), "statnTnm": r.get("statnTnm"),
                            "updnLine": r.get("updnLine"), "trainSttus": r.get("trainSttus"),
                            "directAt": r.get("directAt"), "lstcarAt": r.get("lstcarAt"),
                        }, ensure_ascii=False) + "\n")
                        written[name] += 1
                        # 셀 bump — (노선,열차,역)을 오늘 처음 볼 때만 (n = 관측 일수)
                        if tn and sid and (tn, sid) not in bset:
                            O.bump_subway(conn, name, tn, sid, dt)
                            bset.add((tn, sid))
                            did_bump = True
            except Exception as e:
                print(f"[{t:%H:%M:%S}] {name} 실패: {type(e).__name__}: {e}", flush=True)
        if did_bump:
            conn.commit()
        cal = " ".join(f"{ln['name']}:{read_calls(ln['keyid'], qday)}" for ln in lines)
        if int(t.timestamp()) // INTERVAL_SEC % 20 == 0:
            wr = " ".join(f"{n}:{w}" for n, w in written.items())
            print(f"[{t:%H:%M:%S}] 콜[{cal}] 기록[{wr}]", flush=True)

        time.sleep(INTERVAL_SEC + random.uniform(-3, 3))


if __name__ == "__main__":
    main()
