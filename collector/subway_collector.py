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


def load_key():
    """자기 폴더의 .env 를 먼저 본다. (plist 에 키를 박지 않기 위해)

    launchd 로 뜬 프로세스는 ~/Desktop 을 못 읽는다(macOS TCC).
    """
    key = os.environ.get("SEOUL_SUBWAY_KEY")
    if key:
        return key
    for path in (os.path.join(HERE, ".env"), ENV_FILE):
        try:
            with open(path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("SEOUL_SUBWAY_KEY="):
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


# ── 수집일 장부 — 요일별로 며칠 관측했나 (수집률의 분자) ──────────────────
# 지하철 수집률 = 요일별 수집 일수 / 목표. trainNo 가 매일 반복되므로 하루당
# 열차·역별 1샘플씩 쌓이고, 요일별 N일이면 정시성 분포가 나온다. 옛 jsonl 은
# gzip 으로 내보내지므로(백업) 누적 일수는 이 작은 장부에 따로 남긴다.
def days_ledger_path():
    return os.path.join(OUT_DIR, ".subway-days.json")


def record_day(dt, day):
    """오늘(운행일)을 그 요일 목록에 넣는다 — 이미 있으면 no-op (중복 방지)."""
    p = days_ledger_path()
    try:
        led = json.load(open(p, encoding="utf-8"))
    except (OSError, ValueError):
        led = {}
    days = set(led.get(dt, []))
    if day in days:
        return
    days.add(day)
    led[dt] = sorted(days)
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(led, f)
    os.replace(tmp, p)


def fetch(key):
    url = f"{BASE}/{key}/json/realtimePosition/0/1000/{urllib.parse.quote(LINE)}"
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


def calls_path(day):
    return os.path.join(OUT_DIR, f".calls-{day}")


def read_calls(day):
    try:
        with open(calls_path(day)) as f:
            return int(f.read().strip() or 0)
    except (OSError, ValueError):
        return 0


def bump_calls(day):
    n = read_calls(day) + 1
    tmp = calls_path(day) + ".tmp"
    with open(tmp, "w") as f:
        f.write(str(n))
    os.replace(tmp, calls_path(day))  # 원자적 — 재시작 중 깨진 카운터 방지
    return n


def main():
    key = load_key()
    if not key:
        sys.exit(f"SEOUL_SUBWAY_KEY 없음. export 하거나 {ENV_FILE} 에 넣으세요.")

    os.makedirs(OUT_DIR, exist_ok=True)

    # 매일 위상을 흔든다 (§3.3.3). 고정 간격이면 매일 같은 시각만 샘플링된다.
    jitter_base = random.Random(service_day(now())).uniform(0, INTERVAL_SEC)
    time.sleep(jitter_base)

    day = service_day(now())
    written = 0
    recorded = None  # 오늘을 수집일 장부에 이미 넣었는지 (하루 1회만 쓰면 됨)
    last_state = {}  # trainNo -> (statnId, trainSttus)

    def export(t):
        """오늘·어제만 남기고 shinbundang-*.jsonl 을 gzip → exportDir (구글드라이브 백업).
        지하철 파일은 작아 gzip 이 즉시라 스레드 불필요."""
        keep = {service_day(t), service_day(t - timedelta(days=1))}
        O.export_old_jsonl("shinbundang", keep)

    export(now())   # 시작 시 1회 — 꺼져 있는 동안 쌓인 옛 파일 정리

    print(f"[{now():%H:%M:%S}] 수집 시작 · {LINE} · {INTERVAL_SEC}s 간격 · "
          f"일 상한 {DAILY_CAP}회 (오늘 이미 {read_calls(quota_day(now()))}회 사용)", flush=True)

    while True:
        t = now()
        d = service_day(t)

        if d != day:  # 날이 바뀌면 위상 초기화 (카운터는 파일이 날짜별이라 자동)
            print(f"[{t:%H:%M:%S}] 운행일 전환 {day} → {d} (전일 {read_calls(day)}콜 / {written}건)", flush=True)
            day, written, last_state = d, 0, {}
            export(t)   # 그저께가 된 파일을 백업으로 내보낸다
            time.sleep(random.uniform(0, INTERVAL_SEC))
            continue

        if not in_service(t):
            time.sleep(300)
            continue

        qday = quota_day(t)  # 쿼터는 달력일 — 운행일(day)과 자정~04시에 갈린다
        calls = read_calls(qday)
        if calls >= DAILY_CAP:
            print(f"[{t:%H:%M:%S}] 일 상한 {calls}/{DAILY_CAP} 도달 — 자정 리셋까지 대기", flush=True)
            time.sleep(300)
            continue

        try:
            calls = bump_calls(qday)  # 호출 전에 센다 — 죽어도 과다호출로 새지 않게
            payload = fetch(key)
            rows, err = rows_of(payload)
            if err:
                # 열차 0대(심야)와 진짜 에러를 구분해서 남긴다
                print(f"[{t:%H:%M:%S}] 응답없음: {err} (콜 {calls})", flush=True)
            else:
                path = os.path.join(OUT_DIR, f"shinbundang-{day}.jsonl")
                with open(path, "a", encoding="utf-8") as f:
                    for r in rows:
                        tn = r.get("trainNo")
                        state = (r.get("statnId"), r.get("trainSttus"))
                        if last_state.get(tn) == state:
                            continue  # 상태 안 바뀜 → 같은 관측
                        last_state[tn] = state
                        f.write(json.dumps({
                            "t": t.isoformat(),
                            "recptnDt": r.get("recptnDt"),
                            "trainNo": tn,
                            "statnId": r.get("statnId"),
                            "statnNm": r.get("statnNm"),
                            "statnTid": r.get("statnTid"),
                            "statnTnm": r.get("statnTnm"),
                            "updnLine": r.get("updnLine"),
                            "trainSttus": r.get("trainSttus"),
                            "directAt": r.get("directAt"),
                            "lstcarAt": r.get("lstcarAt"),
                        }, ensure_ascii=False) + "\n")
                        written += 1
                if written and recorded != day:
                    record_day(daytype(t), day)   # 오늘 실관측 확보 → 수집일 장부에 1회 기록
                    recorded = day
                if calls % 20 == 0:
                    print(f"[{t:%H:%M:%S}] 콜 {calls}/{DAILY_CAP} · 기록 {written}건 · 열차 {len(rows)}대", flush=True)
        except Exception as e:
            print(f"[{t:%H:%M:%S}] 실패: {type(e).__name__}: {e}", flush=True)

        time.sleep(INTERVAL_SEC + random.uniform(-3, 3))


if __name__ == "__main__":
    main()
