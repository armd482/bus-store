#!/usr/bin/env python3
"""
버스 위치 수집기 — stop_times 재료 (docs §4.4, §5)

이 데이터는 C 아키텍처(자체 GTFS)용이다. 현재 채택된 B+ 는 TMAP sectionTime 을
쓰므로 필요 없다. → C 를 되살릴 선택지를 살려두기 위한 수집.

노선 선택은 orchestrator 가 한다 — 이미 목표를 채운 구간을 재폴링하지 않고
미커버 노선으로 옮겨간다. 그래야 총 소요가 달력이 아니라 커버리지에 묶인다.

핵심 사실 (✅ §3.1 실측):
  - 해상도는 정류장 단위다. gpslati/gpslong 은 버스 GPS 가 아니라 "현재 정류장 좌표"이고
    정류장을 넘을 때만 바뀐다. → 우리가 필요한 건 통과 시각이므로 이걸로 충분.
  - 타임스탬프 필드가 없다. 통과 시각은 (t_prev, t] 로만 좁혀진다. 둘 다 기록한다.
  - 30초 폴링이면 이동의 94.7%를 1칸으로 잡는다 (2칸 이상 건너뜀 5.2%).
"""

import json
import os
import random
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import orchestrator as O

BASE = "https://apis.data.go.kr/1613000/BusLcInfoInqireService/getRouteAcctoBusLcList"
REPICK_EVERY = 120  # 사이클마다 노선 재선정 (약 1시간)

# server.py 가 대시보드용으로 주입한다. 단독 실행 시엔 로컬 더미.
STATE = {"errors": {}}
LOCK = __import__("threading").Lock()


def _load_key(envname):
    """자기 폴더의 .env 를 먼저 본다 — launchd 는 ~/Desktop 을 못 읽는다(macOS TCC)."""
    v = os.environ.get(envname)
    if v:
        return v
    for p in (os.path.join(O.HERE, ".env"),
              os.path.join(O.HERE, "..", "..", "bus-test", ".env.local")):
        try:
            for line in open(p, encoding="utf-8"):
                if line.strip().startswith(envname + "="):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
        except OSError:
            continue
    return None


def load_key():
    return _load_key("GBIS_BUS_KEY")


def load_keys():
    """config.busKeys(env 변수명 목록) → 실제 키 리스트. 없는 건 건너뛴다.

    ★ TAGO 세션 30·rate 한도는 **키(=계정) 단위**다 (✅ 2026-07-23 실측: 같은 EC2
    IP 에서 키1·키2 를 나란히 던져 처리량 2.16배, 키2 는 429 0건). 두 키를 쓰면
    같은 IP 로도 동시 세션 60·처리량 2배 → 커버 속도 2배, 수집 기간 절반.
    쿼터도 계정마다 별개라 두 배가 된다.
    """
    names = O.cfg().get("busKeys") or ["GBIS_BUS_KEY"]
    out = [(nm, _load_key(nm)) for nm in names]
    return [(nm, k) for nm, k in out if k]


def now():
    return datetime.now(O.KST)


def service_day(t):
    return O.service_day_of(t).strftime("%Y-%m-%d")


# ── 일 호출수는 디스크에 (KeepAlive 재시작해도 상한을 넘지 않게) ────────
# ⚠️ 키는 운행일(04시 경계)이 아니라 **달력일**이다 — data.go.kr 쿼터가 자정에 리셋된다.
#    운행일로 세면 04시에 카운터만 0이 되는데 API 는 00-04시 콜(~3.7만)을 이미 새 날로
#    세고 있어, 470,000 + 37,000 = 507,000 으로 실제 50만 상한을 넘길 수 있다.
#    (운행일 경계는 데이터 파일·요일 분류에만 쓴다 — 그쪽은 04시가 맞다.)
def quota_day(t):
    return t.strftime("%Y-%m-%d")


def calls_path(day):
    return os.path.join(O.DATA, f".buscalls-{day}")


def read_calls(day):
    try:
        return int(open(calls_path(day)).read().strip() or 0)
    except (OSError, ValueError):
        return 0


def add_calls(day, n):
    v = read_calls(day) + n
    tmp = calls_path(day) + ".tmp"
    with open(tmp, "w") as f:
        f.write(str(v))
    os.replace(tmp, calls_path(day))
    return v


def paced(fn, items, rate, workers, max_inflight, hold=0):
    """제약이 둘이라 방어도 둘이다 — 버스트(rate) 와 동시 세션(in-flight).

    ① 버스트 — 제출을 1/rate 간격으로 벌린다.
       ThreadPoolExecutor.map 은 워커 수만큼 한꺼번에 던진다. 그 버스트가
       토큰 버킷에 걸려 HTTP 429 "API token rate limit exceeded" 를 부른다.
       ✅ 실측 (같은 평균 rate, 다른 결과):
           균등 6.0/s (제출 간격 167ms) → 88건 전부 성공, 429 0건
           버스트 6.7/s (20개 동시)     → 180건 중 140건 실패
         → 문제는 rate 가 아니라 버스트다.
           균등 2/4/6 /s = 무결점. 10/s 부터 429·세션99 가 섞이기 시작(141/144).

    ② 동시 세션 30 — 세마포어로 in-flight 를 직접 센다.
       ⚠️ rate 만으로 맞추려던 게 틀렸다. in-flight = rate × 응답시간인데
          응답시간은 우리가 정하는 값이 아니다.
       ✅ 실측: 응답 중앙값 2.48s / 평균 2.26s / **최대 4.99s**.
           중앙값이면 6×2.5 = 15 로 여유롭지만, 꼬리에서 6×5 = 30 → 상한 정통.
           그래서 실패가 평균이 아니라 꼬리에서 터졌다 (170노선 사이클 3.5%).
       세마포어를 걸면 응답이 얼마나 느려지든 세션은 안 넘는다. 대신 느려지면
       제출이 알아서 막혀 사이클이 길어진다 — 데이터를 버리는 것보다 낫다.

    ③ ★ 좀비 세션 — ②의 가정("release=세션 닫힘")은 **타임아웃에서 깨진다**.
       우리가 15s 에 포기해도 TAGO 는 그 요청을 모른다 → 세션이 서버 쪽에 살아
       있다(좀비). 슬롯을 곧장 반납하면 다음 요청이 아직 살아있는 세션 위에 쌓여
       (우리 18 + 좀비 N) > 30 → code99 (✅ 2026-07-24: TAGO 지연 시 1~2노선 잔여).
       그래서 **타임아웃난 슬롯은 hold 초만큼 붙잡았다 반납**한다 — 세마포어
       카운트를 실제 TAGO 세션에 맞춘다. 타임아웃이 몰리면 그만큼 슬롯이 잠겨
       동시성이 자동으로 준다(정확히 필요한 반응). 회복되면 저절로 풀린다.
    """
    sem = __import__("threading").Semaphore(max_inflight)

    def guarded(it):
        r = None
        try:
            r = fn(it)
            return r
        finally:
            # 타임아웃이면 좀비가 TAGO 에서 빠질 시간(hold)만큼 슬롯을 더 붙잡는다.
            if hold and r is not None and r[2] and ("Timeout" in r[2] or "timed out" in r[2]):
                time.sleep(hold)
            sem.release()

    out = [None] * len(items)
    with ThreadPoolExecutor(max_workers=max(workers, max_inflight)) as ex:
        futs = {}
        for i, it in enumerate(items):
            sem.acquire()          # in-flight 상한 — 넘치면 여기서 막힌다
            futs[ex.submit(guarded, it)] = i
            time.sleep(1.0 / rate)  # 버스트 방지 — 상한에 안 걸려도 항상 벌린다
        for f, i in futs.items():
            out[i] = f.result()
    return out


def fetch_all(keys, picked, rate, workers, inflight, hold=0):
    """picked 를 키 수만큼 라운드로빈으로 나눠 **각 키로 병렬 paced**.

    세션·rate 한도가 키 단위라(load_keys 참조) 키마다 독립 세마포어·독립 rate 로
    돌린다 → 같은 IP 로도 동시성이 키 수만큼 곱해진다. 반환은 (routeid, items, err)
    리스트 (picked 순서와 무관 — 호출부가 routeid 로 매핑).
    """
    if len(keys) == 1:
        _, key = keys[0]
        return paced(lambda p: fetch(key, p["cityCode"], p["routeid"]),
                     picked, rate, workers, inflight, hold)
    groups = [picked[i::len(keys)] for i in range(len(keys))]   # 균등 라운드로빈
    parts = [None] * len(keys)

    def worker(idx):
        _, key = keys[idx]
        parts[idx] = paced(lambda p: fetch(key, p["cityCode"], p["routeid"]),
                           groups[idx], rate, workers, inflight, hold)
    ths = [__import__("threading").Thread(target=worker, args=(i,)) for i in range(len(keys))]
    for t in ths:
        t.start()
    for t in ths:
        t.join()
    return [r for part in parts for r in part]


def fetch(key, city, routeid):
    """⚠️ 실패가 HTTP 200 으로 온다. resultCode 를 반드시 볼 것.

    세션 초과 시:
      {"response":{"header":{"resultCode":99,"resultMsg":"가용한 세션이 존재하지 않습니다. (30/30)"},"body":""}}
    → HTTP 200 이고 예외도 안 난다. resultCode 를 안 보면 조용히 데이터를 버린다.
    ✅ 실측: 제약은 "30 TPS"가 아니라 **동시 세션 30개**다. 응답이 ~3초이므로
       실효 처리량은 30÷3s = 초당 10건이 상한. 우리 프로세스 전체의 **in-flight 합**이
       30을 넘으면 안 된다 (워커 수가 아니라 세마포어 maxInflight 가 지킨다 — config).
    """
    q = urllib.parse.urlencode({"serviceKey": key, "_type": "json", "cityCode": city,
                                "routeId": routeid, "numOfRows": 200})
    try:
        with urllib.request.urlopen(f"{BASE}?{q}", timeout=15) as r:
            d = json.loads(r.read().decode())
        obs = now()   # ★ 응답 수신 **직후** = 이 노선의 실제 관측시각.
        # ⚠️ 사이클 끝 시각 하나를 340노선에 다 찍으면 첫 노선은 최대 ~40s(사이클 길이)
        #    늦게 찍힌다 — 통과구간 (t_prev,t]·기점 출발시각·시각표 분석이 그만큼 왜곡된다.
        h = d.get("response", {}).get("header", {})
        code = str(h.get("resultCode", "?"))
        if code not in ("00", "0"):
            return routeid, [], f"code{code}:{h.get('resultMsg','')[:24]}", obs
        body = d["response"].get("body") or {}
        if not isinstance(body, dict):
            return routeid, [], "body_not_dict", obs
        it = (body.get("items") or {}).get("item") or []
        if isinstance(it, dict):
            it = [it]
        return routeid, it, None, obs
    except urllib.error.HTTPError as e:
        # 상태코드가 곧 원인이다: HTTP429=rate limit(키 공유·버스트), HTTP5xx=서버 장애.
        # 'HTTPError' 로 뭉치면 로그만으로 구분이 안 된다 (✅ 실전에서 아쉬웠던 것).
        return routeid, [], f"HTTP{e.code}", now()
    except Exception as e:
        return routeid, [], type(e).__name__, now()


def main():
    keys = load_keys()
    if not keys:
        sys.exit("GBIS_BUS_KEY 없음")

    k = O.cfg()
    conn = O.connect()
    bands, target, nb = k["timebands"], k["targetSamples"], len(k["timebands"])
    interval, quota, maxr = k["intervalSec"], k["dailyQuota"], k["maxRoutes"]
    workers, rate = k["maxWorkers"], k["dispatchRate"]
    inflight_max = k["maxInflight"]
    hold = k.get("busZombieHoldSec", 10)   # 타임아웃 슬롯을 붙잡을 초 (paced ③)
    # ★ in-flight 를 사이클마다 TAGO 상태에 맞춰 조절한다 (AIMD — 혼잡제어와 같은 꼴).
    #   code99(세션 30 초과)가 보이면 다음 사이클 in-flight 를 줄이고(×0.6), 깨끗한
    #   사이클이면 조금씩 올린다(+2). TAGO 가 느려지면 자동으로 물러나고 회복되면
    #   차오른다. 고정 20 은 TAGO 지연이 늘 때(2.48→3.94s) 좀비 세션과 겹쳐 code99
    #   버스트를 냈다 (✅ 2026-07-23). 하한 8 — 그 밑이면 사이클이 너무 길어진다.
    inflight = inflight_max
    window = k["serviceWindow"]

    day = service_day(now())
    # (routeid, vehicleno) -> (nodeord, 그 정류장에서 처음 본 시각).
    # ⚠️ 키에 routeid 가 필요하다 — 같은 차량이 당일 다른 노선으로 재배차되면
    #    (경기 시내버스 운영에서 실제로 있다) 이전 노선의 nodeord 가 새 노선의
    #    전이 계산에 섞여 엉뚱한 구간 통과가 기록된다. ordv<=prev 와 4×interval
    #    가드가 대부분 걸러주지만, 순번이 우연히 증가 방향이면 통과한다.
    last = {}
    picked, cyc, written = [], 0, 0

    rotated_day = None   # 로테이션을 마친 운행일 — 하루 한 번만 돌게

    def kickoff_export():
        """bus-*.jsonl 2단 로테이션(백업 어제 / 삭제 그저께) — 별도 스레드.
        rclone 네트워크 호출이 있으니 사이클을 막지 않게 스레드로 돌린다."""
        __import__("threading").Thread(
            target=O.rotate_jsonl, args=("bus",), daemon=True).start()

    print(f"[{now():%H:%M:%S}] 수집 시작 · 목표 {target}샘플 · 밴드 {nb}개 · "
          f"최대 {maxr}노선 · 키 {len(keys)}개({', '.join(nm for nm, _ in keys)}) · "
          f"상한 {quota:,} (오늘 {read_calls(quota_day(now())):,} 사용)", flush=True)
    if len(keys) < 2:
        print(f"[{now():%H:%M:%S}] ⚠️ 키 1개 — maxRoutes {maxr} 를 단일 세션풀로 돌리면 "
              f"사이클이 길어진다. GBIS_BUS_KEY2 를 .env 에 넣으면 커버 속도 2배", flush=True)

    while True:
        t = now()
        d = service_day(t)
        qday = quota_day(t)   # 쿼터는 달력일 — 운행일(d)과 자정~04시에 갈린다
        if d != day:
            print(f"[{t:%H:%M:%S}] 운행일 전환 {day} → {d}", flush=True)
            day, last, written, picked = d, {}, 0, []
            with LOCK:
                STATE["errLog"] = []   # 오류 로그 매일 초기화 — 어제 실패가 오늘 화면에 안 남게
            # emptyStreak 도 매일 리셋 — 리셋 조건이 '폴링돼서 버스가 보이는 것'뿐이라,
            # 콜드가 된 노선은 슬롯이 차 있는 한 다시 폴링될 기회가 없어 영구 고착된다
            # (성긴 배차 노선은 운행 중에도 6사이클 연속 0대가 가능하다). 하루 단위로
            # 재기회를 주면 고착이 최대 하루로 묶인다.
            conn.execute("UPDATE route SET emptyStreak = 0")
            conn.commit()
            continue
        # 로테이션은 운행일 경계(04시)가 아니라 rotateHour(기본 6시)에 — 경계를 걸친
        # 사이클이 아직 전날 파일에 쓰고 있을 수 있고, 04시는 첫차·전환이 겹친다.
        due = O.rotate_due(rotated_day, t)
        if due:
            rotated_day = due
            kickoff_export()   # 어제 백업 + 그저께 삭제(백업 확인 후)
        if not O.in_window(t, window):
            # serviceWindow 밖 — 현재 설정은 [0,24](24시간)라 이 분기는 안 탄다.
            # ⚠️ 전수 실측상 "버스가 0인 시간대는 없다"(config _serviceWindow —
            #    '03-04시는 0'이라던 초기 판단은 194개 표본의 오판). 창을 좁힐 때만 유효.
            with LOCK:
                STATE["night"] = True
            time.sleep(300)
            continue

        # ⚠️ 심야 노선 수를 상수로 정하지 않는다. 운행시간 필터 + 관측(emptyStreak)이
        #    실제로 도는 것만 남긴다. ✅ 실측상 01시에 도는 노선은 17~418개 사이인데
        #    (노선 소요시간을 몰라 추정 폭이 크다) 그걸 40 같은 숫자로 못 박으면
        #    05:16 까지 달리는 36번을 버리고 차고에 있는 낮 노선을 찍게 된다.
        want = maxr

        # 노선 재선정 — 채운 노선은 빠지고 미커버가 들어온다
        # ⚠️ len(picked) != want 로 매 사이클 재선정하지 않는다 — 심야엔 운행 노선이
        #    상한보다 적은 게 정상이라, 그 조건이면 밤새 사이클마다 무거운 커버리지
        #    쿼리(cell 전체 GROUP BY)를 돌리고 로그도 도배된다. 모자랄 땐
        #    10사이클(~7분)마다만 다시 본다 — 새벽 운행 재개도 그 안에 잡힌다.
        if not picked or cyc % REPICK_EVERY == 0 or (len(picked) < want and cyc % 10 == 0):
            picked = O.pick_routes(conn, want, target, nb, t=t)
            if not picked:
                # ⚠️ 빈 풀과 완주를 구분한다. 안 그러면 새로 배포한 사람이
                #    "모든 노선 완주"를 보고 정상인 줄 안다.
                pool = conn.execute("SELECT COUNT(*) FROM route").fetchone()[0]
                if pool == 0:
                    print(f"[{t:%H:%M:%S}] ❌ 노선 풀이 비어 있다 — 먼저 `python3 fetch_routes.py` 를 돌릴 것",
                          flush=True)
                    time.sleep(60)
                else:
                    print(f"[{t:%H:%M:%S}] 폴링할 노선 없음 (풀 {pool:,}) — "
                          f"전부 완주했거나 지금 운행 중인 노선이 없다", flush=True)
                    time.sleep(600)
                continue
            print(f"[{t:%H:%M:%S}] 노선 재선정: {len(picked)}개 "
                  f"(충전율 {picked[0]['fill']*100:.1f}% ~ {picked[-1]['fill']*100:.1f}%)", flush=True)

        if read_calls(qday) + len(picked) > quota:
            print(f"[{t:%H:%M:%S}] 일 상한 근접 — 대기", flush=True)
            time.sleep(300)
            continue

        started = time.time()
        with LOCK:
            STATE["fetching"] = True
        meta = {p["routeid"]: p for p in picked}
        # ★ 쿼터는 호출 **전**에 예약 기록한다 [리뷰 R1 #4]. 사이클 도중 프로세스가
        #   죽으면(자동 재시작) 이미 나간 콜이 장부에서 빠져 재시작 후 실제 API 사용량이
        #   카운터보다 커지고, 반복되면 쿼터를 넘긴다. 조금 과다계상(죽어서 못 보낸 콜까지
        #   센다)이 쿼터 초과보다 안전하다.
        add_calls(qday, len(picked))
        results = fetch_all(keys, picked, rate, workers, inflight, hold)

        # code99(세션 고갈)는 일시적이다 — 꼬리 지연이 세션 30개를 채우는 순간에만
        # 몰리고 몇 초 뒤엔 풀린다 (✅ 실측: 사이클별 실패 7→8→0). rate/inflight 를
        # 더 조이면 사이클만 길어지므로, 실패분만 잠깐 뒤에 한 번 더 흘린다.
        failed = [meta[rid] for rid, _, err, _ in results if err]
        if failed and read_calls(qday) + len(failed) <= quota:
            add_calls(qday, len(failed))   # 재시도도 호출 전에 예약 기록
            # ⚠️ 재시도를 곧장 20개 동시로 던지면 code99(세션 30 초과)가 재발한다.
            #    타임아웃난 요청은 우리가 포기해도 TAGO 쪽 세션이 살아 있어(좀비),
            #    새 20개 + 좀비 > 30 → code99 연쇄. 실제로 이 연쇄가 75/170 같은
            #    버스트를 만든다 (✅ 2026-07-23: TAGO 지연 2.48→3.94s 로 꼬리가
            #    15s 를 넘기 시작하면서 악화). 그래서 실패 성격에 따라 물러선다:
            #    - code99 가 섞였으면 세션이 빠지도록 오래 쉬고(6s) in-flight 를 반으로
            #    - 타임아웃뿐이면 짧게(2s), 동시성 그대로
            has99 = any("code99" in err for _, _, err, _ in results if err)
            pause = 6 if has99 else 2
            rinflight = max(6, inflight // 2) if has99 else inflight
            time.sleep(pause)
            retry = {rid: (rid, items, err, o)
                     for rid, items, err, o in fetch_all(keys, failed, rate, workers, rinflight, hold)}
            results = [retry.get(r[0], r) if r[2] else r for r in results]
        cyc += 1

        cyc_obs = now()   # 사이클 종료 시각 — 로그·STATE 용 (데이터의 t 는 노선별 obs)
        rows, bumps = [], []
        moving = 0
        errs = {}

        for routeid, items, err, obs in results:   # ★ obs = 이 노선의 실제 응답시각
            if err:
                errs[err] = errs.get(err, 0) + 1
                continue
            # 밴드·요일·공휴일을 **노선별 관측시각**으로 계산 — 사이클이 밴드/운행일
            # 경계를 걸치면(08:59~09:01, 03:59~04:01) 노선마다 다르게 떨어져야 정확하다.
            band = O.band_of(obs, bands)
            dtype = O.day_type(obs)
            hol = O.is_holiday(obs)
            O.mark_empty(conn, routeid, not items)   # 추정이 아니라 관측이 정한다
            moving += len(items)
            for b in items:
                v, ordv = b.get("vehicleno"), b.get("nodeord")
                try:
                    ordv = int(ordv)
                except (TypeError, ValueError):
                    continue  # 순번 없는 항목 — 전이 계산 불가
                if not v:
                    continue  # 차량번호 없음 — last[None] 으로 서로 다른 버스가 섞인다
                vk = (routeid, v)          # 노선까지 포함해야 재배차 오염이 없다
                prev = last.get(vk)
                last[vk] = (ordv, obs)
                if prev is None or ordv <= prev[0]:
                    # 처음 보거나, 같은 정류장이거나, **역방향** — 역방향은 회차 아티팩트다:
                    # 종점 도착 후 재출발하면 ord 가 168→1 로 떨어진다 (✅ 실측 0.07%).
                    # 물리적 전이가 아니므로 버린다. last 는 갱신했으므로 새 운행분
                    # (1→2→…)은 다음 사이클부터 정상 기록된다.
                    continue
                if (obs - prev[1]).total_seconds() > interval * 4:
                    # ⚠️ 유령 통과 방지 — 노선이 재선정에서 빠졌다 돌아오면 prev 가
                    #    1시간+ 전 것이다. 그걸 전이로 치면 폭 1시간짜리 '통과'가 정상
                    #    샘플처럼 셀에 들어간다 (최악: 우연히 인접 정류장이면 감지 불가).
                    #    첫 관측으로 취급하고 버린다. 4×interval 인 이유: 실패 1사이클
                    #    (~90s 공백)은 기존처럼 기록하고, 그 이상 공백만 자른다.
                    continue
                # 최소 필드만 저장한다 (행 381B → ~220B).
                #   필수: 통과 구간 (t_prev, t] + 차량(소요시간 체인 키) + 노선/구간
                #   참고: band/daytype — 행 단독 해석용. rebuild 는 t 에서 현 규칙으로 재계산
                #   안전: nodeid — ord 는 노선 개편 시 흔들릴 수 있다
                # routeno/routetp/cityCode/nodenm/좌표는 coverage.sqlite 의
                # route 테이블에서 routeid 로 조인한다 — 행마다 반복 저장하지 않는다.
                rows.append({
                    "t": obs.isoformat(), "t_prev": prev[1].isoformat(),
                    "routeid": routeid, "vehicleno": v,
                    "from_ord": prev[0], "to_ord": ordv,
                    "nodeid": b.get("nodeid"),
                    "band": band, "daytype": dtype,
                })
                if band is not None and not hol:
                    # 장부는 **인접 구간 단위**로 계상한다. 2칸 이상 건너뛴 전이(5.2%)를
                    # (31,33) 같은 비인접 셀로 넣으면 분모(인접 구간수 = nstops-1)와
                    # 어긋나고, 정류장 간격이 짧아 늘 건너뛰어지는 구간은 영영 미충족으로
                    # 남는다. 통과는 (t_prev, t] 안에서 전부 일어났으므로 사이의 각 인접
                    # 구간에 1관측씩 준다 — 구간 폭이 넓은 관측일 뿐 거짓은 아니다.
                    # 원본 행(jsonl)은 전이 그대로 둔다. rebuild 도 같은 분해를 쓴다.
                    sday = service_day(obs)   # 이 관측의 운행일 (n_days 계산용)
                    for o in range(prev[0], ordv):
                        bumps.append((routeid, o, o + 1, band, dtype, sday))

        if rows:
            with open(os.path.join(O.DATA, f"bus-{day}.jsonl"), "a", encoding="utf-8") as f:
                for r in rows:
                    f.write(json.dumps(r, ensure_ascii=False) + "\n")
            written += len(rows)
        if bumps:
            for a in bumps:
                O.bump(conn, *a)
        # bumps 가 없어도 커밋 — mark_empty 의 emptyStreak 갱신이 트랜잭션에 걸려 있다.
        # 조건부로 두면 전이 0건인 심야에 쓰기 트랜잭션이 몇 분씩 열린 채 유지되고
        # (WAL 비대 + 다른 쓰기 차단) 크래시 시 그 갱신들이 유실된다.
        conn.commit()

        took = time.time() - started
        with LOCK:
            STATE["cycles"] = cyc
            STATE["lastObs"] = time.time()
            STATE["lastCycleSec"] = took
            STATE["picked"] = len(picked)
            STATE["moving"] = moving
            STATE["written"] = written
            STATE["errors"] = errs
            STATE["retried"] = len(failed)
            STATE["night"] = False
            STATE["fetching"] = False
            # 잔여 실패(재시도 후)만 이력에 남긴다 — 대시보드가 최근 오류 리스트로 보여준다.
            # 회복된 재시도는 오류가 아니므로 제외. 최근 50건만 유지(메모리 상한).
            if errs:
                log = STATE.setdefault("errLog", [])
                detail = " ".join(f"{k}×{v}" for k, v in sorted(errs.items(), key=lambda x: -x[1]))
                log.append({"t": time.time(), "n": sum(errs.values()),
                            "picked": len(picked), "detail": detail})
                del log[:-50]

        # 실패는 항상 보인다. 조용히 데이터를 버리는 게 제일 나쁘다.
        nerr = sum(errs.values())
        ok = len(picked) - nerr
        rec = len(failed) - nerr  # 재시도로 회복된 수
        mem = O.rss_mb()
        print(f"[{cyc_obs:%H:%M:%S}] 응답 {ok}/{len(picked)}노선 · 운행 {moving}대 · "
              f"통과 +{len(rows)} (누적 {written:,}) · {took:.0f}s"
              + (f" · {mem:.0f}MB" if mem else "")
              + (f" · 재시도 {len(failed)}→회복 {rec}" if failed else ""), flush=True)
        if errs:
            # 재시도로 회복 못 한 것만 여기 온다 — 이 줄은 server.log 에도 남으므로
            # (⚠️ 표식) 원인을 자르지 않고 전부 기록한다. 회복된 재시도는 위
            # 사이클 줄(콘솔 전용)에만 나온다.
            detail = " ".join(f"{k}×{v}" for k, v in sorted(errs.items(), key=lambda x: -x[1]))
            print(f"[{cyc_obs:%H:%M:%S}] ⚠️ 실패 {nerr}/{len(picked)} (재시도 후) — {detail}", flush=True)
        if cyc % 20 == 0:
            print(f"[{cyc_obs:%H:%M:%S}] 콜 {read_calls(qday):,}/{quota:,}", flush=True)

        # ★ 다음 사이클 in-flight 조절 (AIMD). code99 는 세션 30 초과라 곧장 물러나고
        #   (×0.6), 깨끗하면 천천히 차오른다(+2). errs 에 잔여 code99 가 없어도 이번
        #   사이클에서 code99 를 겪었을 수 있으므로 원본 results 로 판단한다.
        saw99 = any("code99" in (err or "") for _, _, err in results)
        prev_inflight = inflight
        if saw99:
            inflight = max(8, int(inflight * 0.6))
        elif inflight < inflight_max:
            inflight = min(inflight_max, inflight + 2)
        if inflight != prev_inflight:
            print(f"[{cyc_obs:%H:%M:%S}] in-flight {prev_inflight}→{inflight} "
                  f"({'code99 후퇴' if saw99 else '회복'})", flush=True)

        # 지터는 max 안에. 밖에 두면 took > interval 일 때 음수가 되어 죽는다.
        time.sleep(max(1.0, interval - took + random.uniform(-2, 2)))


if __name__ == "__main__":
    main()
