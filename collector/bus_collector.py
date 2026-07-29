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
import math
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import orchestrator as O
import env_config as E
from http_pool import KeyedHTTPSPool
from rolling_dispatcher import RollingDispatcher

BASE = "https://apis.data.go.kr/1613000/BusLcInfoInqireService/getRouteAcctoBusLcList"
BASE_HOST = "apis.data.go.kr"
BASE_PATH = "/1613000/BusLcInfoInqireService/getRouteAcctoBusLcList"
RESERVE_BLOCK = 64  # 쿼터 선예약 블록(reserve 참조). 파일 교체를 ~1/64로 줄이고
                    # 크래시 시 키당 최대 63콜 과다계상(과소계상 없음).
COUNTER_CHECK_SEC = 600  # 구버전 합계 장부 migration 안전망. 시작·자정 외 10분마다.
REPICK_EVERY = 40  # 40×interval마다 정기 재선정 (~21분). 밴드 변경 시 즉시 재선정
                   # — 현재 밴드·요일 부족량으로 고르므로(pick_routes) 패널을 오래 고정하면
                   # 밴드가 넘어간 뒤 옛 밴드를 계속 노려 목적함수가 무력화된다 [리뷰 R2].

# server.py 가 대시보드용으로 주입한다. 단독 실행 시엔 로컬 더미.
STATE = {"errors": {}}
LOCK = __import__("threading").Lock()
_CALL_LOCK = __import__("threading").Lock()
_HTTP_POOL = None


def _load_key(envname):
    """환경변수 → collector/.env → 루트 .env 순으로 읽는다.

    공통 DATA_GO_KR_KEY fallback은 첫 번째 키에만 적용한다. 두 번째 키까지 같은
    fallback을 쓰면 동일 계정을 서로 다른 키로 오인해 쿼터를 이중 계산하게 된다.
    """
    fallback = "DATA_GO_KR_KEY" if envname == "GBIS_BUS_KEY" else None
    return E.get(envname, fallback)


def load_key():
    return _load_key("GBIS_BUS_KEY")


def load_keys():
    """config.busKeys(env 변수명 목록) → 실제 키 리스트. 없는 건 건너뛴다.

    ★ TAGO 세션 30·rate 한도는 **키(=계정) 단위**다 (✅ 2026-07-23 실측: 같은 EC2
    IP 에서 키1·키2 를 나란히 던져 처리량 2.16배, 키2 는 429 0건). 두 키를 쓰면
    같은 IP 로도 동시 세션 60·처리량 2배 → 커버 속도 2배, 수집 기간 절반.
    쿼터도 계정마다 별개라 두 배가 된다.

    환경변수 이름만 다르고 실제 키 값이 같으면 독립 계정이 아니다. 그대로 두 키로
    세면 같은 세션 풀에 동시 요청을 더 넣고 쿼터 장부만 나눠 code99·하드캡 초과를
    부를 수 있으므로, 실제 값 중복은 첫 번째 하나만 활성화한다.
    """
    names = O.cfg().get("busKeys") or ["GBIS_BUS_KEY"]
    out = []
    seen = {}
    for name in names:
        key = _load_key(name)
        if not key:
            continue
        if key in seen:
            print(f"[키 설정] ⚠️ {name}은 {seen[key]}과 같은 실제 키 — 중복 제외",
                  flush=True)
            continue
        seen[key] = name
        out.append((name, key))
    return out


def now():
    return datetime.now(O.KST)


def service_day(t):
    return O.service_day_of(t).strftime("%Y-%m-%d")


def next_global_inflight(current, minimum, maximum, code99_count, attempted,
                         minimum_samples, clean_windows, recovery_windows,
                         pressure_windows, recovery_step=2):
    """EC2 egress 공통 AIMD와 다음 정상창·압력창 상태를 반환한다.

    code99는 동시성이 낮을 때도 군집 발생하므로 단일 창의 소수 오류로 후퇴하지
    않는다. 한 창에서 5% 이상이거나 최근 3창 중 2창이 1% 이상일 때만 압력으로
    확정하고, 후퇴 뒤 이력을 비워 같은 군집으로 연속 하락하지 않게 한다.
    """
    code99_rate = code99_count / max(1, attempted)
    pressure_windows = (list(pressure_windows) + [code99_rate >= 0.01])[-3:]
    overloaded = code99_rate >= 0.05 or sum(pressure_windows) >= 2
    if overloaded:
        return max(minimum, current - 4), 0, []
    if code99_count or attempted < minimum_samples:
        return current, 0, pressure_windows
    clean_windows += 1
    if clean_windows < recovery_windows or current >= maximum:
        return current, clean_windows, pressure_windows
    return min(maximum, current + max(1, int(recovery_step))), 0, pressure_windows


def counter_check_due(qday, counter_day, monotonic_now, next_check):
    """호출 장부 migration을 시작·날짜 전환·안전망 주기에만 실행한다."""
    return qday != counter_day or monotonic_now >= next_check


def quota_panel_target(t, calls, base_panel, interval, dispatch_rate,
                       planned_per_key, can_expand):
    """남은 달력일의 계획 호출량을 패널 수로 환산한다.

    이 함수는 기존 패널을 줄이지 않고, 기존 패널을 실제로 소화한 최근 창이 있을
    때만 확장한다. 확장 상한도 ``dispatch_rate × interval``이라 제출 rate나
    in-flight를 올리지 않는다. 심야처럼 운행 후보가 적으면 pick_routes가 이 목표보다
    적은 실제 후보만 반환하므로 빈 노선을 만들어 쿼터를 태우지 않는다.
    """
    if not calls:
        return max(1, int(base_panel))
    tomorrow = (t + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0)
    seconds_left = max(1.0, (tomorrow - t).total_seconds())
    required_rates = [
        max(0, planned_per_key - used) / seconds_left
        for used in calls.values()
    ]
    desired = math.ceil(sum(required_rates) * interval)
    if isinstance(dispatch_rate, dict):
        capacity = sum(
            max(1, math.floor(dispatch_rate.get(kid, 0.1) * interval))
            for kid in calls)
    else:
        per_key_capacity = max(1, math.floor(dispatch_rate * interval))
        capacity = per_key_capacity * len(calls)
    nkeys = len(calls)
    capacity = max(nkeys, capacity - capacity % nkeys)
    # 심야에 rate가 운행 노선 수만큼 낮아져도 목표 패널 자체를 줄이지 않는다.
    # 그래야 아침 재선정에서 새로 운행을 시작한 노선을 다시 최대 base_panel까지
    # 발견할 수 있다. 실제 심야 선택 수는 pick_routes의 운행시간 필터가 줄인다.
    capacity = max(int(base_panel), capacity)
    wanted = max(int(base_panel), min(desired, capacity))
    if wanted > base_panel and not can_expand:
        return int(base_panel)
    # 키별 라우팅이 균등하므로 키 수의 배수로 올려 한 키만 한 슬롯 더 받지 않게 한다.
    return min(capacity, int(math.ceil(wanted / nkeys) * nkeys))


def quota_rate_targets(t, calls, planned_per_key, max_rates, routes_by_key,
                       interval, base_panel):
    """남은 쿼터를 주간에 보충하되 작은 심야 운행 풀은 과다 폴링하지 않는다."""
    tomorrow = (t + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0)
    seconds_left = max(1.0, (tomorrow - t).total_seconds())
    sparse_pool = sum(routes_by_key.values()) < math.ceil(base_panel * 0.9)
    targets = {}
    for kid, used in calls.items():
        required = max(0.0, planned_per_key - used) / seconds_left
        target = min(max_rates[kid], max(0.1, required))
        if sparse_pool:
            route_demand = routes_by_key.get(kid, 0) / max(1.0, interval)
            target = min(target, max(0.1, route_demand))
        targets[kid] = target
    return targets


def percentile(values, fraction):
    """외부 의존성 없는 nearest-rank 백분위수."""
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1,
                       math.ceil(len(ordered) * fraction) - 1))
    return float(ordered[index])


def pick_routes_readonly(n, target, nbands, t):
    """노선 전수 집계를 수집 DB writer와 다른 짧은 수명 읽기 연결에서 수행한다."""
    conn = O.connect_readonly()
    try:
        return O.pick_routes(conn, n, target, nbands, t=t)
    finally:
        conn.close()


def panel_cache_path():
    return os.path.join(O.DATA, ".bus-panel.json")


def load_cached_panel(limit):
    """재시작 직후 쓸 직전 패널. 새 비동기 선정이 끝나면 즉시 교체된다."""
    try:
        with open(panel_cache_path(), encoding="utf-8") as f:
            routes = json.load(f).get("routes", [])
    except (OSError, ValueError, AttributeError):
        return []
    valid = [
        route for route in routes
        if isinstance(route, dict)
        and route.get("routeid") and route.get("cityCode") is not None
    ]
    return valid[:max(0, int(limit))]


def save_cached_panel(routes):
    """선정 결과를 원자 교체해 중간 종료에도 이전 캐시를 보존한다."""
    path = panel_cache_path()
    tmp = f"{path}.tmp-{os.getpid()}"
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(
                {"savedAt": time.time(), "routes": routes},
                f, ensure_ascii=False, separators=(",", ":"))
        os.replace(tmp, path)
    except OSError as exc:
        try:
            os.remove(tmp)
        except OSError:
            pass
        print(f"[패널 캐시] ⚠️ 저장 실패: {exc}", flush=True)


# ── 일 호출수는 디스크에 (KeepAlive 재시작해도 상한을 넘지 않게) ────────
# ⚠️ 키는 운행일(04시 경계)이 아니라 **달력일**이다 — data.go.kr 쿼터가 자정에 리셋된다.
#    운행일로 세면 04시에 카운터만 0이 되는데 API 는 00-04시 콜(~3.7만)을 이미 새 날로
#    세고 있어, 470,000 + 37,000 = 507,000 으로 실제 50만 상한을 넘길 수 있다.
#    (운행일 경계는 데이터 파일·요일 분류에만 쓴다 — 그쪽은 04시가 맞다.)
def quota_day(t):
    return t.strftime("%Y-%m-%d")


def calls_path(day, keyid=None):
    suffix = f"-{keyid}" if keyid else ""
    return os.path.join(O.DATA, f".buscalls{suffix}-{day}")


def read_calls(day, keyid=None):
    try:
        with open(calls_path(day, keyid)) as f:
            return int(f.read().strip() or 0)
    except (OSError, ValueError):
        return 0


def _write_calls(day, n, keyid=None):
    tmp = calls_path(day, keyid) + ".tmp"
    with open(tmp, "w") as f:
        f.write(str(n))
    os.replace(tmp, calls_path(day, keyid))


def add_calls(day, n, keyid=None):
    """호출 전 예약. keyid가 있으면 키별 장부와 호환용 합계 장부를 함께 올린다."""
    with _CALL_LOCK:
        v = read_calls(day, keyid) + n
        _write_calls(day, v, keyid)
        if keyid:
            _write_calls(day, read_calls(day) + n)
    return v


def reserve_calls(day, wanted, cap, keyid):
    """cap 안에서 wanted만큼 원자적으로 선예약하고 실제 확보량을 반환한다."""
    with _CALL_LOCK:
        current = read_calls(day, keyid)
        grant = min(max(0, int(wanted)), max(0, cap - current))
        if not grant:
            return 0
        _write_calls(day, current + grant, keyid)
        if keyid:
            _write_calls(day, read_calls(day) + grant)
        return grant


def ensure_key_counters(keys, day):
    """구버전 합계 장부의 아직 미배분된 호출을 키별 장부로 마이그레이션.

    배포 직후 fetch_routes가 KEY1 장부를 먼저 만들었어도 합계-키별합 잔여를
    놓치지 않는다.
    """
    if not keys:
        return
    # reserve의 블록 갱신과 동시에 합계/키별 장부를 읽고 쓰면 migration이 갱신을
    # 덮을 수 있다. 같은 프로세스의 모든 장부 변경을 한 락으로 직렬화한다.
    with _CALL_LOCK:
        total = read_calls(day)
        assigned = sum(read_calls(day, kid) for kid, _ in keys)
        missing = max(0, total - assigned)
        if not missing:
            return
        q, r = divmod(missing, len(keys))
        for i, (kid, _) in enumerate(keys):
            _write_calls(day, read_calls(day, kid) + q + (i < r), kid)


class QuotaReservations:
    """호출 장부를 블록 단위로 선예약하는 스케줄러 전용 캐시."""

    def __init__(self, cap, block=RESERVE_BLOCK, clock=now,
                 allocate=reserve_calls):
        self.cap = cap
        self.block = block
        self.clock = clock
        self.allocate = allocate
        self.day = None
        self.left = {}

    def reserve(self, kid, n):
        qday = quota_day(self.clock())
        if qday != self.day:
            # 전날에 남은 선예약은 전날 장부에 이미 보수적으로 계상돼 있다.
            # 새 달력일에는 절대 이 잔여를 재사용하지 않는다.
            self.day = qday
            self.left.clear()

        left = self.left.get(kid, 0)
        if left >= n:
            self.left[kid] = left - n
            return True

        needed = n - left
        # room 확인과 장부 증가는 같은 _CALL_LOCK 임계구역에서 수행한다.
        # migration이 사이에 끼어 stale room 기준으로 cap을 넘는 틈을 없앤다.
        grant = self.allocate(qday, max(self.block, needed), self.cap, kid)
        if grant < needed:
            self.left[kid] = left + grant
            return False
        self.left[kid] = left + grant - n
        return True


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
        if _HTTP_POOL is not None:
            status, raw = _HTTP_POOL.get(key, f"{BASE_PATH}?{q}")
            if status >= 400:
                return routeid, [], f"HTTP{status}", now()
            d = json.loads(raw.decode())
        else:
            with urllib.request.urlopen(f"{BASE}?{q}", timeout=15) as r:
                d = json.loads(r.read().decode())
        obs = now()   # ★ 응답 수신 **직후** = 이 노선의 실제 관측시각.
        # ⚠️ 사이클 끝 시각 하나를 전 노선에 다 찍으면 첫 노선은 최대 한 사이클만큼
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
    global _HTTP_POOL
    keys = load_keys()
    if not keys:
        sys.exit("GBIS_BUS_KEY 없음")

    k = O.cfg()
    key_cap = k.get("busKeyDailyCap", 480000)
    planned_calls = min(
        key_cap, k.get("busPlannedCallsPerKey", key_cap - 5000))
    conn = O.connect()
    bands, target, nb = k["timebands"], k["targetSamples"], len(k["timebands"])
    interval, maxr = k["intervalSec"], k["maxRoutes"]
    configured_keys = len(k.get("busKeys") or ["GBIS_BUS_KEY"])
    effective_maxr = max(1, maxr * len(keys) // max(1, configured_keys))
    workers, rate = k["maxWorkers"], k["dispatchRate"]
    quota_max_rate = max(
        float(rate), float(k.get("busQuotaMaxRatePerKey", rate)))
    inflight_max = k["maxInflight"]
    physical_global_max = max(1, inflight_max * len(keys))
    global_max = min(
        physical_global_max,
        max(1, int(k.get("busGlobalMaxInflight", 44))))
    global_min = min(
        global_max,
        max(1, int(k.get("busGlobalMinInflight", 28))))
    global_initial = min(
        global_max,
        max(global_min, int(k.get("busGlobalInitialInflight", 36))))
    global_recovery_windows = max(
        1, int(k.get("busGlobalCleanWindows", 5)))
    code99_cooldown = max(0, float(k.get("busCode99CooldownSec", 3)))
    retry_code99 = bool(k.get("busRetryCode99", False))
    hold = k.get("busZombieHoldSec", 10)
    window = k["serviceWindow"]
    # stale Keep-Alive의 내부 재전송도 물리 호출이므로 같은 키 장부에서 먼저 예약한다.
    quota = QuotaReservations(key_cap)
    key_ids = {key: kid for kid, key in keys}
    _HTTP_POOL = (
        KeyedHTTPSPool(
            BASE_HOST, inflight_max,
            timeout=float(k.get("busHttpTimeoutSec", 15)),
            retry_reserver=lambda key: quota.reserve(key_ids[key], 1))
        if k.get("busHttpKeepAlive", True) else None)

    day = service_day(now())
    last = {}  # (routeid, vehicleno) -> (nodeord, 관측시각, nodeid, route_version)
    route_versions = dict(conn.execute(
        "SELECT routeid,currentVersion FROM route WHERE currentVersion IS NOT NULL"))
    picked, written, report_no = load_cached_panel(effective_maxr), 0, 0
    panel_target = effective_maxr
    capacity_clean_windows = 0
    global_clean_windows = 0
    global_pressure_windows = []
    rate_caps = {kid: quota_max_rate for kid, _ in keys}
    rate_clean_windows = {kid: 0 for kid, _ in keys}
    rate_targets = {kid: float(rate) for kid, _ in keys}
    picked_band = None
    next_repick = 0.0
    picker = ThreadPoolExecutor(max_workers=1, thread_name_prefix="route-picker")
    pick_future = None
    pick_request = None
    pick_retry_at = 0.0
    picker_generation = 0
    pool_stats_prev = (
        _HTTP_POOL.stats() if _HTTP_POOL is not None
        else {"created": 0, "reused": 0, "closed": 0,
              "stale_retries": 0})
    rotated_day = None

    # 선예약 블록 — 스케줄러 스레드 전용(단일 스레드라 자체 락 불필요). 요청 1건마다
    # 디스크 장부를 교체하면 하루 ~200만 회의 tmp-write+os.replace가 나고 그동안
    # 스케줄러 락을 잡는다. 대신 키별로 RESERVE_BLOCK 콜을 미리 디스크에 예약하고
    # 실제 요청은 메모리 잔여만 차감한다 → 파일 쓰기 ~1/RESERVE_BLOCK. 크래시 시
    # 키당 최대 RESERVE_BLOCK-1 콜을 과다계상하지만(48만 중 63) 절대 과소계상하지
    # 않아 실제 쿼터를 넘지 않는다. 자정에는 전날 메모리 잔여를 버리고 새 장부를 쓴다.
    dispatcher = RollingDispatcher(
        keys, fetch, quota.reserve, interval, rate, workers, inflight_max, hold,
        global_inflight=global_initial, code99_cooldown=code99_cooldown,
        retry_code99=retry_code99)
    if picked:
        dispatcher.set_routes(picked)
        print(
            f"[{now():%H:%M:%S}] 직전 패널 {len(picked)}개 즉시 복원 "
            f"(새 선정은 백그라운드 계산)",
            flush=True)

    def blank_stats():
        return {
            "attempted": 0, "successful": 0, "retried": 0, "recovered": 0,
            "residual": 0,
            "moving": 0, "rows": 0, "errors": [], "final": {},
            "durations": [], "collector_delays": [],
            "by_key": {
                kid: {
                    "attempted": 0, "successful": 0, "retried": 0,
                    "residual": 0, "errors": [],
                }
                for kid, _ in keys
            },
        }

    stats = blank_stats()
    stats_qday = quota_day(now())
    report_started = time.monotonic()
    report_at = report_started + interval
    report_waiting = False
    counter_day = None
    next_counter_check = 0.0

    def report_window(report_day, report_band, elapsed, next_deadline):
        """완료 이벤트를 한 건강 창으로 확정한다. DB 쓰기는 메인 스레드만 한다."""
        nonlocal stats, report_no, report_started, report_at, next_repick
        nonlocal capacity_clean_windows, global_clean_windows
        nonlocal global_pressure_windows
        nonlocal picker_generation
        report_obs = now()
        elapsed = max(0.001, elapsed)
        report_no += 1
        snap = dispatcher.snapshot()
        all_errors = stats["errors"]
        calls_now = {
            kid: read_calls(report_day, kid)
            for kid, _ in keys
        }
        for kid, _ in keys:
            key_errors = stats["by_key"][kid]["errors"]
            if any("HTTP429" in err for err in key_errors):
                rate_caps[kid] = max(
                    0.5, min(rate_caps[kid], snap["rates"][kid] * 0.7))
                rate_clean_windows[kid] = 0
            else:
                rate_clean_windows[kid] += 1
                if (rate_clean_windows[kid] >= 5
                        and rate_caps[kid] < quota_max_rate):
                    rate_caps[kid] = min(
                        quota_max_rate, rate_caps[kid] + 0.5)
                    rate_clean_windows[kid] = 0
        new_rates = quota_rate_targets(
            report_obs, calls_now, planned_calls, rate_caps,
            snap["routesByKey"], interval, effective_maxr)
        rate_changes = [
            f"{kid}={snap['rates'][kid]:.2f}→{new_rates[kid]:.2f}"
            for kid, _ in keys
            if abs(snap["rates"][kid] - new_rates[kid]) >= 0.05
        ]
        if rate_changes:
            dispatcher.set_rates(new_rates)
            print(
                f"[{report_obs:%H:%M:%S}] 쿼터 rate "
                + " ".join(rate_changes),
                flush=True)
        rate_targets.clear()
        rate_targets.update(new_rates)
        snap = dispatcher.snapshot()
        durations = stats["durations"]
        latency_avg = (
            sum(durations) / len(durations) if durations else 0.0)
        latency_p50 = percentile(durations, 0.50)
        latency_p90 = percentile(durations, 0.90)
        collector_delays = stats["collector_delays"]
        collector_delay_avg = (
            sum(collector_delays) / len(collector_delays)
            if collector_delays else 0.0)
        collector_delay_p50 = percentile(collector_delays, 0.50)
        collector_delay_p90 = percentile(collector_delays, 0.90)
        pool_now = (
            _HTTP_POOL.stats() if _HTTP_POOL is not None
            else pool_stats_prev)
        conn_created = max(
            0, pool_now.get("created", 0) - pool_stats_prev.get("created", 0))
        conn_reused = max(
            0, pool_now.get("reused", 0) - pool_stats_prev.get("reused", 0))
        conn_closed = max(
            0, pool_now.get("closed", 0) - pool_stats_prev.get("closed", 0))
        stale_retries = max(
            0, pool_now.get("stale_retries", 0)
            - pool_stats_prev.get("stale_retries", 0))
        pool_stats_prev.clear()
        pool_stats_prev.update(pool_now)
        health_ts = time.time()
        O.record_health(
            conn, report_day, report_band, stats["attempted"], stats["successful"],
            stats["retried"], stats["residual"],
            sum("code99" in e for e in all_errors),
            sum("Timeout" in e or "timed out" in e for e in all_errors),
            sum("HTTP429" in e for e in all_errors),
            sum(e.startswith("HTTP5") for e in all_errors),
            elapsed, snap["globalLimit"],
            latency_avg, latency_p50, latency_p90,
            1, collector_delay_avg, collector_delay_p50, collector_delay_p90,
            conn_created, conn_reused, conn_closed, ts=health_ts)
        for kid, _ in keys:
            key_stats = stats["by_key"][kid]
            key_errors = key_stats["errors"]
            O.record_key_health(
                conn, health_ts, report_day, report_band, kid,
                key_stats["attempted"], key_stats["successful"],
                key_stats["retried"], key_stats["residual"],
                sum("code99" in e for e in key_errors),
                sum("Timeout" in e or "timed out" in e for e in key_errors),
                sum("HTTP429" in e for e in key_errors),
                sum(e.startswith("HTTP5") for e in key_errors),
                elapsed, snap["limits"][kid], snap["routesByKey"][kid],
                panel_target)
        conn.commit()

        # 패널을 늘려도 호출량이 늘 조건인지 먼저 확인한다. 현재 활성 패널의 90%를
        # 3개 연속 창에서 완료한 뒤에만 쿼터 기반 확장을 허용한다. 대기열이 밀린
        # 상태에서는 노선만 더 넣어 실효 주기를 악화시키지 않는다. 심야의 작은
        # 운행 후보 풀을 100% 소화한 것은 주간 기본 패널의 처리능력 증거가 아니므로
        # 기본 패널의 90% 이상이 실제 활성인 창만 센다.
        cadence_ok = (
            snap["routes"] >= math.ceil(effective_maxr * 0.9)
            and stats["attempted"] >= math.ceil(snap["routes"] * 0.9)
            and not any("code99" in err for err in all_errors))
        capacity_clean_windows = (
            capacity_clean_windows + 1 if cadence_ok else 0)
        if not cadence_ok and panel_target > effective_maxr:
            # 확장 뒤 처리 지연이나 code99가 보이면 정기 재선정(~21분)을 기다리지
            # 않고 다음 메인 반복에서 기본 패널로 즉시 복귀한다.
            next_repick = 0.0
            # 이미 확장 패널을 계산 중이면 그 결과도 폐기하고 축소 기준으로 다시 고른다.
            picker_generation += 1

        mem = O.rss_mb()
        with LOCK:
            STATE["cycles"] = report_no
            if stats["attempted"]:
                STATE["lastObs"] = time.time()
            STATE["lastCycleSec"] = elapsed
            STATE["picked"] = len(picked)
            STATE["panelTarget"] = panel_target
            STATE["moving"] = stats["moving"]
            STATE["written"] = written
            STATE["errors"] = dict(stats["final"])
            STATE["retried"] = stats["retried"]
            STATE["globalInflightLimit"] = snap["globalLimit"]
            STATE["dispatchRates"] = dict(snap["rates"])
            STATE["requestLatencyAvg"] = latency_avg
            STATE["requestLatencyP50"] = latency_p50
            STATE["requestLatencyP90"] = latency_p90
            STATE["collectorDelayAvg"] = collector_delay_avg
            STATE["collectorDelayP50"] = collector_delay_p50
            STATE["collectorDelayP90"] = collector_delay_p90
            STATE["connectionReusePct"] = (
                conn_reused / (conn_created + conn_reused)
                if conn_created + conn_reused else 0)
            STATE["staleConnectionRetries"] = stale_retries
            STATE["rssMB"] = mem
            STATE["inflightLimits"] = dict(snap["limits"])
            STATE["night"] = False
            STATE["fetching"] = snap["inflight"] > 0
            if stats["final"]:
                detail = " ".join(
                    f"{err}×{n}" for err, n in
                    sorted(stats["final"].items(), key=lambda x: -x[1]))
                log = STATE.setdefault("errLog", [])
                log.append({"t": time.time(), "n": stats["residual"],
                            "picked": stats["attempted"], "detail": detail})
                del log[:-50]

        recovered = stats["recovered"]
        print(f"[{report_obs:%H:%M:%S}] {elapsed:.0f}초 창 응답 "
              f"{stats['successful']}/{stats['attempted']} · "
              f"운행 {stats['moving']}대 · 통과 +{stats['rows']} "
              f"(누적 {written:,}) · 예약 {snap['queued']} · "
              f"in-flight 합계 {snap['inflight']}/{snap['globalLimit']} · "
              f"rate합계 {sum(snap['rates'].values()):.1f}/s · "
              f"키상한 " + " ".join(
                  f"{kid}={snap['limits'][kid]}" for kid, _ in keys)
              + (f" · 쿼터차단 {snap['quotaBlocked']}키"
                 if snap["quotaBlocked"] else "")
              + (f" · {mem:.0f}MB" if mem else "")
              + (f" · 재시도 {stats['retried']}→회복 {recovered}"
                 if stats["retried"] else "")
              + (f" · 지연 {latency_p50:.2f}/{latency_p90:.2f}s"
                 if durations else "")
              + (f" · 수확지연 "
                 f"{collector_delay_p50:.2f}/{collector_delay_p90:.2f}s"
                 if collector_delays else "")
              + (f" · 연결재사용 "
                 f"{conn_reused/(conn_created+conn_reused)*100:.0f}%"
                 if conn_created + conn_reused else "")
              + (f" · stale재연결 {stale_retries}"
                 if stale_retries else ""), flush=True)
        if stats["final"]:
            detail = " ".join(
                f"{err}×{n}" for err, n in
                sorted(stats["final"].items(), key=lambda x: -x[1]))
            print(f"[{report_obs:%H:%M:%S}] ⚠️ 실패 "
                  f"{stats['residual']}/{stats['attempted']} — {detail}",
                  flush=True)
        if report_no % 20 == 0:
            detail = " ".join(
                f"{kid}={read_calls(report_day,kid):,}/{key_cap:,}"
                for kid, _ in keys)
            print(f"[{report_obs:%H:%M:%S}] 콜 {detail}", flush=True)

        # code99는 키 자체가 아니라 EC2 egress가 공유하는 세션 압력으로 실측됐다.
        # 어느 키에서든 보이면 전역 상한을 후퇴시키고, 충분한 정상 창 뒤에만 +1한다.
        code99_count = sum("code99" in err for err in all_errors)
        old_global = snap["globalLimit"]
        new_global, global_clean_windows, global_pressure_windows = (
            next_global_inflight(
            old_global, global_min, global_max, code99_count, stats["attempted"],
            old_global, global_clean_windows,
            global_recovery_windows, global_pressure_windows,
            recovery_step=(
                4 if any(v > float(rate) + 0.05
                         for v in rate_targets.values()) else 2)))
        if new_global != old_global:
            dispatcher.set_global_inflight_limit(new_global)
            print(
                f"[{report_obs:%H:%M:%S}] 전역 in-flight "
                f"{old_global}→{new_global}"
                f"({'code99 과부하 후퇴' if new_global < old_global else '정상창 회복'})",
                flush=True)

        stats = blank_stats()
        report_started = time.monotonic()
        report_at = next_deadline

    print(f"[{now():%H:%M:%S}] 연속 수집 시작 · 노선별 {interval}초 · 목표 {target}샘플 · "
          f"밴드 {nb}개 · 최대 {effective_maxr}노선 · "
          f"키 {len(keys)}개({', '.join(nm for nm, _ in keys)}) · "
          f"전역 in-flight {global_initial}({global_min}~{global_max}) · "
          f"키당 rate {rate}~{quota_max_rate:g}/s · "
          f"키당 계획 {planned_calls:,} / 상한 {key_cap:,} "
          f"(오늘 합계 {read_calls(quota_day(now())):,} 사용)", flush=True)

    try:
        while True:
            t = now()
            d, qday = service_day(t), quota_day(t)
            dispatcher.raise_if_failed()

            if qday != stats_qday:
                # 쿼터는 00:00 달력일 기준이다. 전날 건강 통계를 먼저 닫아 새 날과
                # 섞지 않고, cap으로 멈춘 키는 60초 타이머를 기다리지 않고 즉시 푼다.
                mono = time.monotonic()
                report_window(
                    stats_qday, picked_band if picked_band is not None
                    else O.band_of(t, bands),
                    mono - report_started, mono + interval)
                blocked = set(dispatcher.reset_quota_blocks_by_key())
                print(f"[{t:%H:%M:%S}] 쿼터 날짜 전환 {stats_qday} → {qday} · "
                      f"차단 해제 {len(blocked)}키", flush=True)
                stats_qday = qday

            mono = time.monotonic()
            if counter_check_due(qday, counter_day, mono, next_counter_check):
                ensure_key_counters(keys, qday)
                counter_day = qday
                next_counter_check = mono + COUNTER_CHECK_SEC
            if d != day:
                # 04시는 데이터 운행일만 바뀐다. data.go.kr 쿼터와 AIMD는 자정
                # 기준이므로 여기서 선예약·in-flight를 초기화하지 않는다.
                print(f"[{t:%H:%M:%S}] 운행일 전환 {day} → {d}", flush=True)
                day, last, written = d, {}, 0
                next_repick = 0
                picker_generation += 1
                conn.execute("UPDATE route SET emptyStreak = 0")
                conn.commit()
                with LOCK:
                    STATE["errLog"] = []

            due = O.rotate_due(rotated_day, t)
            if due:
                rotated_day = due
                __import__("threading").Thread(
                    target=O.rotate_jsonl, args=("bus",), daemon=True).start()

            if not O.in_window(t, window):
                if picked:
                    picked = []
                    dispatcher.set_routes([])
                    picker_generation += 1
                report_waiting = True
                with LOCK:
                    STATE["night"] = True
                time.sleep(1)
                continue

            if report_waiting:
                # 대기 시간을 첫 건강 창 duration에 포함하지 않는다. 대기 직전의
                # 부분 통계도 새 정상 창과 섞지 않는다.
                stats = blank_stats()
                report_started = time.monotonic()
                report_at = report_started + interval
                report_waiting = False

            cur_band = O.band_of(t, bands)
            mono = time.monotonic()
            if pick_future is not None and pick_future.done():
                req = pick_request
                try:
                    selected = pick_future.result()
                except Exception as exc:
                    print(
                        f"[{t:%H:%M:%S}] ⚠️ 노선 재선정 실패: "
                        f"{type(exc).__name__}: {exc} · 5초 뒤 재시도",
                        flush=True)
                    pick_retry_at = mono + 5
                else:
                    stale = (
                        req["generation"] != picker_generation
                        or req["day"] != d
                        or req["band"] != cur_band)
                    if stale:
                        print(
                            f"[{t:%H:%M:%S}] 노선 재선정 결과 폐기 "
                            f"(계산 중 운행일/밴드/패널 변경)",
                            flush=True)
                    else:
                        picked = selected
                        save_cached_panel(picked)
                        panel_target = req["panel"]
                        picked_band = cur_band
                        next_repick = mono + REPICK_EVERY * interval
                        dispatcher.set_routes(picked)
                        if not picked:
                            report_waiting = True
                            pool = conn.execute(
                                "SELECT COUNT(*) FROM route").fetchone()[0]
                            msg = (
                                "❌ 노선 풀이 비어 있다 — 먼저 "
                                "`python3 fetch_routes.py`"
                                if pool == 0
                                else f"폴링할 노선 없음 (풀 {pool:,})")
                            print(f"[{t:%H:%M:%S}] {msg}", flush=True)
                            pick_retry_at = mono + 10
                        else:
                            print(
                                f"[{t:%H:%M:%S}] 노선 재선정: "
                                f"{len(picked)}개 "
                                f"(충전율 {picked[0]['fill']*100:.1f}% ~ "
                                f"{picked[-1]['fill']*100:.1f}%) · "
                                f"{interval}초에 균등 배치",
                                flush=True)
                pick_future = None
                pick_request = None

            band_changed = bool(picked) and cur_band != picked_band
            # 기존 REPICK_EVERY(40창≈21분)를 시간 기준으로 보존한다.
            # 집계가 오래 걸려도 기존 패널·HTTP 스케줄러·DB writer는 계속 돈다.
            if ((not picked or band_changed or mono >= next_repick)
                    and pick_future is None and mono >= pick_retry_at):
                calls = {
                    kid: read_calls(qday, kid)
                    for kid, _ in keys
                }
                new_panel_target = quota_panel_target(
                    t, calls, effective_maxr, interval, rate_targets,
                    planned_calls,
                    capacity_clean_windows >= 3)
                if new_panel_target != panel_target:
                    print(
                        f"[{t:%H:%M:%S}] 쿼터 패널 {panel_target}→"
                        f"{new_panel_target} · 최근 소화 연속창 "
                        f"{capacity_clean_windows} · "
                        + " ".join(
                            f"{kid}={calls[kid]:,}" for kid, _ in keys),
                        flush=True)
                pick_request = {
                    "generation": picker_generation,
                    "day": d,
                    "band": cur_band,
                    "panel": new_panel_target,
                }
                pick_future = picker.submit(
                    pick_routes_readonly, new_panel_target, target, nb, t)
                print(
                    f"[{t:%H:%M:%S}] 노선 재선정 계산 시작 "
                    f"(기존 {len(picked)}개 패널 계속 수집)",
                    flush=True)

            first = dispatcher.get(timeout=0.5)
            events = dispatcher.drain(first)
            if events:
                # 연속 도착을 건별 커밋하면 작은 EC2에서 WAL fsync가 초당 여러 번
                # 발생한다. 네트워크 스케줄과 obs 시각은 그대로 두고 최대 0.5초치만
                # 모아 SQLite·JSONL을 한 번에 반영한다.
                batch_until = min(report_at, time.monotonic() + 0.5)
                while len(events) < 256:
                    wait = batch_until - time.monotonic()
                    if wait <= 0:
                        break
                    event = dispatcher.get(timeout=wait)
                    if event is None:
                        break
                    events.extend(dispatcher.drain(event, 256 - len(events)))
            if events:
                rows, bumps = [], []
                for event in events:
                    routeid, items, err, obs = event["result"]
                    key_stats = stats["by_key"][event["keyid"]]
                    key_stats["attempted"] += 1
                    key_stats["retried"] += int(event["retried"])
                    key_stats["errors"] += event["errors"]
                    stats["attempted"] += 1
                    stats["retried"] += int(event["retried"])
                    stats["errors"] += event["errors"]
                    if event.get("wire_duration") is not None:
                        wire_duration = float(event["wire_duration"])
                        stats["durations"].append(wire_duration)
                        stats["collector_delays"].append(max(
                            0.0, float(event["duration"]) - wire_duration))
                    if err:
                        key_stats["residual"] += 1
                        stats["residual"] += 1
                        stats["final"][err] = stats["final"].get(err, 0) + 1
                        continue
                    key_stats["successful"] += 1
                    stats["successful"] += 1
                    if event["retried"]:
                        stats["recovered"] += 1
                    band = O.band_of(obs, bands)
                    dtype = O.day_type(obs)
                    hol = O.is_holiday(obs)
                    O.mark_empty(conn, routeid, not items)
                    stats["moving"] += len(items)
                    if band is not None and not hol:
                        O.mark_service(
                            conn, routeid, band, dtype, service_day(obs), bool(items))
                    for bus in items:
                        v, ordv = bus.get("vehicleno"), bus.get("nodeord")
                        try:
                            ordv = int(ordv)
                        except (TypeError, ValueError):
                            continue
                        if not v:
                            continue
                        vk = (routeid, v)
                        prev = last.get(vk)
                        nodeid = bus.get("nodeid")
                        version = route_versions.get(routeid)
                        last[vk] = (ordv, obs, nodeid, version)
                        if prev is None or ordv <= prev[0]:
                            continue
                        if (obs - prev[1]).total_seconds() > interval * 4:
                            continue
                        row = {
                            "t": obs.isoformat(), "t_prev": prev[1].isoformat(),
                            "routeid": routeid, "vehicleno": v,
                            "from_ord": prev[0], "to_ord": ordv,
                            "from_nodeid": prev[2], "to_nodeid": nodeid,
                            "nodeid": nodeid, "route_version": version or prev[3],
                            "band": band, "daytype": dtype,
                        }
                        rows.append(row)
                        if band is not None and not hol:
                            sday = service_day(obs)
                            quality = O.transition_quality(
                                prev[1], obs, prev[0], ordv, bands)
                            if quality == "exact":
                                bumps.append(
                                    (routeid, prev[0], ordv, band, dtype, sday))
                            else:
                                if quality == "skipped":
                                    quality = ("censored" if ordv-prev[0] <= 3
                                               else "implausible")
                                O.bump_span(
                                    conn, routeid, prev[0], ordv, band, dtype,
                                    sday, quality, (obs-prev[1]).total_seconds())

                if rows:
                    by_day = {}
                    for row in rows:
                        rt = datetime.fromisoformat(row["t"])
                        by_day.setdefault(service_day(rt), []).append(row)
                    for row_day, day_rows in by_day.items():
                        path = os.path.join(O.DATA, f"bus-{row_day}.jsonl")
                        with open(path, "a", encoding="utf-8") as f:
                            for row in day_rows:
                                f.write(json.dumps(row, ensure_ascii=False) + "\n")
                    written += len(rows)
                    stats["rows"] += len(rows)
                for bump_args in bumps:
                    O.bump(conn, *bump_args)
                    O.mark_included_direct(conn, *bump_args[:5])
                conn.commit()  # 네트워크 worker는 DB에 접근하지 않는다

            if time.monotonic() < report_at:
                continue

            # 처리 시간이 길어도 밀린 보고를 연속 출력하지 않는다.
            mono = time.monotonic()
            report_window(
                stats_qday, cur_band, mono - report_started,
                max(report_at + interval, mono + 0.1))
    finally:
        dispatcher.close()
        picker.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    main()
