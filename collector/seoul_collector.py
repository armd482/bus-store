#!/usr/bin/env python3
"""서울 버스 도착정보를 노선별 독립 주기로 연속 수집한다.

서울 도착정보는 운영사가 계산한 다음 2대의 도착 예정값(arrmsg1/2, nstnSec1/2)을
노선 단위로 제공한다. 약 702개 노선을 3개 키에 균등 분산하고 각 노선을 약 36분마다
조회하면 하루 키당 약 9,295콜이다. 정상 요청 예산은 키당 9,300콜, 정지 상한은
9,900콜로 분리해 약 600콜을 재시도·재시작 여유로 남긴다.

config.timebands 7개는 수집 횟수를 정하는 스케줄이 아니라 분석용 라벨이다. 각 노선은
자기 next_due를 가지며 느린 요청이나 재시도는 다른 노선의 주기를 막지 않는다.

  python3 seoul_collector.py            # 연속 수집
  python3 seoul_collector.py --routes   # 노선 목록만 갱신하고 종료
"""
import glob
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

import orchestrator as O
import env_config as E
from rolling_dispatcher import RollingDispatcher

BASE = "http://ws.bus.go.kr/api/rest"  # 443이 열리지 않아 HTTP를 사용한다.
PREFIX = "seoul"
OUT_DIR = O.DATA
DAILY_CAP = 9900
PLANNED_CALLS = 9300
ROUTES_FILE = os.path.join(O.DATA, "seoul_routes.json")
STATUS_FILE = os.path.join(O.DATA, "seoul-status.json")
_CALL_LOCK = threading.Lock()

# routeType 7(인천)·8(경기)은 경기 TAGO 수집과 중복되므로 제외한다.
SEOUL_TYPES = {"1", "2", "3", "4", "5", "6"}
TYPE_NM = {"1": "공항", "2": "마을", "3": "간선", "4": "지선", "5": "순환", "6": "광역"}


def now():
    return datetime.now(O.KST)


def service_day(t):
    return (t - timedelta(hours=4)).strftime("%Y-%m-%d")


def quota_day(t):
    """data.go.kr 쿼터는 운행일이 아닌 달력일 자정에 바뀐다."""
    return t.strftime("%Y-%m-%d")


def _load_key(envname):
    """첫 서울 키만 기존 공통 키로 호환하고, 추가 키에는 fallback을 쓰지 않는다."""
    if envname == "SEOUL_BUS_KEY":
        return E.get("SEOUL_BUS_KEY") or E.get("GBIS_BUS_KEY", "DATA_GO_KR_KEY")
    return E.get(envname)


def load_key():
    keys = load_keys()
    return keys[0][1] if keys else None


def load_keys(warn=True):
    """설정된 실제 키를 값 기준으로 중복 제거한다."""
    names = O.cfg().get("seoulBusKeys") or ["SEOUL_BUS_KEY"]
    out, seen = [], {}
    for name in names:
        key = _load_key(name)
        if not key:
            continue
        if key in seen:
            if warn:
                print(f"[서울 키 설정] ⚠️ {name}은 {seen[key]}과 같은 실제 키 — 중복 제외",
                      flush=True)
            continue
        seen[key] = name
        out.append((name, key))
    return out


def calls_path(day, keyid=None):
    suffix = f"-{keyid}" if keyid else ""
    return os.path.join(O.DATA, f"seoul-calls{suffix}-{day}.txt")


def read_calls(day, keyid=None):
    try:
        with open(calls_path(day, keyid)) as f:
            return int(f.read().strip() or 0)
    except (OSError, ValueError):
        return 0


def _write_calls(day, value, keyid=None):
    path = calls_path(day, keyid)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        f.write(str(value))
    os.replace(tmp, path)


def add_calls(day, n, keyid=None):
    """호출 전에 키별 장부와 호환용 합계 장부를 원자적으로 올린다."""
    with _CALL_LOCK:
        value = read_calls(day, keyid) + n
        _write_calls(day, value, keyid)
        if keyid:
            _write_calls(day, read_calls(day) + n)
    return value


def ensure_key_counters(keys, day):
    """구버전 단일 장부의 호출량을 첫 활성 키 장부로 보수적으로 이관한다."""
    if not keys:
        return
    with _CALL_LOCK:
        total = read_calls(day)
        assigned = sum(read_calls(day, kid) for kid, _ in keys)
        missing = max(0, total - assigned)
        if missing:
            first = keys[0][0]
            _write_calls(day, read_calls(day, first) + missing, first)


def sync_quota_db(day, keys=None):
    """파일 카운터 누계를 DB 이력(quota_daily)에 반영. 실패해도 무시된다.

    서울은 cleanup_state_files 가 당일 파일만 남기므로 과거 추이는 여기서만
    남는다 (세 수집기 공통 정책 — 파일=당일 쿼터 방어 · DB=과거 이력).
    """
    ids = [k for k, _ in (keys or [])] or [None]
    for kid in ids:
        O.record_quota(day, "seoul", kid or "SEOUL", read_calls(day, kid), DAILY_CAP)


class QuotaReservations:
    """키별 정지 상한을 요청 제출 직전에 예약한다."""

    def __init__(self, cap, clock=now):
        self.cap = int(cap)
        self.clock = clock

    def reserve(self, kid, n):
        day = quota_day(self.clock())
        with _CALL_LOCK:
            current = read_calls(day, kid)
            if current + n > self.cap:
                return False
            _write_calls(day, current + n, kid)
            _write_calls(day, read_calls(day) + n)
        return True


def minimum_safe_interval(route_count, key_count, cap, utilization=0.95):
    """활성 키가 빠져도 키별 cap의 utilization 이하를 쓰는 최소 평균 주기."""
    if route_count <= 0 or key_count <= 0 or cap <= 0:
        return 0
    utilization = min(1.0, max(0.01, float(utilization)))
    return route_count * 86400.0 / (key_count * cap * utilization)


def service_window(value, tail_min=0):
    """노선의 (첫차 분, 막차+꼬리 분). 자정을 넘기면 종료가 1440 을 넘는 값이 된다.

    ⚠️ first/last 는 `YYYYMMDDHHMMSS` 형식이다 (✅ 실측 '20260722043000' = 04:30).
    앞 4자리를 시각으로 읽으면 '20:26' 이 되어 전 노선이 심야로 분류된다.
    tail_min 은 막차 **출발** 이후 종착까지의 여유 — 그 구간이 마지막 배차 관측이다.
    """
    def hm(raw):
        digits = "".join(ch for ch in str(raw or "") if ch.isdigit())
        if len(digits) >= 12:
            digits = digits[8:12]          # YYYYMMDD|HHMM|SS
        elif len(digits) >= 4:
            digits = digits[:4]
        else:
            return None
        return (int(digits[:2]) % 24) * 60 + int(digits[2:4])

    start, last = hm(value.get("first")), hm(value.get("last"))
    if start is None or last is None:
        return None                        # 모르면 상시 운행으로 본다 (보수적)
    return start, last + (1440 if last < start else 0) + tail_min


def is_running(window, t):
    """지금 이 노선이 운행 중인가. window 가 None 이면 True (모르면 찍는다)."""
    if window is None:
        return True
    start, end = window
    if end - start >= 1440:
        return True                        # 창이 하루를 덮는다
    minute = t.hour * 60 + t.minute
    return start <= minute + (1440 if minute < start else 0) <= end


def service_route_hours(windows, total_routes):
    """전 노선 운행시간 합(시간) — 주기 산정의 분자.

    노선마다 운행시간이 달라(심야 00~03시엔 702 중 4~6개만 운행 — ✅ 실측)
    '노선 수 × 24시간'으로 주기를 잡으면 안 다니는 시간대에 빈 응답을 사느라
    예산을 태운다. 실제 운행시간 합으로 잡아야 같은 예산에 주기를 당길 수 있다.
    """
    known = 0.0
    for window in windows.values():
        known += 24.0 if window is None else min(1440, window[1] - window[0]) / 60.0
    missing = total_routes - len(windows)
    return known + missing * 24.0


def service_safe_interval(route_hours, key_count, planned_calls):
    """운행시간 합 기준 최소 주기 — 키당 planned_calls 를 넘지 않는 값."""
    if route_hours <= 0 or key_count <= 0 or planned_calls <= 0:
        return 0
    return route_hours * 3600.0 / (planned_calls * key_count)


def fetch(key, op, **params):
    q = urllib.parse.urlencode({"serviceKey": key, **params})
    with urllib.request.urlopen(f"{BASE}/{op}?{q}", timeout=20) as response:
        root = ET.fromstring(response.read().decode("utf-8", "replace"))
    # 서울 API도 업무 오류를 HTTP 200 XML로 반환한다.
    code = root.findtext(".//headerCd")
    if code is not None and str(code).strip() not in ("0", "00"):
        msg = root.findtext(".//headerMsg") or root.findtext(".//headerMsg1") or "unknown"
        raise RuntimeError(f"headerCd={code}:{msg[:80]}")
    return root


def refresh_routes(key):
    """서울 소속 노선 목록. 노선번호 검색어 0~9로 전체를 훑는다."""
    seen = {}
    for search in "0123456789":
        try:
            root = fetch(key, "busRouteInfo/getBusRouteList", strSrch=search)
        except Exception as exc:
            print(f"  노선 조회 실패({search}): {type(exc).__name__}", flush=True)
            continue
        for item in root.findall(".//itemList"):
            fields = {child.tag: (child.text or "") for child in item}
            if fields.get("routeType") in SEOUL_TYPES and fields.get("busRouteId"):
                seen[fields["busRouteId"]] = {
                    "no": fields.get("busRouteNm"), "tp": fields.get("routeType"),
                    "term": fields.get("term"), "first": fields.get("firstBusTm"),
                    "last": fields.get("lastBusTm"),
                }
        time.sleep(0.3)
    if seen:
        with open(ROUTES_FILE, "w", encoding="utf-8") as file:
            json.dump(seen, file, ensure_ascii=False)
    return seen


def routes(key, force=False):
    if not force and os.path.exists(ROUTES_FILE):
        try:
            with open(ROUTES_FILE, encoding="utf-8") as file:
                result = json.load(file)
            if result:
                return result
        except (OSError, ValueError):
            pass
    print(f"[{now():%H:%M:%S}] 서울 노선 목록 갱신 중…", flush=True)
    result = refresh_routes(key)
    print(f"[{now():%H:%M:%S}] 서울 소속 {len(result):,}개", flush=True)
    return result


def snapshot(key, rid):
    """노선 한 개의 전 정류장 도착정보를 슬림 형식으로 반환한다."""
    root = fetch(key, "arrive/getArrInfoByRouteAll", busRouteId=rid)
    out = []
    for item in root.findall(".//itemList"):
        fields = {child.tag: (child.text or "") for child in item}
        out.append({
            "ord": fields.get("staOrd"), "stId": fields.get("stId"),
            "stNm": fields.get("stNm"), "mkTm": fields.get("mkTm"),
            "arr1": fields.get("arrmsg1"), "arr2": fields.get("arrmsg2"),
            "veh1": fields.get("plainNo1"), "veh2": fields.get("plainNo2"),
            "sec1": fields.get("nstnSec1"), "sec2": fields.get("nstnSec2"),
            "spd1": fields.get("traSpd1"), "term": fields.get("term"),
        })
    if not out:
        raise RuntimeError("empty_route_snapshot")
    return out


def dispatcher_fetch(key, _city, rid):
    """RollingDispatcher가 쓰는 실패-값 반환 어댑터."""
    try:
        rows = snapshot(key, rid)
        return rid, rows, None, now()
    except urllib.error.HTTPError as exc:
        return rid, [], f"HTTP{exc.code}", now()
    except Exception as exc:
        return rid, [], f"{type(exc).__name__}:{str(exc)[:100]}", now()


# 구버전 상태 파일 접근자는 대시보드/도구의 짧은 호환 기간을 위해 읽기만 남긴다.
def done_path(day):
    return os.path.join(O.DATA, f"seoul-done-{day}.json")


def load_done(day):
    try:
        with open(done_path(day), encoding="utf-8") as file:
            return {(int(key.split(":", 1)[0]), key.split(":", 1)[1]): True
                    for key in json.load(file)}
    except (OSError, ValueError, IndexError):
        return {}


def failures_path(day):
    return os.path.join(O.DATA, f".seoul-failures-{day}.json")


def load_failures(day):
    try:
        with open(failures_path(day), encoding="utf-8") as file:
            return json.load(file)
    except (OSError, ValueError):
        return {}


def read_status():
    try:
        with open(STATUS_FILE, encoding="utf-8") as file:
            return json.load(file)
    except (OSError, ValueError):
        return {}


def write_status(status):
    tmp = STATUS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as file:
        json.dump(status, file, ensure_ascii=False)
    os.replace(tmp, STATUS_FILE)


def cleanup_state_files(t, keys=None):
    """현재 달력일의 합계·활성 키 장부만 남기고 구버전 done/failure를 제거한다."""
    qday = quota_day(t)
    keys = keys if keys is not None else load_keys(warn=False)
    keep = {os.path.abspath(calls_path(qday))}
    keep.update(os.path.abspath(calls_path(qday, kid)) for kid, _ in keys)
    patterns = (
        os.path.join(O.DATA, "seoul-calls-*.txt"),
        os.path.join(O.DATA, "seoul-done-*.json"),
        os.path.join(O.DATA, ".seoul-failures-*.json"),
    )
    removed = []
    for pattern in patterns:
        for path in glob.glob(pattern):
            if os.path.abspath(path) in keep:
                continue
            try:
                os.remove(path)
                removed.append(os.path.basename(path))
            except OSError as exc:
                print(f"[상태 정리] ⚠️ {os.path.basename(path)} 삭제 실패: {exc}",
                      flush=True)
    if removed:
        print(f"[상태 정리] 과거 서울 장부 {len(removed)}개 삭제: "
              + ", ".join(sorted(removed)), flush=True)
    return removed


def main():
    keys = load_keys()
    if not keys:
        sys.exit("키 없음 — config.seoulBusKeys에 대응하는 키를 .env에 넣어야 한다")
    if "--routes" in sys.argv:
        print(f"{len(routes(keys[0][1], force=True)):,}개 저장: {ROUTES_FILE}")
        return

    os.makedirs(O.DATA, exist_ok=True)
    route_map = routes(keys[0][1])
    if not route_map:
        sys.exit("노선 목록이 비었다 — --routes로 먼저 받을 것")

    config = O.cfg()
    cap = int(config.get("seoulBusDailyCap", DAILY_CAP))
    planned_calls = min(
        cap, int(config.get("seoulPlannedCallsPerKey", PLANNED_CALLS)))
    configured_interval = float(config.get("seoulIntervalSec", 2175))
    # 정상 슬롯만으로 planned_calls를 넘지 않게 계산한다.
    # cap-planned_calls는 물리 재시도와 재시작 위상 변화에만 쓰는 여유다.
    # ★ 분모를 '노선 수 × 24시간'이 아니라 **실제 운행시간 합**으로 잡는다 —
    #   심야 00~03시엔 702 중 4~6개만 운행하는데(✅ 실측) 전 노선을 계속 찍으면
    #   빈 응답에 예산을 태운다. 운행 노선만 찍으면 같은 예산으로 주기가 당겨진다.
    tail_min = int(config.get("seoulServiceTailMin", 60))
    windows = {rid: service_window(value, tail_min)
               for rid, value in route_map.items()}
    hours = service_route_hours(windows, len(route_map))
    safe_interval = service_safe_interval(hours, len(keys), planned_calls)
    interval = max(configured_interval, safe_interval)
    retry_limit = int(config.get("seoulRetryLimit", 1))
    quota = QuotaReservations(cap)
    qday = quota_day(now())
    ensure_key_counters(keys, qday)
    cleanup_state_files(now(), keys)

    dispatcher = RollingDispatcher(
        keys, dispatcher_fetch, quota.reserve, interval,
        float(config.get("seoulDispatchRate", 1)),
        int(config.get("seoulMaxWorkers", 2)),
        int(config.get("seoulMaxInflight", 2)),
        retry_limit=retry_limit,
    )
    def running_panel(t):
        return [{"routeid": rid, "cityCode": 0} for rid in route_map
                if is_running(windows.get(rid), t)]

    dispatcher.set_routes(running_panel(now()))

    type_counts = {}
    for value in route_map.values():
        type_counts[value["tp"]] = type_counts.get(value["tp"], 0) + 1
    projected = hours * 3600 / interval / len(keys)
    print(f"[{now():%H:%M:%S}] 서울 연속 수집 · 노선 {len(route_map):,}개 · "
          f"키 {len(keys)}개({', '.join(kid for kid, _ in keys)}) · "
          f"노선별 {interval:.0f}초 · 키당 정상예산 "
          f"{projected:,.0f}/{planned_calls:,} · 정지상한 {cap:,}콜", flush=True)
    print("  " + " · ".join(
        f"{TYPE_NM.get(key, key)}{value}" for key, value in sorted(type_counts.items())),
        flush=True)
    if interval > configured_interval + 0.5:
        # 키가 빠졌거나 운행시간 합이 늘어 설정 주기로는 예산을 넘긴다는 뜻이다.
        print(f"[{now():%H:%M:%S}] ⚠️ 예산 보호로 주기 "
              f"{configured_interval:.0f}→{interval:.0f}초 자동 연장 "
              f"(운행시간 {hours:,.0f}h · 키 {len(keys)}개 · 예산 {planned_calls:,}/키)",
              flush=True)

    bands = config["timebands"]
    current_service_day = service_day(now())
    current_quota_day = qday
    rotated_day = None
    # 운행시간에 따라 패널이 바뀌므로 주기적으로 갱신한다 (첫차·막차 진입/이탈).
    # 같은 주기에 쿼터 누계도 DB 이력에 반영한다 — 날짜 전환 때만 쓰면 당일 값이
    # 대시보드·조회에 안 보인다.
    panel_refresh_sec = int(config.get("seoulPanelRefreshSec", 300))
    next_panel = 0.0
    panel_size = None
    written = 0
    last_success = {}
    report_started = time.monotonic()
    report_at = report_started + 60
    cumulative = {
        "attempted": 0, "successful": 0, "retried": 0, "residual": 0,
        "rows": 0, "errors": {},
    }

    try:
        while True:
            t = now()
            dispatcher.raise_if_failed()
            qday = quota_day(t)
            if qday != current_quota_day:
                ensure_key_counters(keys, qday)
                sync_quota_db(current_quota_day, keys)   # 지난 날짜 최종 누계 확정
                cleanup_state_files(t, keys)
                blocked = dispatcher.reset_quota_blocks()
                print(f"[{t:%H:%M:%S}] 쿼터 날짜 전환 {current_quota_day} → {qday} "
                      f"· 차단 해제 {blocked}키", flush=True)
                current_quota_day = qday

            if time.monotonic() >= next_panel:
                panel = running_panel(t)
                if panel_size is not None and len(panel) != panel_size:
                    print(f"[{t:%H:%M:%S}] 운행 패널 {panel_size:,}→{len(panel):,}노선 "
                          f"(운행시간 기준)", flush=True)
                panel_size = len(panel)
                dispatcher.set_routes(panel)
                sync_quota_db(qday, keys)
                next_panel = time.monotonic() + panel_refresh_sec

            new_service_day = service_day(t)
            if new_service_day != current_service_day:
                print(f"[{t:%H:%M:%S}] 운행일 전환 {current_service_day} → "
                      f"{new_service_day} (전일 {written:,}행)", flush=True)
                current_service_day = new_service_day
                written = 0
                last_success = {}
                cumulative = {
                    "attempted": 0, "successful": 0, "retried": 0, "residual": 0,
                    "rows": 0, "errors": {},
                }

            due = O.rotate_due(rotated_day, t)
            if due:
                rotated_day = due
                threading.Thread(
                    target=O.rotate_jsonl, args=(PREFIX,), daemon=True).start()

            first = dispatcher.get(timeout=0.5)
            events = dispatcher.drain(first)
            if events:
                batch_until = time.monotonic() + 0.5
                while len(events) < 128:
                    wait = batch_until - time.monotonic()
                    if wait <= 0:
                        break
                    event = dispatcher.get(timeout=wait)
                    if event is None:
                        break
                    events.extend(dispatcher.drain(event, 128 - len(events)))

            rows_by_day = {}
            for event in events:
                rid, rows, error, observed = event["result"]
                cumulative["attempted"] += 1
                cumulative["retried"] += int(event["retried"])
                if error:
                    cumulative["residual"] += 1
                    cumulative["errors"][error] = cumulative["errors"].get(error, 0) + 1
                    continue
                cumulative["successful"] += 1
                last_success[rid] = observed.timestamp()
                band = O.band_of(observed, bands)
                output_day = service_day(observed)
                for row in rows:
                    row["t"] = observed.isoformat()
                    row["rid"] = rid
                    row["no"] = route_map[rid]["no"]
                    row["tp"] = route_map[rid]["tp"]
                    row["band"] = band
                    row["daytype"] = O.day_type(observed)
                    rows_by_day.setdefault(output_day, []).append(row)
                cumulative["rows"] += len(rows)

            for output_day, output_rows in rows_by_day.items():
                path = os.path.join(OUT_DIR, f"{PREFIX}-{output_day}.jsonl")
                with open(path, "a", encoding="utf-8") as file:
                    for row in output_rows:
                        file.write(json.dumps(row, ensure_ascii=False) + "\n")
                written += len(output_rows)

            if time.monotonic() < report_at:
                continue
            report_now = now()
            snap = dispatcher.snapshot()
            recent_cutoff = report_now.timestamp() - interval * 1.5
            recent_routes = sum(ts >= recent_cutoff for ts in last_success.values())
            status = {
                "updated": report_now.timestamp(),
                "serviceDay": current_service_day,
                "routes": len(route_map),
                "keys": [kid for kid, _ in keys],
                "intervalSec": interval,
                "configuredIntervalSec": configured_interval,
                "plannedCallsPerKey": planned_calls,
                "recentRoutes": recent_routes,
                "attempted": cumulative["attempted"],
                "successful": cumulative["successful"],
                "retried": cumulative["retried"],
                "residual": cumulative["residual"],
                "rows": cumulative["rows"],
                "written": written,
                "errors": cumulative["errors"],
                "inflight": snap["inflight"],
                "quotaBlocked": snap["quotaBlocked"],
            }
            write_status(status)
            calls = " · ".join(
                f"{kid}={read_calls(current_quota_day, kid):,}/{cap:,}"
                for kid, _ in keys)
            elapsed = time.monotonic() - report_started
            print(f"[{report_now:%H:%M:%S}] {elapsed:.0f}초 상태 · "
                  f"성공 {cumulative['successful']}/{cumulative['attempted']} · "
                  f"최근 노선 {recent_routes}/{len(route_map)} · "
                  f"in-flight {snap['inflight']} · {calls}"
                  + (f" · 잔여실패 {cumulative['residual']}"
                     if cumulative["residual"] else ""), flush=True)
            report_started = time.monotonic()
            report_at = report_started + 60
    finally:
        dispatcher.close()


if __name__ == "__main__":
    main()
