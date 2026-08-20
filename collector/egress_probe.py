#!/usr/bin/env python3
"""TAGO 출발 egress의 고정 동시성 비교 프로브.

정규 버스 수집기는 실행하지 않은 상태에서만 쓴다. API 물리 호출은 실행 전에
키별 장부에 예약하며, 기본 실행은 dry-run이다. ``--execute``를 명시해야 한다.
"""

import argparse
import hashlib
import json
import os
import random
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import bus_collector as B
import orchestrator as O
from http_pool import KeyedHTTPSPool


# A/B를 각각 단일 egress로 재서 EIP 개체차를 dual 결과와 혼동하지 않는다.
# 7분 호출창 + 6회의 5초 연결 정리로, 9,500 물리 호출 안전 상한 안에 둔다.
DEFAULT_PLAN = (
    "single-a:52,dual:52,dual:52,single-b:52,"
    "single-a:64,single-b:64,dual:64"
)


class ProbeAbort(RuntimeError):
    """사전 중단 기준을 만족했다."""


def percentile(values, fraction):
    return B.percentile(values, fraction) if values else 0.0


def parse_plan(text):
    phases = []
    for item in text.split(","):
        mode, _, value = item.strip().partition(":")
        if mode not in ("single", "single-a", "single-b", "dual") or not value.isdigit():
            raise ValueError(f"잘못된 phase: {item!r} (예: single-a:44)")
        target = int(value)
        if target < 1 or (mode == "dual" and target % 2):
            raise ValueError(f"{item!r}: dual target은 양의 짝수여야 함")
        phases.append((mode, target))
    if not phases:
        raise ValueError("phase가 비어 있음")
    return phases


def source_mode(keys, sources, groups, mode):
    """mode에 맞는 실제 인증키→사설 IP, key ID→egress 그룹을 만든다."""
    if mode == "dual":
        if len(set(groups.values())) != 2:
            raise ValueError("dual probe에는 정확히 두 egress 그룹이 필요")
        return dict(sources), dict(groups)
    group_sources = {}
    for kid, actual in keys:
        group = groups[kid]
        source = sources[actual]
        prior = group_sources.setdefault(group, source)
        if prior != source:
            raise ValueError(f"egress 그룹 {group}에 서로 다른 source IP가 있습니다")
    ordered_groups = sorted(group_sources)
    if len(ordered_groups) != 2:
        raise ValueError("single probe에는 정확히 두 egress 그룹이 필요")
    # 기존 single은 A 호환 별칭이다. 새 실험은 반드시 single-a/b를 쓴다.
    source = group_sources[ordered_groups[1 if mode == "single-b" else 0]]
    return ({actual: source for _kid, actual in keys},
            {kid: "single" for kid, _actual in keys})


def phase_valid(avg_inflight, target):
    """rate gate 때문에 목표 동시성을 못 채운 구간은 비교하면 안 된다."""
    return avg_inflight >= target * 0.90


def phase_group_limits(mode, target, groups):
    """단일 mode는 target 전체를 한 source가 쓰고 dual만 반반 나눈다."""
    if mode != "dual":
        return {"single": target}
    return {group: target // 2 for group in set(groups.values())}


class PhysicalCalls:
    """장부 예약과 별개로 실제 물리 호출 수를 정확히 세는 스레드 안전 카운터."""

    def __init__(self, maximum):
        self.maximum = int(maximum)
        self.value = 0
        self._lock = threading.Lock()

    def allow(self, n=1):
        with self._lock:
            if self.value + n > self.maximum:
                return False
            self.value += n
            return True


def load_routes():
    routes = B.load_cached_panel(1000)
    if routes:
        return routes
    conn = O.connect_readonly()
    try:
        return [
            {"routeid": routeid, "cityCode": city}
            for routeid, city in conn.execute(
                "SELECT routeid,cityCode FROM route "
                "WHERE cityCode IS NOT NULL ORDER BY routeid LIMIT 1000")
        ]
    finally:
        conn.close()


def shuffled_routes(routes, seed):
    """전체 패널을 결정적으로 섞어 phase 첫 구간의 순서 편향을 없앤다."""
    shuffled = list(routes)
    random.Random(seed).shuffle(shuffled)
    return shuffled


def route_order_digest(routes):
    """결과 파일만으로도 어떤 패널 순서를 썼는지 비교할 수 있게 한다."""
    text = "\n".join(str(route["routeid"]) for route in routes)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def require_collector_stopped(unit="findpath.service"):
    """운영 수집기와 겹치면 측정·장부가 모두 오염되므로 fail closed 한다."""
    try:
        check = subprocess.run(
            ["systemctl", "--user", "is-active", "--quiet", unit],
            check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except OSError as exc:
        raise RuntimeError("수집기 상태를 확인할 systemctl을 실행할 수 없습니다") from exc
    if check.returncode == 0:
        raise RuntimeError(
            f"{unit}가 실행 중입니다. 수집기를 명시적으로 멈춘 뒤에만 probe를 실행하세요")


def classify(err):
    text = err or ""
    return {
        "code99": "code99" in text,
        "http429": "HTTP429" in text,
        "timeout": "Timeout" in text or "timed out" in text,
    }


def run_phase(mode, target, duration, rate, keys, routes, quota, key_cap,
              source_addresses, key_groups, physical, total_deadline,
              bucket_min_samples):
    """한 phase를 rate gate 하에서 가능한 한 목표 동시성으로 유지한다."""
    sources, groups = source_mode(keys, source_addresses, key_groups, mode)
    group_limits = phase_group_limits(mode, target, groups)
    kid_for_actual = {actual: kid for kid, actual in keys}

    def reserve_actual(actual):
        kid = kid_for_actual[actual]
        if not physical.allow(1):
            return False
        if quota.reserve(kid, 1):
            return True
        # quota가 거부한 경우에는 이 호출을 실제 호출로 세지 않는다.
        with physical._lock:
            physical.value -= 1
        return False

    pool = KeyedHTTPSPool(
        B.BASE_HOST, max_per_key=target,
        timeout=float(O.cfg().get("busHttpTimeoutSec", 15)),
        retry_reserver=reserve_actual, source_addresses=sources)
    previous_pool = B._HTTP_POOL
    B._HTTP_POOL = pool
    started = time.monotonic()
    cutoff = started + duration
    last_area_at = started
    inflight_area = 0.0
    max_inflight = 0
    futures = {}
    group_inflight = {group: 0 for group in group_limits}
    key_due = {kid: started + i / (len(keys) * rate)
               for i, (kid, _actual) in enumerate(keys)}
    route_index = {kid: i for i, (kid, _actual) in enumerate(keys)}
    key_cursor = 0
    durations, errors = [], []
    buckets = {}
    aborted = None
    completed_routes = set()
    route_hits = {}
    vehicle_count = 0
    last_gate_at = started
    underfilled_seconds = 0.0
    rate_gated_underfilled_seconds = 0.0

    def account(at):
        nonlocal last_area_at, inflight_area, max_inflight
        # phase 종료 뒤 drain은 다음 mode 오염을 막기 위한 정리 시간일 뿐, 60초
        # 고정 관측창의 실제 평균 in-flight/throughput에 넣지 않는다.
        bounded = min(at, cutoff)
        inflight_area += max(0.0, bounded - last_area_at) * len(futures)
        last_area_at = max(last_area_at, bounded)
        max_inflight = max(max_inflight, len(futures))

    def add_bucket(at, err):
        index = int((at - started) // 10)
        bucket = buckets.setdefault(index, {"done": 0, "code99": 0,
                                             "http429": 0})
        bucket["done"] += 1
        flags = classify(err)
        bucket["code99"] += flags["code99"]
        bucket["http429"] += flags["http429"]
        # phase 경계의 1~2건만으로 50%가 되는 우연 중단을 막는다. 20건이면
        # 20%는 적어도 네 개의 실제 거절을 뜻하며, 10초 버킷 기준은 유지된다.
        if (bucket["done"] >= bucket_min_samples
                and bucket["code99"] / bucket["done"] >= 0.20):
            raise ProbeAbort("10초 bucket code99 >= 20%")
        if (bucket["done"] >= bucket_min_samples
                and bucket["http429"] / bucket["done"] >= 0.10):
            raise ProbeAbort("10초 bucket HTTP429 >= 10%")

    def account_gate(at):
        """목표보다 빈 슬롯이 key rate gate 탓인지, 느린 응답 탓인지 분리한다."""
        nonlocal last_gate_at, underfilled_seconds, rate_gated_underfilled_seconds
        elapsed = max(0.0, at - last_gate_at)
        if len(futures) < target:
            underfilled_seconds += elapsed
            has_capacity = any(
                group_inflight[group] < limit
                for group, limit in group_limits.items())
            has_rate_ready_key = any(
                group_inflight[groups[kid]] < group_limits[groups[kid]]
                and at >= key_due[kid]
                for kid, _actual in keys)
            if has_capacity and not has_rate_ready_key:
                rate_gated_underfilled_seconds += elapsed
        last_gate_at = at

    def submit(kid, actual):
        nonlocal key_cursor
        route = routes[route_index[kid] % len(routes)]
        route_index[kid] += len(keys)
        group = groups[kid]
        account(time.monotonic())
        future = executor.submit(B.fetch, actual, route["cityCode"], route["routeid"])
        futures[future] = {"kid": kid, "group": group,
                           "routeid": route["routeid"],
                           "started": time.monotonic()}
        group_inflight[group] += 1
        key_due[kid] = max(key_due[kid], time.monotonic()) + 1.0 / rate
        key_cursor = (key_cursor + 1) % len(keys)

    try:
        with ThreadPoolExecutor(max_workers=target) as executor:
            while True:
                now = time.monotonic()
                account_gate(now)
                if now >= total_deadline:
                    raise ProbeAbort("총 경과 12분 초과")
                # 완료 요청을 먼저 수확해 가능한 슬롯을 즉시 다시 채운다.
                for future, job in list(futures.items()):
                    if not future.done():
                        continue
                    account(now)
                    del futures[future]
                    group_inflight[job["group"]] -= 1
                    items = []
                    try:
                        _routeid, items, err, _obs = future.result()
                    except Exception as exc:  # B.fetch가 대부분 자체 포착하지만 방어한다.
                        err = type(exc).__name__
                    elapsed = now - job["started"]
                    durations.append(elapsed)
                    errors.append(err)
                    completed_routes.add(job["routeid"])
                    route_hits[job["routeid"]] = route_hits.get(job["routeid"], 0) + 1
                    if not err:
                        vehicle_count += len(items)
                    add_bucket(now, err)
                    if len(durations) >= 10 and percentile(durations, 0.50) > 10:
                        raise ProbeAbort("phase p50 지연 > 10초")

                if physical.value >= physical.maximum:
                    raise ProbeAbort("누적 물리 호출 상한 도달")

                if now < cutoff:
                    # 목표·그룹·key rate를 모두 만족하는 다음 키를 찾는다.
                    for offset in range(len(keys)):
                        kid, actual = keys[(key_cursor + offset) % len(keys)]
                        group = groups[kid]
                        if (len(futures) < target
                                and group_inflight[group] < group_limits[group]
                                and now >= key_due[kid]):
                            if not reserve_actual(actual):
                                raise ProbeAbort(f"{kid} 쿼터 예약 거부")
                            submit(kid, actual)
                            break
                    else:
                        time.sleep(0.005)
                        continue
                    continue

                if not futures:
                    break
                # phase 종료 뒤에는 새 요청을 만들지 않고 진행 중 요청만 정리한다.
                time.sleep(0.01)
    except ProbeAbort as exc:
        aborted = str(exc)
    finally:
        account(time.monotonic())
        account_gate(time.monotonic())
        B._HTTP_POOL = previous_pool
        pool.close()

    wall_elapsed = max(0.001, time.monotonic() - started)
    measured_elapsed = min(duration, wall_elapsed)
    code99 = sum(classify(err)["code99"] for err in errors)
    http429 = sum(classify(err)["http429"] for err in errors)
    timeouts = sum(classify(err)["timeout"] for err in errors)
    return {
        "mode": mode, "target": target, "elapsed": measured_elapsed,
        "wallElapsed": wall_elapsed,
        "completed": len(errors), "success": sum(not err for err in errors),
        "code99": code99, "http429": http429, "timeouts": timeouts,
        "code99Rate": code99 / max(1, len(errors)),
        "http429Rate": http429 / max(1, len(errors)),
        "throughput": len(errors) / measured_elapsed,
        "latencyP50": percentile(durations, 0.50),
        "latencyP90": percentile(durations, 0.90),
        "avgInflight": inflight_area / measured_elapsed,
        "maxInflight": max_inflight,
        "targetOccupancy": (inflight_area / measured_elapsed) / target,
        "underfilledSeconds": underfilled_seconds,
        "rateGatedUnderfilledSeconds": rate_gated_underfilled_seconds,
        "rateGateShareOfUnderfill": (
            rate_gated_underfilled_seconds / max(0.001, underfilled_seconds)),
        "completedRoutes": len(completed_routes),
        "routeHitMin": min(route_hits.values(), default=0),
        "routeHitMax": max(route_hits.values(), default=0),
        "meanVehiclesPerSuccess": vehicle_count / max(1, sum(not err for err in errors)),
        "valid": phase_valid(inflight_area / measured_elapsed, target),
        "aborted": aborted, "buckets": buckets,
        "physicalCalls": physical.value,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--plan", default=DEFAULT_PLAN)
    ap.add_argument("--duration", type=float, default=60)
    ap.add_argument("--rate-per-key", type=float, default=6)
    ap.add_argument("--max-calls", type=int, default=9500)
    ap.add_argument("--max-total-sec", type=float, default=720)
    ap.add_argument("--bucket-min-samples", type=int, default=20,
                    help="10초 오류율 중단 판정의 최소 완료 수")
    ap.add_argument("--seed", type=int, default=20260729,
                    help="전체 패널 순서를 섞을 결정적 난수 시드")
    ap.add_argument("--collector-unit", default="findpath.service",
                    help="--execute 전 반드시 inactive여야 하는 운영 수집기 unit")
    ap.add_argument("--execute", action="store_true",
                    help="실제 API 호출·장부 예약을 허용")
    args = ap.parse_args()
    phases = parse_plan(args.plan)
    if args.duration <= 0 or args.rate_per_key <= 0:
        raise SystemExit("duration/rate-per-key는 양수여야 함")

    keys = B.load_keys()
    config = O.cfg()
    sources, groups = B.load_egress_sources(keys, config)
    routes = shuffled_routes(load_routes(), args.seed)
    if len(keys) != 4 or len(routes) < len(keys) or not sources:
        raise SystemExit("4개 활성 키·두 egress·캐시 노선이 모두 필요")

    print("[probe] phases=" + ", ".join(f"{m}:{t}" for m, t in phases))
    route_digest = route_order_digest(routes)
    print(f"[probe] {len(routes)} shuffled routes (seed {args.seed}, {route_digest[:12]}) "
          f"· key rate {args.rate_per_key:g}/s · max physical {args.max_calls} "
          f"· max wall {args.max_total_sec:g}s")
    if not args.execute:
        print("[probe] dry-run — 실제 실행에는 --execute가 필요")
        return

    try:
        require_collector_stopped(args.collector_unit)
    except RuntimeError as exc:
        raise SystemExit(f"[probe] 실행 거부: {exc}")

    qday = B.quota_day(B.now())
    B.ensure_key_counters(keys, qday)
    quota = B.QuotaReservations(int(config["busKeyDailyCap"]))
    physical = PhysicalCalls(args.max_calls)
    deadline = time.monotonic() + args.max_total_sec
    results = []
    for index, (mode, target) in enumerate(phases, 1):
        result = run_phase(
            mode, target, args.duration, args.rate_per_key, keys, routes,
            quota, int(config["busKeyDailyCap"]), sources, groups,
            physical, deadline, args.bucket_min_samples)
        results.append(result)
        print("[probe] {mode}:{target} {throughput:.2f}/s "
              "p50={latencyP50:.2f}s code99={code99Rate:.1%} "
              "inflight={avgInflight:.1f}/{target} {valid}"
              .format(**result), flush=True)
        if result["aborted"]:
            print(f"[probe] 중단: {result['aborted']}", flush=True)
            break
        if index < len(phases):
            # Keep-Alive와 서버측 세션이 다음 모드로 섞이지 않게 짧게 비운다.
            time.sleep(5)

    output = {
        "startedAt": time.time(), "plan": phases,
        "ratePerKey": args.rate_per_key, "routeCount": len(routes),
        "routeSeed": args.seed, "routeOrderDigest": route_digest,
        "results": results,
        "physicalCalls": physical.value,
    }
    path = os.path.join(O.DATA, f"egress-probe-{int(time.time())}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"[probe] 결과 {path} · 물리 호출 {physical.value}", flush=True)


if __name__ == "__main__":
    main()
