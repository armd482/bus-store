"""노선별 독립 주기의 연속 요청 스케줄러.

네트워크 요청만 백그라운드에서 병렬 실행한다. SQLite·JSONL·차량 전이 상태는
호출자가 단일 스레드에서 결과 큐를 소비하며 갱신해야 한다.
"""

import heapq
import queue
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor


class DispatcherError(RuntimeError):
    """스케줄러 스레드가 죽었다는 치명 신호. 메인이 get()에서 받아 fail-fast 한다."""


def next_slot(previous_due, interval, current):
    """밀린 슬롯을 몰아서 실행하지 않고 current 이후 첫 정규 슬롯을 반환한다."""
    due = previous_due + interval
    if due <= current:
        due += (int((current - due) // interval) + 1) * interval
    return due


class RollingDispatcher:
    """키별 rate/in-flight를 지키면서 각 노선을 독립 주기로 호출한다."""

    def __init__(self, keys, fetch, reserve, interval, rate, workers,
                 max_inflight, hold=0, retry_limit=1):
        self.fetch = fetch
        self.reserve = reserve
        self.interval = float(interval)
        self.rate = float(rate)
        self.hold = float(hold)
        self.retry_limit = retry_limit
        self._keys = {kid: key for kid, key in keys}
        self._key_order = [kid for kid, _ in keys]
        self._states = {
            kid: {"heap": [], "inflight": 0, "limit": max_inflight,
                  "next_submit": 0.0,
                  "quota_blocked_until": 0.0, "quota_was_blocked": False}
            for kid in self._key_order
        }
        self._routes = {}
        self._futures = {}
        self._results = queue.Queue()
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._seq = 0
        self._fatal = None
        # maxInflight는 키별 상한이다. 활성 키를 동시에 담을 수 있게 풀만 넉넉히 둔다.
        self._executor = ThreadPoolExecutor(
            max_workers=max(workers, max_inflight) * max(1, len(keys)))
        self._thread = threading.Thread(
            target=self._run, name="bus-rolling-dispatcher", daemon=True)
        self._thread.start()

    @property
    def inflight_limits(self):
        with self._lock:
            return {kid: state["limit"] for kid, state in self._states.items()}

    def set_inflight_limit(self, kid, value):
        """한 키의 AIMD 상한만 바꾼다. 이미 실행 중인 요청은 취소하지 않는다."""
        with self._lock:
            self._states[kid]["limit"] = max(1, int(value))
        self._wake.set()

    def reset_quota_blocks(self):
        """자정에 차단을 해제하고, 전날 cap 차단 이력이 있던 키 수를 반환한다."""
        return len(self.reset_quota_blocks_by_key())

    def reset_quota_blocks_by_key(self):
        """자정에 차단을 해제하고, 전날 cap 차단 이력이 있던 키 ID를 반환한다."""
        with self._lock:
            blocked = [
                kid for kid, state in self._states.items()
                if state["quota_was_blocked"]]
            for state in self._states.values():
                state["quota_blocked_until"] = 0.0
                state["quota_was_blocked"] = False
        self._wake.set()
        return blocked

    def raise_if_failed(self):
        """메인 루프가 serviceWindow 대기 중이어도 스케줄러 사망을 즉시 감지한다."""
        if self._fatal is not None:
            raise DispatcherError(self._fatal)

    def set_routes(self, picked):
        """활성 패널 교체. 유지 노선의 위상은 보존하고 신규 노선만 30초에 분산한다."""
        now = time.monotonic()
        wanted = {p["routeid"]: p for p in picked}
        with self._lock:
            # 빠진 노선은 삭제하지 않고 active=False tombstone으로 남긴다. 곧장 지우면
            # heap에 남은 옛 generation 항목과, 같은 routeid가 나중에 재진입할 때의
            # generation이 우연히 겹쳐 스테일 항목을 되살릴 수 있다. dict 크기는 노선
            # 풀 크기로 상한이라(하루 수천 개) 메모리 영향은 무시할 만하다.
            for rid, state in list(self._routes.items()):
                if rid not in wanted:
                    state["active"] = False

            # 막 빠진 노선의 요청이 아직 끝나지 않았는데 같은 노선이 재진입하면
            # 기존 세대를 되살린다. 새 세대를 즉시 만들면 짧은 순간 같은 routeid가
            # 두 번 실행될 수 있다.
            retained = {
                rid for rid in wanted
                if rid in self._routes
                and (self._routes[rid]["active"]
                     or self._routes[rid]["inflight"])
            }
            new = [p for p in picked if p["routeid"] not in retained]
            loads = {kid: 0 for kid in self._key_order}
            for state in self._routes.values():
                if state["active"]:
                    loads[state["keyid"]] += 1

            assigned = {kid: [] for kid in self._key_order}
            for p in new:
                kid = min(self._key_order, key=lambda x: (loads[x], x))
                loads[kid] += 1
                assigned[kid].append(p)

            for kid, routes in assigned.items():
                count = len(routes)
                for i, p in enumerate(routes):
                    rid = p["routeid"]
                    old = self._routes.get(rid)
                    generation = (old["generation"] + 1) if old else 1
                    # 신규 패널이 첫 순간에 몰리지 않게 interval 전체에 균등 배치한다.
                    due = now + (self.interval * i / count if count else 0)
                    state = {
                        "meta": p, "keyid": kid, "generation": generation,
                        "active": True, "inflight": False, "next_due": due,
                    }
                    self._routes[rid] = state
                    self._push(kid, due, rid, generation, 0, due, [])

            for rid in retained:
                self._routes[rid]["active"] = True
                self._routes[rid]["meta"] = wanted[rid]
        self._wake.set()

    def get(self, timeout=None):
        try:
            item = self._results.get(timeout=timeout)
        except queue.Empty:
            return None
        if "fatal" in item:
            raise DispatcherError(item["fatal"])
        return item

    def drain(self, first=None, limit=256):
        out = []
        if first is not None:
            out.append(first)
        while len(out) < limit:
            try:
                item = self._results.get_nowait()
            except queue.Empty:
                break
            if "fatal" in item:
                raise DispatcherError(item["fatal"])
            out.append(item)
        return out

    def close(self):
        self._stop.set()
        self._wake.set()
        self._thread.join(timeout=5)
        self._executor.shutdown(wait=False)

    def snapshot(self):
        with self._lock:
            limits = {
                kid: state["limit"] for kid, state in self._states.items()}
            routes_by_key = {kid: 0 for kid in self._key_order}
            for route in self._routes.values():
                if route["active"]:
                    routes_by_key[route["keyid"]] += 1
            return {
                "routes": sum(routes_by_key.values()),
                "routesByKey": routes_by_key,
                "inflight": sum(s["inflight"] for s in self._states.values()),
                "limits": limits,
                "limitTotal": sum(limits.values()),
                "queued": sum(len(s["heap"]) for s in self._states.values()),
                "quotaBlocked": sum(
                    s["quota_blocked_until"] > time.monotonic()
                    for s in self._states.values()),
            }

    def _push(self, kid, due, rid, generation, attempt, origin_due, errors):
        self._seq += 1
        heapq.heappush(
            self._states[kid]["heap"],
            (due, self._seq, rid, generation, attempt, origin_due, errors))

    def _guarded_fetch(self, key, meta):
        result = self.fetch(key, meta["cityCode"], meta["routeid"])
        err = result[2]
        if self.hold and err and ("Timeout" in err or "timed out" in err):
            time.sleep(self.hold)
        return result

    def _dispatch_due(self, now):
        for kid in self._key_order:
            ks = self._states[kid]
            if now < ks["quota_blocked_until"]:
                continue
            # 만료된 차단 표식을 정리해 snapshot이 현재 상태만 보고하게 한다.
            ks["quota_blocked_until"] = 0.0
            while (ks["heap"] and ks["heap"][0][0] <= now
                   and ks["inflight"] < ks["limit"]
                   and now >= ks["next_submit"]):
                due, _, rid, generation, attempt, origin_due, errors = heapq.heappop(ks["heap"])
                route = self._routes.get(rid)
                if (route is None or route["generation"] != generation
                        or route["inflight"] or (not route["active"] and attempt == 0)):
                    continue
                if not self.reserve(kid, 1):
                    # 해당 키 쿼터가 찼다. 팝한 슬롯을 원래 due로 되돌리고 이 키 전체를
                    # 60초 차단한다 — 남은 due 노선을 하나씩 뒤로 밀며 10ms마다 깨어나는
                    # 스핀 대신, 1분에 한 번만 다시 확인한다(자정 리셋·패널 재투입 감지).
                    self._push(kid, due, rid, generation, attempt, origin_due, errors)
                    ks["quota_blocked_until"] = now + 60
                    ks["quota_was_blocked"] = True
                    break
                route["inflight"] = True
                ks["inflight"] += 1
                ks["next_submit"] = max(ks["next_submit"], now) + 1.0 / self.rate
                fut = self._executor.submit(
                    self._guarded_fetch, self._keys[kid], route["meta"])
                # 완료를 200ms 폴링까지 기다리지 않고 즉시 회수해 다음 슬롯을 계산한다.
                fut.add_done_callback(lambda _f: self._wake.set())
                self._futures[fut] = {
                    "kid": kid, "rid": rid, "generation": generation,
                    "attempt": attempt, "origin_due": origin_due,
                    "errors": list(errors), "started": time.monotonic(),
                }
                # 한 키의 다음 제출 시각 전에는 while 조건이 거짓이 된다.

    def _collect_done(self, now):
        for fut, job in list(self._futures.items()):
            if not fut.done():
                continue
            del self._futures[fut]
            kid, rid = job["kid"], job["rid"]
            self._states[kid]["inflight"] -= 1
            route = self._routes.get(rid)
            if route and route["generation"] == job["generation"]:
                route["inflight"] = False
            try:
                result = fut.result()
            except Exception as exc:
                result = (rid, [], type(exc).__name__, None)
            err = result[2]
            errors = job["errors"] + ([err] if err else [])

            if err and job["attempt"] < self.retry_limit and route is not None:
                pause = 6.0 if "code99" in err else 2.0
                self._push(kid, now + pause, rid, job["generation"],
                           job["attempt"] + 1, job["origin_due"], errors)
                continue

            self._results.put({
                "result": result, "keyid": kid,
                "retried": job["attempt"] > 0,
                "errors": errors,
                "duration": time.monotonic() - job["started"],
            })
            if route and route["generation"] == job["generation"] and route["active"]:
                due = next_slot(job["origin_due"], self.interval, now)
                route["next_due"] = due
                self._push(kid, due, rid, job["generation"], 0, due, [])

    def _next_wait(self, now):
        waits = []
        for ks in self._states.values():
            if now < ks["quota_blocked_until"]:
                # 쿼터 차단 중 — 해제 시각에만 다시 본다. heap을 근거로 깨지 않는다.
                waits.append(ks["quota_blocked_until"] - now)
                continue
            if ks["inflight"] >= ks["limit"]:
                # 슬롯 포화 — due가 지나도 지금은 제출 못 한다. in-flight future가
                # 끝나면 완료 콜백이 _wake를 세우므로, heap을 근거로 10ms마다 깨지 않는다.
                continue
            if ks["heap"]:
                # due와 rate gate 둘 다 열린 시각에만 제출할 수 있다. next_submit이
                # 이미 과거라는 이유로 10ms마다 깨어나는 idle busy loop를 만들지 않는다.
                ready = max(ks["heap"][0][0], ks["next_submit"])
                waits.append(max(0.01, ready - now))
        # 미래 슬롯도 실행 중 future도 없으면 set_routes/close 같은 명시적 wake까지 잔다.
        return min(waits) if waits else None

    def _run(self):
        try:
            while not self._stop.is_set():
                # clear를 상태 확인보다 먼저 한다. 완료 콜백/set_routes가 그 뒤 set하면
                # wait 직전이어도 신호가 보존되고, 그 전에 set했다면 아래 상태 스캔이
                # 이미 해당 변경을 관찰한다.
                self._wake.clear()
                now = time.monotonic()
                with self._lock:
                    self._collect_done(now)
                    self._dispatch_due(now)
                    wait = self._next_wait(now)
                self._wake.wait(wait)
        except Exception:
            # 스케줄러 사망 = 수집 영구 중단. 조용히 두면 메인은 계속 None을 받고
            # 프로세스는 살아 있어 systemd/watchdog이 재시작하지 않는다(무징후 유실).
            # 치명 신호를 큐에 넣어 메인 get()에서 예외로 올려 fail-fast → 수집 스레드가
            # 죽고 server.py watchdog이 프로세스를 non-zero 종료해 재시작시킨다.
            self._fatal = traceback.format_exc()
            self._stop.set()
            self._results.put({"fatal": self._fatal})
