import unittest
import time

from collector.rolling_dispatcher import (
    DispatcherError, RollingDispatcher, next_slot)


class RollingDispatcherTests(unittest.TestCase):
    def test_next_slot_keeps_regular_phase(self):
        self.assertEqual(next_slot(100.0, 30.0, 120.0), 130.0)

    def test_next_slot_skips_missed_slots_without_catchup_burst(self):
        self.assertEqual(next_slot(100.0, 30.0, 195.0), 220.0)

    def test_next_slot_is_strictly_after_now(self):
        self.assertEqual(next_slot(100.0, 30.0, 130.0), 160.0)

    def test_slow_route_does_not_block_fast_route(self):
        calls = []

        def fetch(_key, _city, rid):
            calls.append((rid, time.monotonic()))
            if rid == "slow":
                time.sleep(0.16)
            return rid, [], None, __import__("datetime").datetime.now()

        dispatcher = RollingDispatcher(
            [("K1", "key")], fetch, lambda _kid, _n: True,
            interval=0.08, rate=100, workers=4, max_inflight=4)
        self.addCleanup(dispatcher.close)
        dispatcher.set_routes([
            {"routeid": "slow", "cityCode": 1},
            {"routeid": "fast", "cityCode": 1},
        ])
        # CI/저사양 호스트의 스레드 기동 지연을 흡수할 만큼 여유 있게 관찰한다.
        deadline = time.monotonic() + 0.50
        while time.monotonic() < deadline:
            dispatcher.get(timeout=0.03)
        fast = [t for rid, t in calls if rid == "fast"]
        slow = [t for rid, t in calls if rid == "slow"]
        self.assertGreaterEqual(len(fast), 4)
        self.assertLess(len(slow), len(fast))

    def test_same_route_never_overlaps(self):
        active = 0
        maximum = 0

        def fetch(_key, _city, rid):
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            time.sleep(0.08)
            active -= 1
            return rid, [], None, __import__("datetime").datetime.now()

        dispatcher = RollingDispatcher(
            [("K1", "key")], fetch, lambda _kid, _n: True,
            interval=0.03, rate=100, workers=4, max_inflight=4)
        self.addCleanup(dispatcher.close)
        dispatcher.set_routes([{"routeid": "one", "cityCode": 1}])
        deadline = time.monotonic() + 0.24
        while time.monotonic() < deadline:
            dispatcher.get(timeout=0.03)
        self.assertEqual(maximum, 1)

    def test_remove_and_readd_does_not_overlap_running_route(self):
        active = 0
        maximum = 0
        started = __import__("threading").Event()

        def fetch(_key, _city, rid):
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            started.set()
            time.sleep(0.08)
            active -= 1
            return rid, [], None, __import__("datetime").datetime.now()

        dispatcher = RollingDispatcher(
            [("K1", "key")], fetch, lambda _kid, _n: True,
            interval=0.04, rate=100, workers=4, max_inflight=4)
        self.addCleanup(dispatcher.close)
        route = {"routeid": "one", "cityCode": 1}
        dispatcher.set_routes([route])
        self.assertTrue(started.wait(0.2))
        dispatcher.set_routes([])
        dispatcher.set_routes([route])
        deadline = time.monotonic() + 0.22
        while time.monotonic() < deadline:
            dispatcher.get(timeout=0.03)
        self.assertEqual(maximum, 1)

    def test_inflight_limits_can_diverge_per_key(self):
        active = {"key1": 0, "key2": 0}
        maximum = {"key1": 0, "key2": 0}
        lock = __import__("threading").Lock()

        def fetch(key, _city, rid):
            with lock:
                active[key] += 1
                maximum[key] = max(maximum[key], active[key])
            time.sleep(0.06)
            with lock:
                active[key] -= 1
            return rid, [], None, __import__("datetime").datetime.now()

        dispatcher = RollingDispatcher(
            [("K1", "key1"), ("K2", "key2")], fetch,
            lambda _kid, _n: True, interval=0.12, rate=100,
            workers=4, max_inflight=2)
        self.addCleanup(dispatcher.close)
        dispatcher.set_inflight_limit("K1", 1)
        dispatcher.set_inflight_limit("K2", 2)
        self.assertEqual(
            dispatcher.snapshot()["limits"], {"K1": 1, "K2": 2})
        dispatcher.set_routes([
            {"routeid": f"route-{i}", "cityCode": 1} for i in range(12)
        ])
        deadline = time.monotonic() + 0.28
        while time.monotonic() < deadline:
            dispatcher.get(timeout=0.03)
        self.assertLessEqual(maximum["key1"], 1)
        self.assertLessEqual(maximum["key2"], 2)
        self.assertGreater(maximum["key1"], 0)
        self.assertGreater(maximum["key2"], 1)

    def test_retry_reserves_each_physical_call(self):
        calls = 0
        reservations = 0

        def reserve(_kid, n):
            nonlocal reservations
            reservations += n
            return True

        def fetch(_key, _city, rid):
            nonlocal calls
            calls += 1
            if calls == 1:
                return rid, [], "code99:busy", __import__("datetime").datetime.now()
            return rid, [], None, __import__("datetime").datetime.now()

        dispatcher = RollingDispatcher(
            [("K1", "key")], fetch, reserve,
            interval=10, rate=100, workers=2, max_inflight=2)
        self.addCleanup(dispatcher.close)
        dispatcher.set_routes([{"routeid": "one", "cityCode": 1}])
        event = dispatcher.get(timeout=7)
        self.assertIsNotNone(event)
        self.assertTrue(event["retried"])
        self.assertEqual(event["errors"], ["code99:busy"])
        self.assertEqual(calls, 2)
        self.assertEqual(reservations, 2)


    def test_scheduler_exception_surfaces_as_fatal(self):
        # reserve가 디스크 오류로 죽으면 스케줄러만 사망하고 메인은 계속 None을 받는
        # 무징후 유실이 나면 안 된다. 치명 신호가 get()에서 예외로 올라와야 한다.
        def fetch(_key, _city, rid):
            return rid, [], None, __import__("datetime").datetime.now()

        def reserve(_kid, _n):
            raise OSError("ledger write failed")

        dispatcher = RollingDispatcher(
            [("K1", "key")], fetch, reserve,
            interval=0.05, rate=100, workers=2, max_inflight=2)
        self.addCleanup(dispatcher.close)
        dispatcher.set_routes([{"routeid": "one", "cityCode": 1}])
        with self.assertRaises(DispatcherError):
            for _ in range(50):
                dispatcher.get(timeout=0.1)
        # 스케줄러 스레드는 죽었고(메인이 fail-fast 해야 함), 원인 traceback이 남는다.
        self.assertFalse(dispatcher._thread.is_alive())
        self.assertIn("OSError", dispatcher._fatal or "")

    def test_quota_exhausted_blocks_key_without_spin(self):
        # reserve가 계속 False면(쿼터 소진) 첫 실패로 키 전체를 60초 차단해야 한다.
        # 남은 due 노선을 하나씩 미루며 10ms마다 reserve를 때리는 스핀이면 안 된다.
        reserve_calls = 0
        lock = __import__("threading").Lock()

        def reserve(_kid, _n):
            nonlocal reserve_calls
            with lock:
                reserve_calls += 1
            return False

        def fetch(_key, _city, rid):
            return rid, [], None, __import__("datetime").datetime.now()

        dispatcher = RollingDispatcher(
            [("K1", "key")], fetch, reserve,
            interval=0.05, rate=100, workers=2, max_inflight=2)
        self.addCleanup(dispatcher.close)
        dispatcher.set_routes([
            {"routeid": f"r{i}", "cityCode": 1} for i in range(5)
        ])
        time.sleep(0.4)
        with lock:
            calls = reserve_calls
        # 스핀이면 이 창에서 수십~수백 회다. 차단이 걸리면 한 자릿수.
        self.assertLessEqual(calls, 3)

    def test_quota_reset_wakes_blocked_key_immediately(self):
        allowed = False

        def reserve(_kid, _n):
            return allowed

        def fetch(_key, _city, rid):
            return rid, [], None, __import__("datetime").datetime.now()

        dispatcher = RollingDispatcher(
            [("K1", "key")], fetch, reserve,
            interval=10, rate=100, workers=2, max_inflight=2)
        self.addCleanup(dispatcher.close)
        dispatcher.set_routes([{"routeid": "one", "cityCode": 1}])

        deadline = time.monotonic() + 0.5
        while (dispatcher.snapshot()["quotaBlocked"] == 0
               and time.monotonic() < deadline):
            time.sleep(0.01)
        self.assertEqual(dispatcher.snapshot()["quotaBlocked"], 1)

        allowed = True
        self.assertEqual(dispatcher.reset_quota_blocks_by_key(), ["K1"])
        event = dispatcher.get(timeout=0.5)
        self.assertIsNotNone(event)
        self.assertEqual(event["result"][0], "one")


if __name__ == "__main__":
    unittest.main()
