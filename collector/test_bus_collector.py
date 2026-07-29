import unittest
import threading
from datetime import datetime
from unittest.mock import patch

from bus_collector import (
    QuotaReservations, counter_check_due, midnight_inflight, next_inflight,
    next_inflight_limits, reserve_calls)


class InflightPolicyTests(unittest.TestCase):
    def test_empty_or_tiny_window_does_not_raise_limit(self):
        self.assertEqual(
            next_inflight(8, 22, False, attempted=0, minimum_samples=80), 8)
        self.assertEqual(
            next_inflight(10, 22, False, attempted=79, minimum_samples=80), 10)
        self.assertEqual(
            next_inflight(12, 22, True, attempted=1, minimum_samples=80), 8)

    def test_sufficient_clean_window_recovers_and_code99_retreats(self):
        self.assertEqual(
            next_inflight(10, 22, False, attempted=80, minimum_samples=80), 12)
        self.assertEqual(
            next_inflight(12, 22, True, attempted=80, minimum_samples=80), 8)

    def test_code99_retreats_only_the_affected_key(self):
        self.assertEqual(
            next_inflight_limits(
                {"K1": 20, "K2": 20}, 22,
                {"K1": 50, "K2": 50},
                {"K1": ["code99:busy"], "K2": []},
                {"K1": 170, "K2": 170}),
            {"K1": 12, "K2": 22})

    def test_midnight_only_clamps_when_quota_was_blocked(self):
        self.assertEqual(midnight_inflight(22, True), 10)
        self.assertEqual(midnight_inflight(8, True), 8)
        self.assertEqual(midnight_inflight(12, False), 12)

    def test_counter_migration_is_gated_by_day_or_safety_interval(self):
        self.assertTrue(counter_check_due("2026-07-27", None, 10, 0))
        self.assertFalse(counter_check_due("2026-07-27", "2026-07-27", 20, 30))
        self.assertTrue(counter_check_due("2026-07-28", "2026-07-27", 20, 30))
        self.assertTrue(counter_check_due("2026-07-27", "2026-07-27", 30, 30))


class QuotaReservationsTests(unittest.TestCase):
    def make_quota(self, cap=100, block=64):
        current = [datetime(2026, 7, 26, 23, 59)]
        ledger = {}

        def allocate(day, wanted, limit, kid):
            old = ledger.get((day, kid), 0)
            grant = min(wanted, max(0, limit - old))
            ledger[(day, kid)] = old + grant
            return grant

        quota = QuotaReservations(
            cap, block, clock=lambda: current[0], allocate=allocate)
        return quota, current, ledger

    def test_calendar_day_discards_previous_days_unused_lease(self):
        quota, current, ledger = self.make_quota()
        self.assertTrue(quota.reserve("K1", 1))
        self.assertEqual(ledger[("2026-07-26", "K1")], 64)

        current[0] = datetime(2026, 7, 27, 0, 0)
        self.assertTrue(quota.reserve("K1", 1))
        # 전날 남은 63개를 재사용하지 않고 새 날짜에 새 블록을 기록해야 한다.
        self.assertEqual(ledger[("2026-07-27", "K1")], 64)

    def test_partial_final_block_stops_exactly_at_cap(self):
        quota, _current, ledger = self.make_quota(cap=100, block=64)
        self.assertTrue(all(quota.reserve("K1", 1) for _ in range(100)))
        self.assertFalse(quota.reserve("K1", 1))
        self.assertEqual(ledger[("2026-07-26", "K1")], 100)

    def test_disk_reservation_read_and_write_are_atomic_at_cap(self):
        ledger = {}
        grants = []

        def read(day, kid=None):
            return ledger.get((day, kid), 0)

        def write(day, value, kid=None):
            ledger[(day, kid)] = value

        def worker():
            grants.append(reserve_calls("2026-07-27", 64, 100, "K1"))

        with patch("bus_collector.read_calls", side_effect=read), \
                patch("bus_collector._write_calls", side_effect=write):
            threads = [threading.Thread(target=worker) for _ in range(4)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

        self.assertEqual(sum(grants), 100)
        self.assertEqual(ledger[("2026-07-27", "K1")], 100)
        self.assertEqual(ledger[("2026-07-27", None)], 100)


if __name__ == "__main__":
    unittest.main()
