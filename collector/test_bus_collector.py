import unittest
import threading
from datetime import datetime
from unittest.mock import patch

from bus_collector import (
    QuotaReservations, counter_check_due, next_global_inflight,
    quota_panel_target, reserve_calls)


class InflightPolicyTests(unittest.TestCase):
    def test_sparse_code99_only_resets_recovery_counter(self):
        self.assertEqual(
            next_global_inflight(
                44, 40, 64, 1, 200, 44, 2, 3, []),
            (44, 0, [False]))

    def test_five_percent_code99_retreats_immediately_by_four(self):
        self.assertEqual(
            next_global_inflight(
                44, 40, 64, 5, 100, 44, 2, 3, []),
            (40, 0, []))

    def test_two_pressure_windows_out_of_three_retreat(self):
        self.assertEqual(
            next_global_inflight(
                44, 40, 64, 2, 100, 44, 2, 3, []),
            (44, 0, [True]))
        self.assertEqual(
            next_global_inflight(
                44, 40, 64, 0, 100, 44, 0, 3, [True]),
            (44, 1, [True, False]))
        self.assertEqual(
            next_global_inflight(
                44, 40, 64, 2, 100, 44, 1, 3, [True, False]),
            (40, 0, []))

    def test_retreat_never_crosses_floor(self):
        self.assertEqual(
            next_global_inflight(
                42, 40, 64, 5, 100, 42, 2, 3, []),
            (40, 0, []))

    def test_global_limit_recovers_two_after_required_clean_windows(self):
        self.assertEqual(
            next_global_inflight(
                44, 40, 64, 0, 100, 44, 1, 3, []),
            (44, 2, [False]))
        self.assertEqual(
            next_global_inflight(
                44, 40, 64, 0, 100, 44, 2, 3, [False]),
            (46, 0, [False, False]))

    def test_tiny_window_does_not_count_as_clean(self):
        self.assertEqual(
            next_global_inflight(
                44, 40, 64, 0, 43, 44, 2, 3, []),
            (44, 0, [False]))

    def test_counter_migration_is_gated_by_day_or_safety_interval(self):
        self.assertTrue(counter_check_due("2026-07-27", None, 10, 0))
        self.assertFalse(counter_check_due("2026-07-27", "2026-07-27", 20, 30))
        self.assertTrue(counter_check_due("2026-07-28", "2026-07-27", 20, 30))
        self.assertTrue(counter_check_due("2026-07-27", "2026-07-27", 30, 30))


class QuotaPanelTests(unittest.TestCase):
    def test_does_not_expand_while_existing_panel_is_backlogged(self):
        t = datetime(2026, 7, 29, 12, 0)
        self.assertEqual(
            quota_panel_target(
                t, {"K1": 100000, "K2": 100000}, 340, 31, 6, 475000,
                can_expand=False),
            340)

    def test_expands_only_to_existing_rate_capacity(self):
        t = datetime(2026, 7, 29, 23, 0)
        self.assertEqual(
            quota_panel_target(
                t, {"K1": 100000, "K2": 100000}, 340, 31, 6, 475000,
                can_expand=True),
            372)

    def test_never_reduces_base_panel_when_quota_is_ahead(self):
        t = datetime(2026, 7, 29, 12, 0)
        self.assertEqual(
            quota_panel_target(
                t, {"K1": 400000, "K2": 400000}, 340, 31, 6, 475000,
                can_expand=True),
            340)


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
