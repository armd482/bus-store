import json
import os
import tempfile
import unittest
from datetime import datetime
from unittest.mock import patch

import orchestrator as O
import seoul_collector as S


class SeoulKeyTests(unittest.TestCase):
    def test_primary_key_keeps_existing_gbis_fallback(self):
        with patch.object(S.E, "get", side_effect=[None, "primary"]) as get:
            self.assertEqual(S._load_key("SEOUL_BUS_KEY"), "primary")
        self.assertEqual(
            get.call_args_list,
            [unittest.mock.call("SEOUL_BUS_KEY"),
             unittest.mock.call("GBIS_BUS_KEY", "DATA_GO_KR_KEY")],
        )

    def test_actual_key_values_are_deduplicated(self):
        values = {
            "SEOUL_BUS_KEY": "same",
            "GBIS_BUS_KEY2": "same",
            "GBIS_BUS_KEY3": "third",
        }
        with patch.object(
            S.O, "cfg", return_value={"seoulBusKeys": list(values)}
        ), patch.object(S, "_load_key", side_effect=values.get):
            self.assertEqual(
                S.load_keys(warn=False),
                [("SEOUL_BUS_KEY", "same"), ("GBIS_BUS_KEY3", "third")],
            )


class SeoulQuotaTests(unittest.TestCase):
    def test_legacy_total_is_migrated_to_primary_key(self):
        keys = [("SEOUL_BUS_KEY", "a"), ("GBIS_BUS_KEY2", "b")]
        with tempfile.TemporaryDirectory() as data_dir, patch.object(O, "DATA", data_dir):
            S._write_calls("2026-07-29", 123)
            S.ensure_key_counters(keys, "2026-07-29")
            self.assertEqual(S.read_calls("2026-07-29", "SEOUL_BUS_KEY"), 123)
            self.assertEqual(S.read_calls("2026-07-29", "GBIS_BUS_KEY2"), 0)

    def test_reservation_never_exceeds_per_key_cap(self):
        clock = lambda: datetime(2026, 7, 29, 12, 0, tzinfo=O.KST)
        with tempfile.TemporaryDirectory() as data_dir, patch.object(O, "DATA", data_dir):
            quota = S.QuotaReservations(2, clock=clock)
            self.assertTrue(quota.reserve("K1", 1))
            self.assertTrue(quota.reserve("K1", 1))
            self.assertFalse(quota.reserve("K1", 1))
            self.assertTrue(quota.reserve("K2", 1))
            self.assertEqual(S.read_calls("2026-07-29", "K1"), 2)
            self.assertEqual(S.read_calls("2026-07-29", "K2"), 1)
            self.assertEqual(S.read_calls("2026-07-29"), 3)

    def test_three_keys_keep_normal_calls_under_9300_budget(self):
        safe = S.minimum_safe_interval(702, 3, 9300, utilization=1)
        self.assertLess(safe, 2175)
        calls = 702 * 86400 / 2175 / 3
        self.assertLessEqual(calls, 9300)
        self.assertAlmostEqual(9900 - calls, 604.55, places=2)

    def test_missing_key_requires_longer_interval(self):
        safe = S.minimum_safe_interval(702, 2, 9300, utilization=1)
        self.assertGreater(safe, 2175)
        self.assertAlmostEqual(safe, 3260.90, places=2)


class SeoulStateRetentionTests(unittest.TestCase):
    def _write(self, path, value):
        with open(path, "w", encoding="utf-8") as file:
            json.dump(value, file)

    def test_keeps_current_calendar_aggregate_and_active_key_ledgers(self):
        t = datetime(2026, 7, 29, 2, 0, tzinfo=O.KST)
        keys = [("SEOUL_BUS_KEY", "a"), ("GBIS_BUS_KEY2", "b")]
        with tempfile.TemporaryDirectory() as data_dir, patch.object(O, "DATA", data_dir):
            expected = {
                "seoul-calls-2026-07-29.txt",
                "seoul-calls-SEOUL_BUS_KEY-2026-07-29.txt",
                "seoul-calls-GBIS_BUS_KEY2-2026-07-29.txt",
            }
            stale = {
                "seoul-calls-2026-07-28.txt",
                "seoul-calls-GBIS_BUS_KEY2-2026-07-28.txt",
                "seoul-calls-REMOVED_KEY-2026-07-29.txt",
                "seoul-done-2026-07-28.json",
                ".seoul-failures-2026-07-28.json",
            }
            for name in expected | stale:
                self._write(os.path.join(data_dir, name), {})

            removed = set(S.cleanup_state_files(t, keys))

            self.assertEqual(removed, stale)
            self.assertEqual(set(os.listdir(data_dir)), expected)

    def test_cleanup_is_safe_when_current_files_do_not_exist_yet(self):
        t = datetime(2026, 7, 29, 5, 0, tzinfo=O.KST)
        with tempfile.TemporaryDirectory() as data_dir, patch.object(O, "DATA", data_dir):
            stale = os.path.join(data_dir, "seoul-done-2026-07-28.json")
            self._write(stale, [])

            self.assertEqual(S.cleanup_state_files(t, []), [os.path.basename(stale)])
            self.assertEqual(os.listdir(data_dir), [])


if __name__ == "__main__":
    unittest.main()
