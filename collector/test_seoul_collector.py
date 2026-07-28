import json
import os
import tempfile
import unittest
from datetime import datetime
from unittest.mock import patch

import orchestrator as O
import seoul_collector as S


class SeoulStateRetentionTests(unittest.TestCase):
    def _write(self, path, value):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(value, f)

    def test_early_morning_keeps_calendar_calls_and_previous_service_day(self):
        # 7월 29일 02시는 쿼터상 7/29, 04시 기준 운행일은 7/28이다.
        t = datetime(2026, 7, 29, 2, 0, tzinfo=O.KST)
        with tempfile.TemporaryDirectory() as data_dir, patch.object(O, "DATA", data_dir):
            expected = {
                "seoul-calls-2026-07-29.txt",
                "seoul-done-2026-07-28.json",
                ".seoul-failures-2026-07-28.json",
            }
            stale = {
                "seoul-calls-2026-07-28.txt",
                "seoul-done-2026-07-27.json",
                "seoul-done-2026-07-29.json",
                ".seoul-failures-2026-07-27.json",
            }
            for name in expected | stale:
                self._write(os.path.join(data_dir, name), {})

            removed = set(S.cleanup_state_files(t))

            self.assertEqual(removed, stale)
            self.assertEqual(set(os.listdir(data_dir)), expected)

    def test_cleanup_is_safe_when_current_files_do_not_exist_yet(self):
        t = datetime(2026, 7, 29, 5, 0, tzinfo=O.KST)
        with tempfile.TemporaryDirectory() as data_dir, patch.object(O, "DATA", data_dir):
            stale = os.path.join(data_dir, "seoul-done-2026-07-28.json")
            self._write(stale, [])

            self.assertEqual(S.cleanup_state_files(t), [os.path.basename(stale)])
            self.assertEqual(os.listdir(data_dir), [])


if __name__ == "__main__":
    unittest.main()
