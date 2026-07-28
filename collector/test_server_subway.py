import os
import tempfile
import unittest
from datetime import datetime
from unittest.mock import patch

import orchestrator as O
import server


class SubwayCollectionStatusTests(unittest.TestCase):
    def _touch(self, root, day, suffix=".jsonl.gz"):
        with open(os.path.join(root, f"subway-{day}{suffix}"), "wb"):
            pass

    def test_counts_only_completed_service_days_and_estimates_third_week(self):
        now = datetime(2026, 7, 28, 13, 0, tzinfo=O.KST)
        with tempfile.TemporaryDirectory() as data_dir, \
                tempfile.TemporaryDirectory() as export_dir, \
                patch.object(O, "DATA", data_dir), \
                patch.object(O, "cfg", return_value={
                    "subwayTarget": 3, "exportDir": export_dir}):
            for day in (
                    "2026-07-18", "2026-07-19", "2026-07-20", "2026-07-21",
                    "2026-07-22", "2026-07-23", "2026-07-24", "2026-07-25",
                    "2026-07-26", "2026-07-27"):
                self._touch(export_dir, day)
            # 어제 원본과 gzip이 함께 있어도 운행일은 한 번만 센다.
            self._touch(data_dir, "2026-07-27", ".jsonl")
            # 현재 운행일은 범위에는 보이지만 다음 04시 전까지 완료일에서 제외한다.
            self._touch(data_dir, "2026-07-28", ".jsonl")

            status = server.subway_collection_status(now)

        self.assertEqual(status["dataDayCount"], 11)
        self.assertEqual(status["completedDayCount"], 10)
        self.assertTrue(status["currentPartial"])
        self.assertEqual(status["completeDayCounts"], {
            "mon": 2, "tue": 1, "wed": 1, "thu": 1,
            "fri": 1, "sat": 2, "sun": 2,
        })
        self.assertAlmostEqual(status["progress"], 10 / 21)
        self.assertFalse(status["followupDone"])
        self.assertEqual(status["estimatedReadyServiceDay"], "2026-08-07")
        self.assertEqual(status["estimatedReadyAt"], "2026-08-08T04:00:00+09:00")

    def test_followup_is_done_after_three_complete_weeks(self):
        now = datetime(2026, 8, 8, 5, 0, tzinfo=O.KST)
        with tempfile.TemporaryDirectory() as data_dir, \
                patch.object(O, "DATA", data_dir), \
                patch.object(O, "cfg", return_value={
                    "subwayTarget": 3, "exportDir": None}):
            day = datetime(2026, 7, 18)
            while day.date().isoformat() <= "2026-08-07":
                self._touch(data_dir, day.date().isoformat())
                day += server.timedelta(days=1)

            status = server.subway_collection_status(now)

        self.assertTrue(status["followupDone"])
        self.assertEqual(status["progress"], 1.0)
        self.assertIsNone(status["estimatedReadyAt"])


if __name__ == "__main__":
    unittest.main()
