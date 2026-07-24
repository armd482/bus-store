import json
import os
from copy import deepcopy
import unittest

import score


class ScoreTests(unittest.TestCase):
    def test_expected_wait_matches_numeric_integration(self):
        cycle, green, dist, speed = 95, 20, 18, 1.45
        crossing = dist / speed
        step = 0.001
        phases = int(cycle / step)
        numeric = (
            sum(
                0 if i * step <= green - crossing else cycle - i * step
                for i in range(phases)
            )
            * step
            / cycle
        )
        self.assertAlmostEqual(
            score.expected_wait(green, cycle, dist, speed), numeric, places=2
        )

    def test_impossible_crossing_is_rejected(self):
        with self.assertRaises(ValueError):
            score.expected_wait(10, 90, 20, 1.0)

    def test_iso_datetimes_handle_midnight(self):
        arrival = score.parse_dt("2026-07-23T23:59:00")
        candidates = [("night_bus", score.parse_dt("2026-07-24T00:05:00"))]
        self.assertEqual(
            score.choose_connection(candidates, arrival, 60), "night_bus"
        )

    def test_buffer_boundary(self):
        arrival = score.parse_dt("2026-07-23T08:09:40")
        candidates = [("bus", score.parse_dt("2026-07-23T08:10:00"))]
        self.assertEqual(score.choose_connection(candidates, arrival, 20), "bus")
        self.assertIsNone(score.choose_connection(candidates, arrival, 21))

    def test_calibration_event_replays_clean_flip(self):
        here = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(here, "ground_truth.json"), encoding="utf-8") as f:
            sc = json.load(f)[0]
        p_naver = score.predict(sc, {}, "Naver", 60)
        p_product = score.predict(sc, {}, "Speed+Signal", 60)
        self.assertEqual(p_naver.connection, "6211_next")
        self.assertFalse(p_naver.connection_ok)
        self.assertEqual(p_product.connection, "6211_early")
        self.assertTrue(p_product.connection_ok)
        self.assertAlmostEqual(abs(p_product.arrival_error_s), 24.01, places=2)

    def test_tmap_personal_uses_supplied_physical_walk_time(self):
        here = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(here, "ground_truth.json"), encoding="utf-8") as f:
            sc = json.load(f)[0]
        prediction = score.predict(
            sc, {}, "TMAP-Personal", 60, tmap_walk_s=472.81
        )
        self.assertAlmostEqual(prediction.arrival_error_s, 0.0, places=2)

    def test_tmap_models_are_added_to_evaluation(self):
        here = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(here, "ground_truth.json"), encoding="utf-8") as f:
            sc = json.load(f)[0]
        rows, metrics = score.evaluate(
            [sc], {}, tmap_walk_times={sc["id"]: 472.81}
        )
        self.assertIn("TMAP-Personal", rows[0]["predictions"])
        self.assertIn("TMAP+Signal", metrics)
        self.assertAlmostEqual(metrics["TMAP-Personal"]["mae_s"], 0.0, places=2)

    def test_verdict_selects_tmap_product_when_all_test_events_have_it(self):
        here = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(here, "ground_truth.json"), encoding="utf-8") as f:
            calibration = json.load(f)[0]
        scenarios = []
        tmap_walk_times = {}
        for index in range(3):
            sc = deepcopy(calibration)
            sc["id"] = f"tmap-test-{index}"
            sc["split"] = "test"
            scenarios.append(sc)
            tmap_walk_times[sc["id"]] = 472.81
        result = score.verdict(
            scenarios, {}, 60, None, tmap_walk_times=tmap_walk_times
        )
        self.assertEqual(result["product_model"], "TMAP+Signal")
        self.assertEqual(result["status"], "pass")

    def test_calibration_is_not_counted_as_validation(self):
        here = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(here, "ground_truth.json"), encoding="utf-8") as f:
            scenarios = json.load(f)
        result = score.verdict(scenarios, {}, 60, None)
        self.assertEqual(result["status"], "insufficient")
        self.assertEqual(result["n"], 0)

    def test_three_new_events_can_pass_only_after_thresholds(self):
        here = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(here, "ground_truth.json"), encoding="utf-8") as f:
            calibration = json.load(f)[0]
        scenarios = []
        for index in range(3):
            sc = deepcopy(calibration)
            sc["id"] = f"test-{index}"
            sc["split"] = "test"
            scenarios.append(sc)
        result = score.verdict(scenarios, {}, 60, None)
        self.assertEqual(result["status"], "pass")
        self.assertEqual(result["n"], 3)
        self.assertEqual(result["uplift"], 3)


if __name__ == "__main__":
    unittest.main()
