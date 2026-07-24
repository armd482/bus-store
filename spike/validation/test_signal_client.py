from datetime import datetime, timedelta
import unittest

import signal_client


class SignalClientTests(unittest.TestCase):
    def test_projected_green_requires_time_to_finish_crossing(self):
        # 녹색 5초가 남았지만 횡단에는 10초가 걸리므로 다음 주기를 기다린다.
        wait = signal_client.projected_wait(
            signal_client.GREEN_STATE,
            remaining_s=5,
            seconds_ahead=0,
            cycle_s=90,
            green_s=20,
            dist_m=10,
            speed_mps=1,
        )
        self.assertEqual(wait, 75)

    def test_projected_green_is_zero_when_crossing_can_finish(self):
        wait = signal_client.projected_wait(
            signal_client.GREEN_STATE,
            remaining_s=12,
            seconds_ahead=0,
            cycle_s=90,
            green_s=20,
            dist_m=10,
            speed_mps=1,
        )
        self.assertEqual(wait, 0)

    def test_expected_wait_matches_design_formula(self):
        self.assertAlmostEqual(
            signal_client.expected_wait(90, 30, 15, 1.5),
            70 * 70 / 180,
        )

    def test_infers_cycle_and_green_from_polled_transitions(self):
        start = datetime(2026, 7, 24, 8, 0, 0)
        samples = [
            (start, signal_client.RED_STATE),
            (start + timedelta(seconds=10), signal_client.GREEN_STATE),
            (start + timedelta(seconds=30), signal_client.RED_STATE),
            (start + timedelta(seconds=100), signal_client.GREEN_STATE),
            (start + timedelta(seconds=120), signal_client.RED_STATE),
            (start + timedelta(seconds=190), signal_client.GREEN_STATE),
        ]
        timing = signal_client.infer_timing(samples)
        self.assertEqual(timing["cycle_s"], 90)
        self.assertEqual(timing["green_s"], 20)

    def test_rejects_intersection_far_from_tmap_crossing(self):
        crossing = {"lon": 127.0, "lat": 37.0}
        intersections = [
            {"mapCtptIntLot": "127.01", "mapCtptIntLat": "37.01"}
        ]
        match, distance = signal_client.nearest_intersection(
            crossing, intersections, max_distance_m=50
        )
        self.assertIsNone(match)
        self.assertGreater(distance, 50)


if __name__ == "__main__":
    unittest.main()
