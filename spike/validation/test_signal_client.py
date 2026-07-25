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
        # 설계 문서 §7.1·§7.4 step 2: red^2 / (2*cycle). cycle=90, green=30 → red=60.
        self.assertAlmostEqual(
            signal_client.expected_wait(90, 30, 15, 1.5),
            60 * 60 / 180,
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

    def test_derived_wait_from_crossing_length(self):
        # 43m 횡단, cycle 150: green = 7 + 43 = 50, red = 100, wait = 100^2/(2*150).
        self.assertAlmostEqual(signal_client.derived_wait(43, 150), 100 ** 2 / (2 * 150), places=4)
        cyc, green = signal_client.derived_timing(43, 150)
        self.assertEqual((cyc, green), (150, 50))

    def test_derived_total_sums_merged_signals(self):
        # Case 1 병합 3신호: 신당 43m, 판교 39m·42m.
        crossings = [{"distance_m": 43}, {"distance_m": 39}, {"distance_m": 42}]
        total = signal_client.fallback_derived_total(crossings, 150)
        self.assertAlmostEqual(total, sum(signal_client.derived_wait(d, 150)
                                          for d in (43, 39, 42)), places=4)

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

    def test_fallback_reports_each_crossing_without_clustering(self):
        crossings = [
            {"lon": 127.01555, "lat": 37.56299, "distance_m": 6},
            {"lon": 127.01523, "lat": 37.56304, "distance_m": 9},
        ]
        self.assertLess(
            signal_client.haversine_m(
                (crossings[0]["lon"], crossings[0]["lat"]),
                (crossings[1]["lon"], crossings[1]["lat"]),
            ),
            40,
        )
        result = signal_client.fallback_signal_sensitivity(crossings)
        self.assertEqual(result["raw_count"], 2)
        self.assertEqual(result["raw_waits_s"]["OTP15"], 30)
        self.assertEqual(result["raw_waits_s"]["Expected20"], 40)
        self.assertEqual(result["road_upper_s"], 120)
        self.assertEqual(
            [row["upper_s"] for row in result["crossing_upper_bounds"]],
            [60, 60],
        )

    def test_road_upper_scales_with_tmap_crossing_distance(self):
        crossings = [
            {"distance_m": 6},
            {"distance_m": 15},
            {"distance_m": 20},
            {"distance_m": 28},
            {"distance_m": 41},
        ]
        result = signal_client.fallback_signal_sensitivity(crossings)
        self.assertEqual(
            [row["upper_s"] for row in result["crossing_upper_bounds"]],
            [60, 60, 60, 150, 150],
        )
        self.assertEqual(result["road_upper_s"], 480)

    def test_route_uses_road_upper_for_each_crossing_when_signal_data_is_missing(self):
        crossings = [
            {
                "lon": 127.01555,
                "lat": 37.56299,
                "distance_m": 15,
                "offset_s": 440,
                "start": [127.01561, 37.56311],
                "end": [127.01555, 37.56299],
            },
            {
                "lon": 127.01523,
                "lat": 37.56304,
                "distance_m": 28,
                "offset_s": 460,
                "start": [127.01552, 37.56294],
                "end": [127.01523, 37.56304],
            },
        ]
        result = signal_client.estimate_route_wait(
            crossings,
            {},
            datetime(2026, 7, 24, 15, 22),
            True,
            1.66,
            [],
            [],
            datetime(2026, 7, 24, 15, 22),
        )
        self.assertEqual(result["wait_s"], 210)
        self.assertTrue(result["used_fallback"])
        self.assertEqual(result["fallback"]["raw_count"], 2)

    def test_route_without_crosswalk_has_zero_signal_wait_without_fallback(self):
        result = signal_client.estimate_route_wait(
            [],
            {},
            datetime(2026, 7, 24, 15, 22),
            True,
            1.66,
            [],
            [],
            datetime(2026, 7, 24, 15, 22),
        )
        self.assertEqual(result["wait_s"], 0)
        self.assertFalse(result["used_fallback"])

    def test_route_combines_expected_and_per_crossing_fallback(self):
        crossings = [
            {
                "lon": 127.0,
                "lat": 37.0,
                "distance_m": 15,
                "offset_s": 10,
                "start": [126.9999, 37.0],
                "end": [127.0, 37.0],
            },
            {
                "lon": 127.01,
                "lat": 37.01,
                "distance_m": 28,
                "offset_s": 30,
                "start": [127.0099, 37.01],
                "end": [127.01, 37.01],
            },
        ]
        intersections = [
            {
                "stdgCd": "11",
                "crsrdId": "1",
                "crsrdNm": "matched",
                "mapCtptIntLot": "127.0",
                "mapCtptIntLat": "37.0",
            }
        ]
        static = {
            "cw": {
                "stdg_cd": "11",
                "crsrd_id": "1",
                "direction": "et",
                "dist_m": 15,
                "cycle_s": 90,
                "green_s": 30,
            }
        }
        result = signal_client.estimate_route_wait(
            crossings,
            static,
            datetime(2026, 7, 24, 15, 22),
            False,
            1.5,
            intersections,
            [],
            datetime(2026, 7, 24, 15, 22),
        )
        expected = signal_client.expected_wait(90, 30, 15, 1.5)
        self.assertAlmostEqual(result["wait_s"], expected + 150)
        self.assertTrue(result["used_fallback"])
        self.assertEqual(result["fallback"]["raw_count"], 1)


if __name__ == "__main__":
    unittest.main()
