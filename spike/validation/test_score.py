import json
import os
from copy import deepcopy
import unittest

import score


class ScoreTests(unittest.TestCase):
    def test_expected_wait_matches_numeric_integration(self):
        # 설계 문서 §7.1·§7.4: 기대대기 = (cycle − effective_green)² / (2*cycle),
        # effective_green = green − 횡단시간. 유효 녹색 안에 도착해야만 대기 0.
        cycle, green, dist, speed = 95, 20, 18, 1.45
        eff_green = green - dist / speed          # = 7.586s
        step = 0.001
        phases = int(cycle / step)
        numeric = (
            sum(
                0 if i * step <= eff_green else cycle - i * step
                for i in range(phases)
            )
            * step
            / cycle
        )
        self.assertAlmostEqual(
            score.expected_wait(green, cycle, dist, speed), numeric, places=2
        )
        # 속도 개인화가 실제로 반영되는지 — 빠를수록 대기가 줄어야 한다(옛 공식은 불변).
        self.assertLess(
            score.expected_wait(green, cycle, dist, 2.0),
            score.expected_wait(green, cycle, dist, 1.0),
        )

    def test_impossible_crossing_is_rejected(self):
        with self.assertRaises(ValueError):
            score.expected_wait(10, 90, 20, 1.0)

    def test_cliff_false_excludes_signal_from_connection_gate(self):
        # 배차버스(cliff=false): 신호 과대예측이 연결을 뒤집지 않는다.
        sc = {
            "id": "cliff-test",
            "walk_start": "2026-07-24T16:07:58",
            "profile": {"walk_time_ratio": 0.68, "speed_mps": 1.66},
            "naver": {"walk_time_s": 300},
            "bus_snapshot": {
                "captured_at": "2026-07-24T16:08:05",
                "candidates": [{"id": "55", "eta_s": 480}],
            },
            "actual": {
                "stop_arrival": "2026-07-24T16:12:50",
                "connection_departures": [
                    {"id": "55", "departed_at": "2026-07-24T16:15:10"}
                ],
                "boarded": "55",
            },
        }
        # 신호 420s → Speed+Signal 도착 16:18:22. 55는 16:16:05 도착.
        p_cliff = score.predict(
            dict(sc, cliff=True), {}, "Speed+Signal", 60, signal_wait_s=420.0
        )
        p_headway = score.predict(
            dict(sc, cliff=False), {}, "Speed+Signal", 60, signal_wait_s=420.0
        )
        # 절벽이면 신호가 게이트에 들어가 55를 놓친다(None).
        self.assertIsNone(p_cliff.connection)
        # 배차버스면 순수 이동 도착으로 판정해 55를 잡는다.
        self.assertEqual(p_headway.connection, "55")
        # 도착시각(MAE)에는 두 경우 모두 신호가 그대로 반영된다.
        self.assertAlmostEqual(
            p_cliff.arrival_error_s, p_headway.arrival_error_s, places=2
        )
        self.assertAlmostEqual(p_headway.arrival_error_s, 331.0, delta=1.0)

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
        # 파일의 test 표본 수와 무관하게, calibration 표본만 남기면
        # 통과 판정 대상(n)은 0이어야 한다.
        here = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(here, "ground_truth.json"), encoding="utf-8") as f:
            scenarios = json.load(f)
        calibration_only = [s for s in scenarios if s.get("split") == "calibration"]
        self.assertTrue(calibration_only, "calibration 표본이 최소 1건 있어야 한다")
        result = score.verdict(calibration_only, {}, 60, None)
        self.assertEqual(result["status"], "insufficient")
        self.assertEqual(result["n"], 0)

    def test_map_outcome_flags_different_bus(self):
        # 지도가 6211_next 를 명시했지만 실제 가장 이른 연결은 6211_early → '다른 버스'.
        here = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(here, "ground_truth.json"), encoding="utf-8") as f:
            sc = json.load(f)[0]
        self.assertEqual(score.classify_map_outcome(sc, 60), ["different_bus"])

    def test_map_outcome_empty_when_map_matches_reality(self):
        # 지도가 실제 가장 이른 연결을 그대로 안내하면 부정확 유형이 없다.
        here = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(here, "ground_truth.json"), encoding="utf-8") as f:
            sc = deepcopy(json.load(f)[0])
        sc["naver"]["selected_connection"] = "6211_early"
        self.assertEqual(score.classify_map_outcome(sc, 60), [])

    def test_conservative_signal_passes_when_connection_correct(self):
        # ★ 핵심 회귀 방지: 신호를 보수적으로 과대예측해 도착 MAE 가 네이버보다
        # 나빠져도(mae_improvement<0.30), 연결 안내가 맞고 위험오답이 0이면 통과한다.
        # 구 게이트(mae_improvement>=0.30)는 정확히 이 경우를 잘못 탈락시켰다.
        here = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(here, "ground_truth.json"), encoding="utf-8") as f:
            calibration = json.load(f)[0]  # cliff=false 배차버스
        scenarios = []
        signal_estimates = {}
        for index in range(3):
            sc = deepcopy(calibration)
            sc["id"] = f"conservative-signal-{index}"
            sc["split"] = "test"
            scenarios.append(sc)
            signal_estimates[sc["id"]] = 300.0  # 실제(3.54s)보다 크게 과대예측
        result = score.verdict(
            scenarios, {}, 60, None, signal_estimates=signal_estimates
        )
        self.assertEqual(result["product_model"], "Speed+Signal")
        self.assertLess(result["mae_improvement"], 0.30)  # 구 게이트라면 탈락
        self.assertEqual(result["dangerous"], 0)
        self.assertEqual(result["status"], "pass")  # 새 게이트는 통과
        self.assertEqual(result["map_wrong"], 3)
        self.assertEqual(result["corrected"], 3)

    def test_signal_estimates_dict_makes_source_variants(self):
        # 보수·유도를 dict 로 주면 소스별 모델(…(보수)·…(유도))이 만들어지고,
        # 제품 모델은 유도(더 정확한 평균)를 우선한다. cliff=false 라 연결은 동일.
        here = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(here, "ground_truth.json"), encoding="utf-8") as f:
            cal = json.load(f)[0]  # cliff=false 배차버스
        scenarios, sig = [], {}
        for i in range(3):
            sc = deepcopy(cal)
            sc["id"] = f"src-{i}"
            sc["split"] = "test"
            scenarios.append(sc)
            sig[sc["id"]] = {"보수": 300.0, "유도": 100.0}
        rows, metrics = score.evaluate(scenarios, {}, signal_estimates=sig)
        self.assertIn("Speed+Signal(보수)", rows[0]["predictions"])
        self.assertIn("Speed+Signal(유도)", rows[0]["predictions"])
        # 두 소스의 도착 MAE 는 다르다(보수이 더 과대 → MAE 큼).
        self.assertGreater(metrics["Speed+Signal(보수)"]["mae_s"],
                           metrics["Speed+Signal(유도)"]["mae_s"])
        result = score.verdict(scenarios, {}, 60, None, signal_estimates=sig)
        self.assertEqual(result["product_model"], "Speed+Signal(유도)")
        self.assertEqual(result["status"], "pass")

    def test_cliff_product_conservative_bias_on_demo(self):
        # 빠듯한 절벽(지하철): 유도(과소예측 60s)는 T1 을 '탄다'고 위험오답,
        # 보수(150s)는 '놓친다'고 T2 적중. 맥락별 제품은 절벽에서 보수를 골라
        # 위험오답 0 을 지킨다. (목적: 어느 모델이 이기나가 아니라, 접근이 지도보다
        # 안전하게 나은가.)
        # ⚠ 이 케이스는 보수(150s) > 실제(130s) 로 설정한 **데모**다. 보수가 항상
        #   실제를 덮는다는 보장은 아니며(3관측 경험치), 실측 절벽 이벤트로 검증되기
        #   전까지 '보수 편향'을 확인하는 용도로만 읽는다(score.py verdict 주석 참조).
        base = {
            "cliff": True,
            "walk_start": "2026-07-25T08:00:00",
            "profile": {"walk_time_ratio": 0.68, "speed_mps": 1.66},
            "naver": {"walk_time_s": 180, "selected_connection": "T1"},
            "bus_snapshot": {"captured_at": "2026-07-25T08:00:00",
                             "candidates": [{"id": "T1", "eta_s": 250},
                                            {"id": "T2", "eta_s": 430}]},
            "actual": {"stop_arrival": "2026-07-25T08:04:15", "signal_wait_s": 130,
                       "connection_departures": [
                           {"id": "T1", "departed_at": "2026-07-25T08:04:10"},
                           {"id": "T2", "departed_at": "2026-07-25T08:07:10"}],
                       "boarded": "T2"},
        }
        scenarios, sig = [], {}
        for i in range(3):
            sc = deepcopy(base)
            sc["id"] = f"cliff-{i}"
            sc["split"] = "test"
            scenarios.append(sc)
            sig[sc["id"]] = {"보수": 150.0, "유도": 60.0}
        _, metrics = score.evaluate(scenarios, {}, signal_estimates=sig)
        # 유도는 위험오답, 보수는 (이 데모에서) 안전 — 절벽에서 둘이 갈린다.
        self.assertGreater(metrics["Speed+Signal(유도)"]["dangerous"], 0)
        self.assertEqual(metrics["Speed+Signal(보수)"]["dangerous"], 0)
        result = score.verdict(scenarios, {}, 60, None, signal_estimates=sig)
        # 맥락별 제품이 절벽에서 보수를 골라 안전하게 통과한다.
        self.assertEqual(result["product_model"], "Speed+Signal(보수)")
        self.assertEqual(result["dangerous"], 0)
        self.assertEqual(result["status"], "pass")

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
