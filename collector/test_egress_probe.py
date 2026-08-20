import unittest

from egress_probe import (
    PhysicalCalls, parse_plan, phase_group_limits, phase_valid, route_order_digest,
    shuffled_routes, source_mode,
)


class EgressProbeTests(unittest.TestCase):
    def test_plan_requires_even_dual_target(self):
        self.assertEqual(
            parse_plan("single-a:44,single-b:44,dual:52"),
            [("single-a", 44), ("single-b", 44), ("dual", 52)])
        with self.assertRaises(ValueError):
            parse_plan("dual:51")

    def test_single_mode_puts_all_keys_on_primary_source(self):
        keys = [("K1", "one"), ("K2", "two")]
        sources = {"one": "10.0.0.1", "two": "10.0.0.2"}
        groups = {"K1": "A", "K2": "B"}
        mapped, assigned = source_mode(keys, sources, groups, "single")
        self.assertEqual(mapped, {"one": "10.0.0.1", "two": "10.0.0.1"})
        self.assertEqual(assigned, {"K1": "single", "K2": "single"})

    def test_single_b_uses_other_egress_source(self):
        keys = [("K1", "one"), ("K2", "two")]
        sources = {"one": "10.0.0.1", "two": "10.0.0.2"}
        groups = {"K1": "GBIS_BUS_SOURCE_IP_A", "K2": "GBIS_BUS_SOURCE_IP_B"}
        mapped, assigned = source_mode(keys, sources, groups, "single-b")
        self.assertEqual(mapped, {"one": "10.0.0.2", "two": "10.0.0.2"})
        self.assertEqual(assigned, {"K1": "single", "K2": "single"})

    def test_only_dual_splits_target_between_egress_groups(self):
        self.assertEqual(phase_group_limits("single-a", 52, {"K": "A"}),
                         {"single": 52})
        self.assertEqual(phase_group_limits("dual", 52, {"K1": "A", "K2": "B"}),
                         {"A": 26, "B": 26})

    def test_phase_valid_requires_ninety_percent_occupancy(self):
        self.assertTrue(phase_valid(54, 60))
        self.assertFalse(phase_valid(53.9, 60))

    def test_physical_counter_never_exceeds_hard_cap(self):
        calls = PhysicalCalls(2)
        self.assertTrue(calls.allow())
        self.assertTrue(calls.allow())
        self.assertFalse(calls.allow())
        self.assertEqual(calls.value, 2)

    def test_route_shuffle_is_deterministic_full_panel_permutation(self):
        routes = [{"routeid": str(i), "cityCode": "1"} for i in range(20)]
        first = shuffled_routes(routes, 42)
        again = shuffled_routes(routes, 42)
        self.assertEqual(first, again)
        self.assertCountEqual(first, routes)
        self.assertNotEqual(first, routes)
        self.assertEqual(route_order_digest(first), route_order_digest(again))


if __name__ == "__main__":
    unittest.main()
