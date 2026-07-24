import unittest

import tmap_client


class TmapClientTests(unittest.TestCase):
    def test_extracts_only_linestring_segments(self):
        payload = {
            "features": [
                {
                    "geometry": {"type": "Point", "coordinates": [127.0, 37.0]},
                    "properties": {"totalDistance": 120},
                },
                {
                    "geometry": {
                        "type": "LineString",
                        "coordinates": [[127.0, 37.0], [127.001, 37.0]],
                    },
                    "properties": {
                        "index": 1,
                        "distance": 100,
                        "facilityType": "17",
                    },
                },
                {
                    "geometry": {
                        "type": "LineString",
                        "coordinates": [[127.001, 37.0], [127.0012, 37.0]],
                    },
                    "properties": {"index": 2, "distance": 20},
                },
            ]
        }
        self.assertEqual(
            tmap_client.extract_segments(payload),
            [
                {"distance_m": 100.0, "facility_type": 17, "feature_index": 1},
                {"distance_m": 20.0, "facility_type": None, "feature_index": 2},
            ],
        )

    def test_personal_time_applies_facility_multiplier(self):
        segments = [
            {"distance_m": 100, "facility_type": None},
            {"distance_m": 17.5, "facility_type": 17},
        ]
        # 100/1.0 + 17.5/(1.0*0.875) = 120초
        self.assertAlmostEqual(
            tmap_client.personal_walk_seconds(segments, 1.0), 120.0
        )

    def test_extracts_crosswalk_location_and_offset(self):
        payload = {
            "features": [
                {
                    "geometry": {
                        "type": "LineString",
                        "coordinates": [[127.0, 37.0], [127.0001, 37.0]],
                    },
                    "properties": {
                        "index": 1,
                        "distance": 20,
                        "facilityType": "11",
                    },
                },
                {
                    "geometry": {
                        "type": "LineString",
                        "coordinates": [
                            [127.0001, 37.0],
                            [127.0002, 37.0],
                            [127.0003, 37.0],
                        ],
                    },
                    "properties": {
                        "index": 2,
                        "distance": 10,
                        "facilityType": "15",
                    },
                },
            ]
        }
        rows = tmap_client.extract_crosswalks(payload, speed_mps=2)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["offset_s"], 10)
        self.assertEqual(rows[0]["lon"], 127.0002)


if __name__ == "__main__":
    unittest.main()
