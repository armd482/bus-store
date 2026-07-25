import unittest
from datetime import datetime

import tmap_journey_client as journey


class TmapJourneyClientTest(unittest.TestCase):
    def test_selects_itinerary_by_route_sequence(self):
        payload = {
            "metaData": {
                "plan": {
                    "itineraries": [
                        {"legs": [{"mode": "BUS", "route": "간선:241"}]},
                        {
                            "legs": [
                                {"mode": "BUS", "route": "지선:6211"},
                                {"mode": "BUS", "route": "광역:9401"},
                            ]
                        },
                    ]
                }
            }
        }
        index, selected = journey.select_itinerary(payload, ["6211", "9401"])
        self.assertEqual(index, 1)
        self.assertEqual(journey.transit_route_names(selected), ["지선:6211", "광역:9401"])

    def test_connects_walk_signal_transit_and_wait(self):
        itinerary = {
            "legs": [
                {
                    "mode": "WALK",
                    "distance": 100,
                    "start": {"lon": 127.0, "lat": 37.0, "name": "A"},
                    "end": {"lon": 127.001, "lat": 37.0, "name": "B"},
                },
                {
                    "mode": "BUS",
                    "route": "지선:6211",
                    "routeId": "r1",
                    "sectionTime": 300,
                },
            ]
        }
        pedestrian = {
            "features": [
                {
                    "geometry": {
                        "type": "LineString",
                        "coordinates": [[127.0, 37.0], [127.001, 37.0]],
                    },
                    "properties": {"distance": 100, "facilityType": 11, "index": 0},
                }
            ]
        }

        result = journey.evaluate_itinerary(
            itinerary,
            datetime(2026, 7, 24, 15, 22),
            2.0,
            "key",
            [],
            [],
            datetime(2026, 7, 24, 15, 22),
            transit_waits_s={"r1": 30},
            pedestrian_fetcher=lambda config, key: pedestrian,
        )
        self.assertTrue(result["complete"])
        self.assertAlmostEqual(result["walk_s"], 50)
        self.assertAlmostEqual(result["signal_s"], 0)
        self.assertAlmostEqual(result["transit_s"], 300)
        self.assertAlmostEqual(result["transit_wait_s"], 30)
        self.assertAlmostEqual(result["total_s"], 380)

    def test_missing_transit_wait_marks_journey_incomplete(self):
        itinerary = {
            "legs": [
                {
                    "mode": "BUS",
                    "route": "간선:241",
                    "routeId": "r1",
                    "sectionTime": 300,
                }
            ]
        }
        result = journey.evaluate_itinerary(
            itinerary,
            datetime(2026, 7, 24, 15, 22),
            1.66,
            "key",
            [],
            [],
            datetime(2026, 7, 24, 15, 22),
        )
        self.assertFalse(result["complete"])
        self.assertEqual(result["missing_wait_legs"], [0])
        self.assertEqual(result["total_s"], 300)


if __name__ == "__main__":
    unittest.main()
