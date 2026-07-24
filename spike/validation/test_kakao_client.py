import unittest
from unittest.mock import patch

import kakao_client


class KakaoClientTests(unittest.TestCase):
    def test_search_place_normalizes_coordinates(self):
        payload = {
            "documents": [
                {
                    "id": "1",
                    "place_name": "테스트 정류장",
                    "x": "127.1",
                    "y": "37.4",
                    "address_name": "테스트 주소",
                }
            ]
        }
        with patch.object(kakao_client, "_get_json", return_value=payload):
            point = kakao_client.search_place("테스트", "secret")
        self.assertEqual(point["lon"], 127.1)
        self.assertEqual(point["lat"], 37.4)

    def test_extract_walk_collects_path_points(self):
        payload = {
            "status": "OK",
            "route": {
                "properties": {"totalDistance": 166, "totalTime": 130},
                "legs": [
                    {
                        "steps": [
                            {
                                "path": {
                                    "points": [
                                        [127.0, 37.0],
                                        [127.001, 37.001],
                                    ]
                                }
                            },
                            {
                                "path": {
                                    "points": [
                                        [127.001, 37.001],
                                        [127.002, 37.002],
                                    ]
                                }
                            },
                        ]
                    }
                ],
            },
        }
        walk = kakao_client.extract_walk(payload)
        self.assertEqual(walk["distance_m"], 166)
        self.assertEqual(walk["steps"], 2)
        self.assertEqual(len(walk["points"]), 3)

    def test_selects_vehicle_route_and_initial_boarding_point(self):
        payload = {
            "status": "OK",
            "routes": [
                {
                    "steps": [
                        {
                            "properties": {
                                "type": "SUBWAY",
                                "vehicles": [{"name": "2호선"}],
                            },
                            "path": {"points": [[127.0, 37.0]]},
                        }
                    ]
                },
                {
                    "steps": [
                        {
                            "properties": {
                                "type": "BUS",
                                "vehicles": [{"name": "6211"}],
                                "stops": [{"name": "신당누리센터"}],
                            },
                            "path": {
                                "points": [[127.014, 37.562], [127.02, 37.56]]
                            },
                        }
                    ]
                },
            ],
        }
        index, route = kakao_client.select_public_route(
            payload, "6211", "신당누리센터"
        )
        point = kakao_client.initial_boarding_point(route)
        self.assertEqual(index, 1)
        self.assertEqual(point["name"], "신당누리센터")
        self.assertEqual(point["lon"], 127.014)

    def test_rejects_same_vehicle_at_wrong_boarding_stop(self):
        payload = {
            "status": "OK",
            "routes": [
                {
                    "steps": [
                        {
                            "properties": {
                                "type": "BUS",
                                "vehicles": [{"name": "6211"}],
                                "stops": [{"name": "성동고등학교건너"}],
                            },
                            "path": {"points": [[127.0, 37.0]]},
                        }
                    ]
                }
            ],
        }
        with self.assertRaises(ValueError):
            kakao_client.select_public_route(
                payload, "6211", "신당누리센터"
            )


if __name__ == "__main__":
    unittest.main()
