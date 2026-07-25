import unittest
from unittest.mock import patch

import stop_client


TAGO_OK = {
    "response": {
        "header": {"resultCode": "00", "resultMsg": "NORMAL SERVICE."},
        "body": {
            "items": {
                "item": {
                    "nodeid": "GGB204000388",
                    "nodenm": "백현마을1단지",
                    "nodeno": "7488",
                    "gpslati": "37.3919667",
                    "gpslong": "127.1145",
                }
            }
        },
    }
}


class StopClientTests(unittest.TestCase):
    def test_resolve_stop_passthrough_direct_coordinate(self):
        # lon/lat 직접 지정이면 API 를 부르지 않고 그대로 쓴다.
        point = {"lon": 127.1145, "lat": 37.3919667, "name": "백현마을1단지"}
        with patch.object(stop_client, "_request_json", side_effect=AssertionError):
            resolved = stop_client.resolve_stop(point)
        self.assertEqual(resolved["lon"], 127.1145)
        self.assertEqual(resolved["lat"], 37.3919667)

    def test_resolve_stop_fetches_by_ars(self):
        # 좌표가 없고 stop{city_code,ars}면 TAGO 역조회로 실측 좌표를 가져온다.
        point = {"stop": {"city_code": 31020, "ars": "07488"}}
        with patch.object(stop_client, "_request_json", return_value=TAGO_OK) as m:
            resolved = stop_client.resolve_stop(point, key="k")
        self.assertEqual(resolved["lon"], 127.1145)
        self.assertEqual(resolved["lat"], 37.3919667)
        self.assertEqual(resolved["name"], "백현마을1단지")
        self.assertEqual(resolved["nodeid"], "GGB204000388")
        # 앞자리 0 은 떼서 nodeno=7488 로 조회한다.
        self.assertIn("nodeno=7488", m.call_args[0][0])

    def test_fetch_raises_on_result_code_error(self):
        bad = {"response": {"header": {"resultCode": "99", "resultMsg": "LIMIT"}, "body": {}}}
        with patch.object(stop_client, "_request_json", return_value=bad):
            with self.assertRaises(RuntimeError):
                stop_client.fetch_stop_by_ars(31020, "07488", key="k")

    def test_fetch_raises_when_stop_not_found(self):
        empty = {"response": {"header": {"resultCode": "00"}, "body": {"items": ""}}}
        with patch.object(stop_client, "_request_json", return_value=empty):
            with self.assertRaises(ValueError):
                stop_client.fetch_stop_by_ars(31020, "99999", key="k")

    def test_resolve_stop_requires_coordinate_or_id(self):
        with self.assertRaises(ValueError):
            stop_client.resolve_stop({"name": "정류장"})


if __name__ == "__main__":
    unittest.main()
