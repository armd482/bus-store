import unittest
from unittest.mock import patch

import server


class LiveSnapshotTests(unittest.TestCase):
    def test_returns_current_state_without_sharing_nested_containers(self):
        state = {
            "started": 10.0,
            "lastObs": 90.0,
            "fetching": True,
            "errors": {"timeout": 2},
            "errLog": [{"reason": "timeout"}],
        }
        with patch.dict(server.STATE, state, clear=True), \
                patch.object(server.time, "time", return_value=100.0):
            result = server.live_snapshot()

            self.assertEqual(result["now"], 100.0)
            self.assertEqual(result["state"]["lastObs"], 90.0)
            self.assertTrue(result["state"]["fetching"])

            result["state"]["errors"]["timeout"] = 99
            result["state"]["errLog"].append({"reason": "code99"})
            self.assertEqual(server.STATE["errors"]["timeout"], 2)
            self.assertEqual(len(server.STATE["errLog"]), 1)


if __name__ == "__main__":
    unittest.main()
