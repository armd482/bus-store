import os
import tempfile
import unittest
from unittest.mock import patch

from collector import env_config
import bus_collector


class EnvConfigTests(unittest.TestCase):
    def _files(self, local_text, root_text):
        temp = tempfile.TemporaryDirectory()
        local = os.path.join(temp.name, "collector.env")
        root = os.path.join(temp.name, "root.env")
        with open(local, "w", encoding="utf-8") as f:
            f.write(local_text)
        with open(root, "w", encoding="utf-8") as f:
            f.write(root_text)
        self.addCleanup(temp.cleanup)
        return local, root

    def test_component_value_overrides_root_value(self):
        files = self._files("GBIS_BUS_KEY=local\n", "GBIS_BUS_KEY=root\n")
        with patch.object(
            env_config, "COMPONENT_ENV_FILE", files[0]
        ), patch.object(
            env_config, "COMMON_ENV_FILES", (files[1],)
        ), patch.dict(os.environ, {}, clear=True):
            self.assertEqual(env_config.get("GBIS_BUS_KEY"), "local")

    def test_common_root_key_is_fallback(self):
        files = self._files("", "DATA_GO_KR_KEY=common\n")
        with patch.object(
            env_config, "COMPONENT_ENV_FILE", files[0]
        ), patch.object(
            env_config, "COMMON_ENV_FILES", (files[1],)
        ), patch.dict(os.environ, {}, clear=True):
            self.assertEqual(
                env_config.get("GBIS_BUS_KEY", "DATA_GO_KR_KEY"), "common"
            )

    def test_operating_system_environment_has_highest_priority(self):
        files = self._files("GBIS_BUS_KEY=local\n", "DATA_GO_KR_KEY=root\n")
        with patch.object(
            env_config, "COMPONENT_ENV_FILE", files[0]
        ), patch.object(
            env_config, "COMMON_ENV_FILES", (files[1],)
        ), patch.dict(os.environ, {"DATA_GO_KR_KEY": "process"}, clear=True):
            self.assertEqual(
                env_config.get("GBIS_BUS_KEY", "DATA_GO_KR_KEY"), "process"
            )

    def test_bus_primary_key_uses_common_fallback(self):
        with patch.object(bus_collector.E, "get", return_value="key") as get:
            self.assertEqual(bus_collector._load_key("GBIS_BUS_KEY"), "key")
            get.assert_called_once_with("GBIS_BUS_KEY", "DATA_GO_KR_KEY")

    def test_bus_secondary_key_does_not_duplicate_common_key(self):
        with patch.object(bus_collector.E, "get", return_value=None) as get:
            self.assertIsNone(bus_collector._load_key("GBIS_BUS_KEY2"))
            get.assert_called_once_with("GBIS_BUS_KEY2", None)

    def test_duplicate_actual_bus_key_is_activated_once(self):
        values = {"GBIS_BUS_KEY": "first", "GBIS_BUS_KEY2": "first",
                  "GBIS_BUS_KEY3": "third"}
        with patch.object(
            bus_collector.O, "cfg",
            return_value={"busKeys": list(values)}
        ), patch.object(
            bus_collector, "_load_key", side_effect=values.get
        ), patch("builtins.print") as log:
            self.assertEqual(
                bus_collector.load_keys(),
                [("GBIS_BUS_KEY", "first"), ("GBIS_BUS_KEY3", "third")],
            )
        log.assert_called_once()
        self.assertIn("중복 제외", log.call_args.args[0])

    def test_missing_configured_bus_key_is_skipped(self):
        values = {"GBIS_BUS_KEY": "first", "GBIS_BUS_KEY2": None,
                  "GBIS_BUS_KEY3": "third"}
        with patch.object(
            bus_collector.O, "cfg",
            return_value={"busKeys": list(values)}
        ), patch.object(bus_collector, "_load_key", side_effect=values.get):
            self.assertEqual(
                bus_collector.load_keys(),
                [("GBIS_BUS_KEY", "first"), ("GBIS_BUS_KEY3", "third")],
            )


if __name__ == "__main__":
    unittest.main()
