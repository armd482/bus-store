import os
import tempfile
import unittest
from unittest.mock import Mock, patch

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

    def test_egress_sources_disabled_when_all_values_are_empty(self):
        config = {
            "busEgressSourceEnvByKey": {
                "GBIS_BUS_KEY": "SOURCE_A",
                "GBIS_BUS_KEY2": "SOURCE_B",
            }}
        with patch.object(bus_collector.E, "get", return_value=None):
            self.assertEqual(
                bus_collector.load_egress_sources(
                    [("GBIS_BUS_KEY", "key1"),
                     ("GBIS_BUS_KEY2", "key2")], config),
                ({}, {}))

    def test_partial_egress_source_configuration_fails_fast(self):
        config = {
            "busEgressSourceEnvByKey": {
                "GBIS_BUS_KEY": "SOURCE_A",
                "GBIS_BUS_KEY2": "SOURCE_B",
            }}
        values = {"SOURCE_A": "172.31.5.134"}
        with patch.object(
                bus_collector.E, "get", side_effect=values.get):
            with self.assertRaisesRegex(RuntimeError, "GBIS_BUS_KEY2"):
                bus_collector.load_egress_sources(
                    [("GBIS_BUS_KEY", "key1"),
                     ("GBIS_BUS_KEY2", "key2")], config)

    def test_egress_sources_are_bound_and_grouped_by_env_name(self):
        config = {
            "busEgressSourceEnvByKey": {
                "GBIS_BUS_KEY": "SOURCE_A",
                "GBIS_BUS_KEY2": "SOURCE_B",
            }}
        values = {
            "SOURCE_A": "172.31.5.134",
            "SOURCE_B": "172.31.1.218",
        }
        sock = Mock()
        with patch.object(
                bus_collector.E, "get", side_effect=values.get
        ), patch.object(
                bus_collector.socket, "socket", return_value=sock):
            sources, groups = bus_collector.load_egress_sources(
                [("GBIS_BUS_KEY", "key1"),
                 ("GBIS_BUS_KEY2", "key2")], config)
        self.assertEqual(
            sources,
            {"key1": "172.31.5.134", "key2": "172.31.1.218"})
        self.assertEqual(
            groups,
            {"GBIS_BUS_KEY": "SOURCE_A", "GBIS_BUS_KEY2": "SOURCE_B"})
        self.assertEqual(
            [call.args[0] for call in sock.bind.call_args_list],
            [("172.31.5.134", 0), ("172.31.1.218", 0)])


if __name__ == "__main__":
    unittest.main()
