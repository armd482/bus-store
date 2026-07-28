import subprocess
import tempfile
import threading
import time
import unittest
from contextlib import nullcontext
from unittest.mock import patch

import orchestrator as O


class RcloneBackupTests(unittest.TestCase):
    def test_timeout_retries_then_succeeds(self):
        calls = [
            subprocess.TimeoutExpired(["rclone", "copy"], 600),
            subprocess.CompletedProcess(
                ["rclone", "copy"], returncode=0, stdout="ok\n", stderr=""),
        ]
        with patch.object(O, "_rclone_serialized", return_value=nullcontext()), \
                patch("subprocess.run", side_effect=calls) as run, \
                patch.object(O.time, "sleep") as sleep:
            ok, out = O._rclone(
                ["copy", "local.gz", "gdrive:busdata"], attempts=3, timeout=600)

        self.assertTrue(ok)
        self.assertEqual(out, "ok\n")
        self.assertEqual(run.call_count, 2)
        sleep.assert_called_once_with(15)

    def test_process_lock_serializes_concurrent_commands(self):
        active = 0
        max_active = 0
        state_lock = threading.Lock()

        def fake_run(*_args, **_kwargs):
            nonlocal active, max_active
            with state_lock:
                active += 1
                max_active = max(max_active, active)
            time.sleep(0.05)
            with state_lock:
                active -= 1
            return subprocess.CompletedProcess(
                ["rclone", "lsf"], returncode=0, stdout="", stderr="")

        with tempfile.TemporaryDirectory() as data_dir, \
                patch.object(O, "DATA", data_dir), \
                patch("subprocess.run", side_effect=fake_run):
            threads = [
                threading.Thread(target=O._rclone, args=(["lsf", "remote:"],))
                for _ in range(3)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

        self.assertEqual(max_active, 1)


if __name__ == "__main__":
    unittest.main()
