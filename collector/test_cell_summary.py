import os
import tempfile
import unittest
from unittest.mock import patch

import orchestrator as O


class CellSummaryTests(unittest.TestCase):
    def test_summary_tracks_insert_update_day_change_and_delete(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "coverage.sqlite")
            with patch.object(O, "DB", db_path), \
                    patch.object(O, "DATA", tmp), \
                    patch.object(
                        O, "cfg",
                        return_value={"targetSamples": 7, "minDays": 2}):
                conn = O.connect()
                try:
                    O.bump(conn, "R1", 1, 2, 3, "wed", "2026-07-29")
                    O.bump(
                        conn, "R1", 1, 2, 3, "wed", "2026-07-29", k=6)
                    self.assertEqual(
                        conn.execute(
                            """SELECT cells,obs,done,fill_n,day_fill_n,
                                      sample_ready,date_ready
                               FROM cell_summary
                               WHERE daytype='wed' AND band=3""").fetchone(),
                        (1, 7, 0, 7, 1, 1, 0))

                    O.bump(conn, "R1", 1, 2, 3, "wed", "2026-08-05")
                    self.assertEqual(
                        conn.execute(
                            """SELECT cells,obs,done,fill_n,day_fill_n,
                                      sample_ready,date_ready
                               FROM cell_summary
                               WHERE daytype='wed' AND band=3""").fetchone(),
                        (1, 8, 1, 7, 2, 1, 1))

                    conn.execute(
                        """DELETE FROM cell WHERE routeid='R1' AND from_ord=1
                           AND to_ord=2 AND band=3 AND daytype='wed'""")
                    self.assertIsNone(conn.execute(
                        """SELECT 1 FROM cell_summary
                           WHERE daytype='wed' AND band=3""").fetchone())
                finally:
                    conn.close()


if __name__ == "__main__":
    unittest.main()
