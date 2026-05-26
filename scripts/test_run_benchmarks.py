from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
os.environ.setdefault("MPLCONFIGDIR", "/private/tmp/mpl-test-cache")

import run_benchmarks as rb  # noqa: E402


class RunBenchmarksHelpersTest(unittest.TestCase):
    def test_load_prompt_presets_falls_back(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            self.assertIn("endless_creative_writing", rb.load_prompt_presets(Path(tmpdir) / "missing.json"))

    def test_resolve_sweep_targets_with_explicit_list(self) -> None:
        self.assertEqual(rb.resolve_sweep_targets("http://unused", [1, 2, 3]), [1, 2, 3])

    def test_resolve_sweep_targets_auto(self) -> None:
        original = rb.connected_node_count
        rb.connected_node_count = lambda _: 4
        try:
            self.assertEqual(rb.resolve_sweep_targets("http://unused", "auto"), [1, 2, 3, 4])
        finally:
            rb.connected_node_count = original

    def test_workload_only_has_503s(self) -> None:
        self.assertTrue(rb.workload_only_has_503s({"errors": ["HTTP Error 503: Service Unavailable"], "results": []}))
        self.assertFalse(rb.workload_only_has_503s({"errors": ["HTTP Error 400: Bad Request"], "results": []}))

    def test_detect_cluster_scenario(self) -> None:
        self.assertEqual(rb.detect_cluster_scenario([{"hardware_model": "iPhone 15"}]), "ios_only")
        self.assertEqual(rb.detect_cluster_scenario([{"hardware_model": "SM-G900V"}]), "android_only")
        self.assertEqual(rb.detect_cluster_scenario([{"hardware_model": "iPhone 15"}, {"hardware_model": "SM-G900V"}]), "heterogeneous")

    def test_render_line_plot_writes_png(self) -> None:
        if importlib.util.find_spec("matplotlib") is None:
            self.skipTest("matplotlib is not installed")
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "plot.png"
            rb.render_line_plot(path, "Demo", "X", "Y", {"series-a": [(1, 2.0), (2, 3.0)]})
            self.assertTrue(path.exists())
            self.assertGreater(path.stat().st_size, 0)

    def test_sweep_row_uses_workload_metric(self) -> None:
        row = rb.sweep_row(
            series_label="Qwen",
            model_id="model-id",
            target=3,
            scenario="heterogeneous",
            workload={"results": [{"request_id": "req-1", "ttft_s": 0.2, "tps": 12.0, "server_metric": {"allocated_node_count": 3, "allocated_node_ids": ["a", "b", "c"]}}]},
        )
        self.assertEqual(row["status"], "ok")
        self.assertEqual(row["loading_node_count"], 3)
        self.assertEqual(row["allocated_devices"], "a,b,c")


if __name__ == "__main__":
    unittest.main()
