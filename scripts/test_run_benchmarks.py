from __future__ import annotations

import importlib.util
import json
import os
import sys
import tempfile
import unittest
import urllib.error
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

    def test_enrich_result_prefers_server_tps(self) -> None:
        enriched = rb.enrich_result(
            {"started_at": 0.0, "first_token_at": 10.0, "completed_at": 20.0},
            {"tokens_streamed": 11, "tokens_per_second": 7.5},
        )
        self.assertEqual(enriched["tps"], 7.5)
        self.assertEqual(enriched["tokens_streamed"], 11)

    def test_request_allocated_devices_reads_metric_ids(self) -> None:
        devices = rb.request_allocated_devices({"server_metric": {"allocated_node_ids": ["a", "b"]}})
        self.assertEqual(devices, ("a", "b"))

    def test_find_request_metric_finds_matching_request(self) -> None:
        metric = rb.find_request_metric({"requests": [{"client_request_id": "req-1", "tokens_streamed": 3}]}, "req-1")
        self.assertEqual(metric, {"client_request_id": "req-1", "tokens_streamed": 3})

    def test_matches_rerun_point(self) -> None:
        rerun_points = [{"model_label": "Qwen3-0.6B", "scenario": "android_only", "target": 2}]
        self.assertTrue(rb.matches_rerun_point("Qwen3-0.6B", "android_only", 2, rerun_points))
        self.assertFalse(rb.matches_rerun_point("Qwen3-0.6B", "android_only", 1, rerun_points))

    def test_select_model_sweep_plan_filters_to_requested_points(self) -> None:
        original = rb.connected_node_count
        rb.connected_node_count = lambda _: 3
        config = {"models": [{"label": "Qwen3-0.6B", "id": "a"}, {"label": "1.0B Llama 3.2", "id": "b"}], "targets": "auto", "scenarios": ["android_only"]}
        try:
            plan = rb.select_model_sweep_plan(
                "http://unused",
                config,
                [{"model_label": "1.0B Llama 3.2", "scenario": "android_only", "target": 2}],
            )
        finally:
            rb.connected_node_count = original
        self.assertEqual(plan, [("android_only", {"label": "1.0B Llama 3.2", "id": "b"}, 2)])

    def test_annotate_failure_marks_capacity_issue(self) -> None:
        text = rb.annotate_failure("HTTP Error 503: Service Unavailable: instance died during startup on rpc nodes [...]")
        self.assertIn("assumed insufficient RAM/capacity", text)

    def test_run_streaming_chat_request_surfaces_server_error(self) -> None:
        original = rb.urllib.request.urlopen

        def fake_urlopen(*args, **kwargs):
            raise urllib.error.HTTPError(
                url="http://unused",
                code=503,
                msg="Service Unavailable",
                hdrs=None,
                fp=io.BytesIO(json.dumps({"error": "instance died during startup"}).encode("utf-8")),
            )

        import io

        rb.urllib.request.urlopen = fake_urlopen
        try:
            with self.assertRaises(RuntimeError) as ctx:
                rb.run_streaming_chat_request("http://unused", "demo-model", "hello", 0.0, 1, "test")
            self.assertIn("instance died during startup", str(ctx.exception))
        finally:
            rb.urllib.request.urlopen = original

    def test_run_streaming_chat_request_surfaces_client_timeout(self) -> None:
        original = rb.urllib.request.urlopen
        rb.urllib.request.urlopen = lambda *args, **kwargs: (_ for _ in ()).throw(TimeoutError())
        try:
            with self.assertRaises(RuntimeError) as ctx:
                rb.run_streaming_chat_request("http://unused", "demo-model", "hello", 0.0, 1, "test")
            self.assertIn(f"client timed out after {rb.REQUEST_TIMEOUT_S}s", str(ctx.exception))
        finally:
            rb.urllib.request.urlopen = original

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
