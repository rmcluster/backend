from __future__ import annotations

import importlib.util
import json
import os
import sys
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest import mock


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
os.environ.setdefault("MPLCONFIGDIR", "/private/tmp/mpl-test-cache")

import run_benchmarks as rb  # noqa: E402


class RunBenchmarksHelpersTest(unittest.TestCase):
    def test_default_prompt_presets_include_creative_writing(self) -> None:
        self.assertIn("creative_writing", rb.DEFAULT_PROMPT_PRESETS)

    def test_arg_parser_accepts_scenario_and_base_url(self) -> None:
        args = rb.build_arg_parser().parse_args(["--scenario", "android_only", "--base-url", "http://10.0.0.1:4917"])
        self.assertEqual(args.scenario, "android_only")
        self.assertEqual(args.base_url, "http://10.0.0.1:4917")

    def test_arg_parser_requires_scenario(self) -> None:
        with self.assertRaises(SystemExit):
            rb.build_arg_parser().parse_args([])

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

    def test_canonicalize_allocated_devices_uses_node_ids(self) -> None:
        canonical = rb.canonicalize_allocated_devices(
            ("192.168.0.10:4001", "192.168.0.11:4002"),
            [
                {"id": "node-a", "ip": "192.168.0.10", "port": 4001},
                {"id": "node-b", "ip": "192.168.0.11", "port": 4002},
            ],
        )
        self.assertEqual(canonical, ("node-a", "node-b"))

    def test_canonicalize_allocated_devices_falls_back_to_unique_ip(self) -> None:
        canonical = rb.canonicalize_allocated_devices(
            ("192.168.0.10:4999", "192.168.0.11:4002"),
            [
                {"id": "node-a", "ip": "192.168.0.10", "port": 4001},
                {"id": "node-b", "ip": "192.168.0.11", "port": 4002},
            ],
        )
        self.assertEqual(canonical, ("node-a", "node-b"))

    def test_allocated_device_sets_match_when_ports_change(self) -> None:
        matches = rb.allocated_device_sets_match(
            {"server_metric": {"allocated_node_ids": ["192.168.0.10:4001", "192.168.0.11:4002"]}},
            {"server_metric": {"allocated_node_ids": ["192.168.0.10:4999", "192.168.0.11:4002"]}},
            [
                {"id": "node-a", "ip": "192.168.0.10", "port": 4999},
                {"id": "node-b", "ip": "192.168.0.11", "port": 4002},
            ],
        )
        self.assertTrue(matches)

    def test_find_request_metric_finds_matching_request(self) -> None:
        metric = rb.find_request_metric({"requests": [{"client_request_id": "req-1", "tokens_streamed": 3}]}, "req-1")
        self.assertEqual(metric, {"client_request_id": "req-1", "tokens_streamed": 3})

    def test_scenario_run_dir_uses_persistent_folder(self) -> None:
        root = Path("/tmp/benchmarks/results")
        self.assertEqual(rb.scenario_run_dir(root, "heterogeneous"), root / "heterogeneous")

    def test_select_model_sweep_plan_uses_single_configured_scenario(self) -> None:
        original = rb.connected_node_count
        rb.connected_node_count = lambda _: 3
        config = {"models": [{"label": "Qwen3-0.6B", "id": "a"}, {"label": "1.0B Llama 3.2", "id": "b"}], "targets": "auto", "scenarios": ["android_only"]}
        try:
            plan = rb.select_model_sweep_plan("http://unused", config)
        finally:
            rb.connected_node_count = original
        self.assertEqual(
            plan,
            [
                ("android_only", {"label": "Qwen3-0.6B", "id": "a"}, 1),
                ("android_only", {"label": "Qwen3-0.6B", "id": "a"}, 2),
                ("android_only", {"label": "Qwen3-0.6B", "id": "a"}, 3),
                ("android_only", {"label": "1.0B Llama 3.2", "id": "b"}, 1),
                ("android_only", {"label": "1.0B Llama 3.2", "id": "b"}, 2),
                ("android_only", {"label": "1.0B Llama 3.2", "id": "b"}, 3),
            ],
        )

    def test_resolve_incomplete_model_sweep_plan_returns_all_points_when_empty(self) -> None:
        config = {"models": [{"label": "Qwen3-0.6B", "id": "a"}], "targets": [1, 2], "scenarios": ["android_only"]}
        plan, progress = rb.resolve_incomplete_model_sweep_plan("http://unused", config, [], [])
        self.assertEqual(
            plan,
            [
                ("android_only", {"label": "Qwen3-0.6B", "id": "a"}, 1),
                ("android_only", {"label": "Qwen3-0.6B", "id": "a"}, 2),
            ],
        )
        self.assertEqual(progress, {"expected_points": 2, "completed_points": 0, "pending_points": 2, "exhausted_points": 0})

    def test_resolve_incomplete_model_sweep_plan_skips_ok_and_retries_error_or_missing(self) -> None:
        config = {
            "models": [{"label": "Qwen3-0.6B", "id": "a"}, {"label": "Llama", "id": "b"}],
            "targets": [1, 2],
            "scenarios": ["android_only"],
        }
        existing_rows = [
            {"series_label": "Qwen3-0.6B", "scenario": "android_only", "parallelism_target": "1", "status": "ok"},
            {"series_label": "Qwen3-0.6B", "scenario": "android_only", "parallelism_target": "2", "status": "error"},
            {"series_label": "Llama", "scenario": "android_only", "parallelism_target": "1", "status": "ok"},
        ]
        existing_details = [
            {"series_label": "Qwen3-0.6B", "scenario": "android_only", "parallelism_target": 1, "status": "ok"},
            {"series_label": "Qwen3-0.6B", "scenario": "android_only", "parallelism_target": 2, "status": "error"},
            {"series_label": "Llama", "scenario": "android_only", "parallelism_target": 1, "status": "ok"},
        ]
        plan, progress = rb.resolve_incomplete_model_sweep_plan("http://unused", config, existing_rows, existing_details)
        self.assertEqual(
            plan,
            [
                ("android_only", {"label": "Qwen3-0.6B", "id": "a"}, 2),
                ("android_only", {"label": "Llama", "id": "b"}, 2),
            ],
        )
        self.assertEqual(progress, {"expected_points": 4, "completed_points": 2, "pending_points": 2, "exhausted_points": 0})

    def test_resolve_incomplete_model_sweep_plan_skips_exhausted_points(self) -> None:
        config = {"models": [{"label": "Qwen3-0.6B", "id": "a"}], "targets": [1], "scenarios": ["android_only"]}
        existing_rows = [{"series_label": "Qwen3-0.6B", "scenario": "android_only", "parallelism_target": "1", "status": "error"}]
        existing_details = [{"series_label": "Qwen3-0.6B", "scenario": "android_only", "parallelism_target": 1, "status": "error", "attempts": rb.MAX_POINT_ATTEMPTS}]
        plan, progress = rb.resolve_incomplete_model_sweep_plan("http://unused", config, existing_rows, existing_details)
        self.assertEqual(plan, [])
        self.assertEqual(progress, {"expected_points": 1, "completed_points": 0, "pending_points": 0, "exhausted_points": 1})

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

    def test_merge_points_replaces_existing_point(self) -> None:
        existing = [{"series_label": "Qwen", "scenario": "android_only", "parallelism_target": 2, "status": "error"}]
        updates = [{"series_label": "Qwen", "scenario": "android_only", "parallelism_target": 2, "status": "ok"}]
        merged = rb.merge_points(existing, updates)
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["status"], "ok")

    def test_run_model_sweep_section_returns_early_when_all_points_ok(self) -> None:
        config = {"models": [{"label": "Qwen3-0.6B", "id": "a"}], "targets": [1], "scenarios": ["android_only"], "queries": 1, "concurrency": 1}
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir)
            out_dir = run_dir / "model-sweep"
            out_dir.mkdir(parents=True, exist_ok=True)
            rb.write_csv(out_dir / "model_sweep.csv", [{"series_label": "Qwen3-0.6B", "scenario": "android_only", "parallelism_target": 1, "status": "ok"}])
            rb.write_json(out_dir / "model_sweep.json", [{"series_label": "Qwen3-0.6B", "scenario": "android_only", "parallelism_target": 1, "status": "ok"}])
            with mock.patch.object(rb, "validate_scenario", side_effect=AssertionError("should not validate when already complete")):
                result = rb.run_model_sweep_section("http://unused", run_dir, config, rb.DEFAULT_PROMPT_PRESETS)
        self.assertEqual(result["rerun_points"], 0)
        self.assertEqual(result["remaining_points"], 0)
        self.assertEqual(result["completed_points"], 1)

    def test_run_model_sweep_section_retries_until_success(self) -> None:
        config = {"models": [{"label": "Qwen3-0.6B", "id": "a"}], "targets": [1], "scenarios": ["android_only"], "queries": 1, "concurrency": 1}
        attempts = {"count": 0}

        def fake_run(*args, **kwargs):
            attempts["count"] += 1
            if attempts["count"] < 3:
                return ({"summary": {}, "results": [], "errors": ["timed out"], "server_metrics_before": {}, "server_metrics_after": {}}, {})
            return ({"summary": {}, "results": [{"request_id": "req-1", "ttft_s": 0.2, "tps": 12.0, "completed_at": 2.0, "first_token_at": 1.0, "total_time_s": 2.0, "server_metric": {"allocated_node_count": 1, "allocated_node_ids": ["a"]}}], "errors": [], "server_metrics_before": {}, "server_metrics_after": {}}, {})

        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir)
            with mock.patch.object(rb, "validate_scenario", return_value={"scenario": "android_only", "nodes": []}), \
                mock.patch.object(rb, "set_parallelism_target"), \
                mock.patch.object(rb.time, "sleep"), \
                mock.patch.object(rb, "run_chat_workload_with_retries", side_effect=fake_run), \
                mock.patch.object(rb, "get_loading_status", return_value={}):
                result = rb.run_model_sweep_section("http://unused", run_dir, config, rb.DEFAULT_PROMPT_PRESETS)
            details = json.loads((run_dir / "model-sweep" / "model_sweep.json").read_text())
        self.assertEqual(attempts["count"], 3)
        self.assertEqual(result["remaining_points"], 0)
        self.assertEqual(result["exhausted_points"], 0)
        self.assertEqual(details[0]["attempts"], 3)
        self.assertEqual(details[0]["status"], "ok")

    def test_run_model_sweep_section_stops_at_max_attempts(self) -> None:
        config = {"models": [{"label": "Qwen3-0.6B", "id": "a"}], "targets": [1], "scenarios": ["android_only"], "queries": 1, "concurrency": 1}
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir)
            with mock.patch.object(rb, "validate_scenario", return_value={"scenario": "android_only", "nodes": []}), \
                mock.patch.object(rb, "set_parallelism_target"), \
                mock.patch.object(rb.time, "sleep"), \
                mock.patch.object(rb, "run_chat_workload_with_retries", return_value=({"summary": {}, "results": [], "errors": ["timed out"], "server_metrics_before": {}, "server_metrics_after": {}}, {})), \
                mock.patch.object(rb, "get_loading_status", return_value={}):
                result = rb.run_model_sweep_section("http://unused", run_dir, config, rb.DEFAULT_PROMPT_PRESETS)
            details = json.loads((run_dir / "model-sweep" / "model_sweep.json").read_text())
        self.assertEqual(result["rerun_points"], rb.MAX_POINT_ATTEMPTS)
        self.assertEqual(result["remaining_points"], 0)
        self.assertEqual(result["exhausted_points"], 1)
        self.assertEqual(details[0]["attempts"], rb.MAX_POINT_ATTEMPTS)
        self.assertEqual(details[0]["status"], "error")

    def test_write_run_metadata_records_scenario(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir)
            rb.write_run_metadata(run_dir, rb.now_utc(), None, {"model_sweep": {"remaining_points": 0}}, "running", "ios_only", "http://demo")
            payload = json.loads((run_dir / "run_metadata.json").read_text())
        self.assertEqual(payload["scenario"], "ios_only")
        self.assertEqual(payload["status"], "running")
        self.assertEqual(payload["config"]["base_url"], "http://demo")

    def test_main_uses_scenario_flag_for_run_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            expected_dir = Path(tmpdir) / "ios_only"
            with mock.patch.object(rb, "resolve_output_root", return_value=Path(tmpdir)), \
                mock.patch.object(rb, "run_model_sweep_section", return_value={"remaining_points": 0}) as run_model_sweep_section, \
                mock.patch.object(rb, "write_run_metadata") as write_run_metadata, \
                mock.patch.object(rb.tempfile, "mkdtemp", return_value=tmpdir), \
                mock.patch.object(rb.shutil, "rmtree"):
                exit_code = rb.main(["--scenario", "ios_only", "--base-url", "http://tracker:4917/"])
        self.assertEqual(exit_code, 0)
        first_call = write_run_metadata.call_args_list[0]
        self.assertEqual(first_call.args[0], expected_dir)
        self.assertEqual(first_call.args[4], "running")
        self.assertEqual(first_call.args[5], "ios_only")
        self.assertEqual(first_call.args[6], "http://tracker:4917")
        self.assertEqual(run_model_sweep_section.call_args.args[0], "http://tracker:4917")


if __name__ == "__main__":
    unittest.main()
