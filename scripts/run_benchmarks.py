#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import os
import shutil
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PRESETS_PATH = Path(__file__).with_name("prompt_presets.json")
DEFAULT_PROMPT_PRESETS = {
    "endless_creative_writing": {
        "label": "Endless Creative Writing",
        "prompt": "Write an open-ended creative fantasy story. Use flowery, wordy, descriptive language and expand on every detail.",
        "temperature": 0.7,
        "max_completion_tokens": 2048,
    },
    "formatted_data": {
        "label": "Ranking People",
        "prompt": 'Create a JSON list for 20 fictional people in JSON format: {"name": <name>, "age": <age>, "height_cm": <height>, "gender": [<male or female>]}. After the json, output a markdown table ranking them by height.',
        "temperature": 0.7,
        "max_completion_tokens": 2048,
    },
}
DEFAULT_MODELS = [
    {"label": "Qwen3-0.6B", "id": "hf:unsloth/Qwen3-0.6B-GGUF:UD-Q4_K_XL.gguf"},
    {"label": "1.0B Llama 3.2", "id": "hf:unsloth/Llama-3.2-1B-Instruct-GGUF:Llama-3.2-1B-Instruct-Q4_K_M.gguf"},
    {"label": "1.5B DeepSeek-R1-Distill-Qwen", "id": "hf:unsloth/DeepSeek-R1-Distill-Qwen-1.5B-GGUF:DeepSeek-R1-Distill-Qwen-1.5B-Q4_K_M.gguf"},
    {"label": "1.7B Qwen3", "id": "hf:unsloth/Qwen3-1.7B-GGUF:Qwen3-1.7B-Q4_K_M.gguf"},
    {"label": "3.0B Llama 3.2", "id": "hf:unsloth/Llama-3.2-3B-Instruct-GGUF:Llama-3.2-3B-Instruct-Q4_K_M.gguf"},
    {"label": "4.0B Qwen3", "id": "hf:unsloth/Qwen3-4B-GGUF:Qwen3-4B-Q4_K_M.gguf"},
    {"label": "4.0B Gemma 3", "id": "hf:unsloth/gemma-3-4b-it-GGUF:gemma-3-4b-it-Q4_K_M.gguf"},
    {"label": "7.0B DeepSeek-R1-Distill-Qwen", "id": "hf:unsloth/DeepSeek-R1-Distill-Qwen-7B-GGUF:DeepSeek-R1-Distill-Qwen-7B-Q4_K_M.gguf"},
]

BASE_URL = "http://127.0.0.1:4917"
RESULTS_DIR = "benchmarks/results"
RUN_LABEL = "benchmark-run"
MODEL_LOAD_TIMEOUT_S = 90
MODEL_LOAD_POLL_S = 2
REQUEST_RETRY_LIMIT = 1
REQUEST_TIMEOUT_S = 300
WARMUP_PROMPT = "Say hi."
WARMUP_MAX_TOKENS = 1
MODEL_SWEEP_CONFIG = {
    "models": DEFAULT_MODELS,
    "targets": "auto",
    "scenarios": ["android_only"],
    "queries": 1,
    "concurrency": 1,
    "prompt_preset": "endless_creative_writing",
    "prompt": "",
    "temperature": 0.7,
    "max_completion_tokens": 64,
}
# Re-run points that are either still failing or were collected before the
# sticky warmup/measured-instance fix, so their comparisons are not trustworthy.
RERUN_POINTS = [
    {"model_label": "Qwen3-0.6B", "scenario": "android_only", "target": 1},
    {"model_label": "Qwen3-0.6B", "scenario": "android_only", "target": 2},
    {"model_label": "Qwen3-0.6B", "scenario": "android_only", "target": 3},
    {"model_label": "1.0B Llama 3.2", "scenario": "android_only", "target": 1},
    {"model_label": "1.0B Llama 3.2", "scenario": "android_only", "target": 2},
    {"model_label": "1.0B Llama 3.2", "scenario": "android_only", "target": 3},
]


def repo_root() -> Path: return Path(__file__).resolve().parents[1]
def now_utc() -> datetime: return datetime.now(timezone.utc)
def timestamp_slug(now: datetime) -> str: return now.strftime("%Y%m%d-%H%M%S")
def resolve_output_root(path_str: str) -> Path:
    path = Path(path_str)
    return path if path.is_absolute() else repo_root() / path


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_model_sweep_artifacts(out_dir: Path, rows: list[dict[str, Any]], details: list[dict[str, Any]]) -> None:
    write_json(out_dir / "model_sweep.json", details)
    write_csv(out_dir / "model_sweep.csv", rows)
    try:
        write_plot(
            out_dir / "model-sweep-tps.png",
            "Tokens per Second by Parallelism Target",
            "Parallelism Target",
            "TPS",
            grouped_means(rows, "parallelism_target", "tps"),
        )
    except ModuleNotFoundError:
        pass


def load_prompt_presets(path: Path = PRESETS_PATH) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return DEFAULT_PROMPT_PRESETS.copy()


def resolve_prompt(config: dict[str, Any], presets: dict[str, Any]) -> tuple[str, str, float, int]:
    preset_name, custom_prompt = str(config.get("prompt_preset") or ""), str(config.get("prompt") or "").strip()
    if preset_name:
        preset = presets[preset_name]
        return (
            preset_name,
            custom_prompt or preset["prompt"],
            config.get("temperature") if config.get("temperature") is not None else preset.get("temperature", 0.7),
            config.get("max_completion_tokens") if config.get("max_completion_tokens") is not None else preset.get("max_completion_tokens", 2048),
        )
    return ("custom", custom_prompt or "Explain how distributed inference works in two short paragraphs.", float(config.get("temperature") if config.get("temperature") is not None else 0.7), int(config.get("max_completion_tokens") if config.get("max_completion_tokens") is not None else 2048))


def api_json(base_url: str, path: str, method: str = "GET", body: dict[str, Any] | None = None, timeout: int = 300) -> dict[str, Any]:
    data, headers = None, {"Accept": "application/json"}
    if body is not None:
        data, headers["Content-Type"] = json.dumps(body).encode("utf-8"), "application/json"
    req = urllib.request.Request(f"{base_url.rstrip('/')}{path}", data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def get_metrics_snapshot(base_url: str) -> dict[str, Any]: return api_json(base_url, "/api/ui/metrics")
def get_loading_status(base_url: str) -> dict[str, Any]: return api_json(base_url, "/api/ui/loading-status")
def connected_nodes(base_url: str) -> list[dict[str, Any]]: return api_json(base_url, "/servers").get("servers", [])
def connected_node_count(base_url: str) -> int: return len(connected_nodes(base_url))
def set_parallelism_target(base_url: str, target: int) -> None: api_json(base_url, "/api/ui/parallelism-target", "POST", {"parallelism_target": target})


def classify_platform(hardware_model: str) -> str:
    name = hardware_model.lower()
    if any(token in name for token in ["iphone", "ipad", "ios"]):
        return "ios"
    return "android" if name else "unknown"


def detect_cluster_scenario(nodes: list[dict[str, Any]]) -> str:
    platforms = {classify_platform(str(node.get("hardware_model") or "")) for node in nodes} - {"unknown"}
    return {frozenset({"android"}): "android_only", frozenset({"ios"}): "ios_only", frozenset({"android", "ios"}): "heterogeneous"}.get(frozenset(platforms), "unknown")


def validate_scenario(base_url: str, scenario: str) -> dict[str, Any]:
    if scenario not in {"android_only", "ios_only", "heterogeneous"}:
        raise RuntimeError(f"Unsupported scenario {scenario!r}; expected android_only, ios_only, or heterogeneous")
    nodes = connected_nodes(base_url)
    detected = detect_cluster_scenario(nodes)
    if detected != scenario:
        raise RuntimeError(f"Connected cluster is {detected}, but scenario is set to {scenario}. Change connected devices or update the scenario label.")
    return {"scenario": detected, "nodes": [{"id": node.get("id"), "hardware_model": node.get("hardware_model")} for node in nodes]}


def resolve_sweep_targets(base_url: str, targets: Any) -> list[int]:
    return list(range(1, connected_node_count(base_url) + 1)) if targets == "auto" else [int(target) for target in targets]


def matches_rerun_point(model_label: str, scenario: str, target: int, rerun_points: list[dict[str, Any]]) -> bool:
    return any(
        str(point.get("model_label")) == model_label and
        str(point.get("scenario")) == scenario and
        int(point.get("target")) == target
        for point in rerun_points
    )


def select_model_sweep_plan(base_url: str, config: dict[str, Any], rerun_points: list[dict[str, Any]]) -> list[tuple[str, dict[str, Any], int]]:
    plan: list[tuple[str, dict[str, Any], int]] = []
    targets = resolve_sweep_targets(base_url, config["targets"])
    for scenario in config["scenarios"]:
        for model in config["models"]:
            for target in targets:
                if rerun_points and not matches_rerun_point(model["label"], str(scenario), int(target), rerun_points):
                    continue
                plan.append((str(scenario), model, int(target)))
    return plan


def wait_for_loading(base_url: str, model: str | None = None, timeout_s: int = MODEL_LOAD_TIMEOUT_S) -> dict[str, Any]:
    deadline, status = time.time() + timeout_s, {}
    while time.time() < deadline:
        status = get_loading_status(base_url)
        phase, active_model = status.get("phase") or "", status.get("model") or ""
        if phase in {"", "ready"}:
            return status
        if model is not None and active_model and active_model != model:
            return status
        time.sleep(MODEL_LOAD_POLL_S)
    return status


def iter_sse_events(response) -> str:
    for raw in response:
        line = raw.decode("utf-8", errors="replace").strip()
        if line.startswith("data: "):
            payload = line[6:].strip()
            if payload == "[DONE]":
                return
            if payload:
                yield payload


def run_streaming_chat_request(base_url: str, model: str, prompt: str, temperature: float, max_completion_tokens: int, preset_name: str, benchmark_group_id: str = "", benchmark_stage: str = "") -> dict[str, Any]:
    request_id, started = str(uuid.uuid4()), time.time()
    headers = {"Accept": "text/event-stream", "Content-Type": "application/json", "X-Benchmark-Request-Id": request_id}
    if benchmark_group_id:
        headers["X-Benchmark-Group-Id"] = benchmark_group_id
    if benchmark_stage:
        headers["X-Benchmark-Stage"] = benchmark_stage
    req = urllib.request.Request(
        f"{base_url.rstrip('/')}/v1/chat/completions",
        data=json.dumps({"model": model, "stream": True, "temperature": temperature, "max_tokens": max_completion_tokens, "messages": [{"role": "user", "content": prompt}]}).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    first_token_at, completion_chars, reasoning_chars = None, 0, 0
    try:
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_S) as response:
            for event_payload in iter_sse_events(response):
                for choice in json.loads(event_payload).get("choices", []):
                    delta = choice.get("delta", {})
                    content, reasoning = delta.get("content") or "", delta.get("reasoning_content") or ""
                    if (content or reasoning) and first_token_at is None:
                        first_token_at = time.time()
                    completion_chars += len(content)
                    reasoning_chars += len(reasoning)
    except urllib.error.HTTPError as exc:
        detail = ""
        try:
            payload = json.loads(exc.read().decode("utf-8"))
            detail = str(payload.get("error") or "").strip()
        except Exception:
            detail = ""
        message = f"HTTP Error {exc.code}: {exc.reason}"
        if detail:
            message = f"{message}: {detail}"
        raise RuntimeError(message) from exc
    except TimeoutError as exc:
        raise RuntimeError(f"client timed out after {REQUEST_TIMEOUT_S}s waiting for streamed response") from exc
    except urllib.error.URLError as exc:
        if isinstance(exc.reason, TimeoutError):
            raise RuntimeError(f"client timed out after {REQUEST_TIMEOUT_S}s waiting for streamed response") from exc
        raise
    completed = time.time()
    return {"request_id": request_id, "preset": preset_name, "started_at": started, "first_token_at": first_token_at, "completed_at": completed, "ttft_s": (first_token_at - started) if first_token_at is not None else None, "total_time_s": completed - started, "completion_chars": completion_chars, "reasoning_chars": reasoning_chars}


def workload_only_has_503s(workload: dict[str, Any]) -> bool:
    return bool(workload["errors"]) and not workload["results"] and all("503" in error for error in workload["errors"])


def is_capacity_failure(message: str) -> bool:
    text = message.lower()
    return "instance died during startup" in text or "llama_kv_cache" in text or "buffer_clear" in text


def annotate_failure(message: str) -> str:
    if is_capacity_failure(message):
        return f"{message} [assumed insufficient RAM/capacity for this model at this target]"
    return message


def enrich_result(result: dict[str, Any], metric: dict[str, Any] | None) -> dict[str, Any]:
    if not metric:
        return result | {"tokens_streamed": None, "generation_time_s": None, "tpot_s": None, "tps": None}
    generation_time_s = result["completed_at"] - result["first_token_at"] if result.get("first_token_at") is not None else None
    tokens = metric.get("tokens_streamed")
    tpot_s = generation_time_s / (tokens - 1) if generation_time_s is not None and tokens and tokens > 1 else None
    return result | {"tokens_streamed": tokens, "generation_time_s": generation_time_s, "tpot_s": tpot_s, "tps": metric.get("tokens_per_second"), "server_metric": metric}


def find_request_metric(snapshot: dict[str, Any], request_id: str) -> dict[str, Any] | None:
    for item in snapshot.get("requests") or []:
        if item.get("client_request_id") == request_id:
            return item
    return None


def request_allocated_devices(result: dict[str, Any]) -> tuple[str, ...]:
    metric = result.get("server_metric") or {}
    return tuple(metric.get("allocated_node_ids") or ())


def run_parallel_chat_workload(base_url: str, model: str, prompt: str, queries: int, concurrency: int, temperature: float, max_completion_tokens: int, preset_name: str, benchmark_group_id: str = "", benchmark_stage: str = "") -> dict[str, Any]:
    results, errors, lock, next_index = [], [], threading.Lock(), 0

    def worker() -> None:
        nonlocal next_index
        while True:
            with lock:
                if next_index >= queries:
                    return
                next_index += 1
            try:
                result = run_streaming_chat_request(base_url, model, prompt, temperature, max_completion_tokens, preset_name, benchmark_group_id, benchmark_stage)
            except Exception as exc:
                with lock:
                    errors.append(str(exc))
                continue
            with lock:
                results.append(result)

    before, started = get_metrics_snapshot(base_url), time.time()
    threads = [threading.Thread(target=worker, daemon=True) for _ in range(concurrency)]
    [thread.start() for thread in threads]
    [thread.join() for thread in threads]
    after = get_metrics_snapshot(base_url)
    metrics_by_id = {item.get("client_request_id"): item for item in (after.get("requests") or []) if item.get("client_request_id")}
    enriched = [enrich_result(result, metrics_by_id.get(result["request_id"])) for result in sorted(results, key=lambda item: item["started_at"])]
    ttfts, tps_values = [item["ttft_s"] for item in enriched if item.get("ttft_s") is not None], [item["tps"] for item in enriched if item.get("tps") is not None]
    return {"summary": {"queries": queries, "concurrency": concurrency, "wall_time_s": time.time() - started, "avg_ttft_s": sum(ttfts) / len(ttfts) if ttfts else None, "avg_tps": sum(tps_values) / len(tps_values) if tps_values else None, "error_count": len(errors)}, "results": enriched, "errors": errors, "server_metrics_before": before, "server_metrics_after": after}


def warm_up_model(base_url: str, model: str, benchmark_group_id: str) -> dict[str, Any]:
    result = run_streaming_chat_request(base_url, model, WARMUP_PROMPT, 0.0, WARMUP_MAX_TOKENS, "warmup", benchmark_group_id, "warmup")
    metric = find_request_metric(get_metrics_snapshot(base_url), result["request_id"])
    return enrich_result(result, metric)


def run_chat_workload_with_retries(base_url: str, model: str, prompt: str, queries: int, concurrency: int, temperature: float, max_completion_tokens: int, preset_name: str) -> tuple[dict[str, Any], dict[str, Any]]:
    loading = wait_for_loading(base_url, model)
    last_error = ""
    for attempt in range(REQUEST_RETRY_LIMIT + 1):
        benchmark_group_id = str(uuid.uuid4())
        try:
            warmup = warm_up_model(base_url, model, benchmark_group_id)
            workload = run_parallel_chat_workload(base_url, model, prompt, queries, concurrency, temperature, max_completion_tokens, preset_name, benchmark_group_id, "measured")
        except Exception as exc:
            last_error = annotate_failure(str(exc))
            if attempt >= REQUEST_RETRY_LIMIT or "503" not in last_error or is_capacity_failure(last_error):
                return {"summary": {"queries": queries, "concurrency": concurrency, "wall_time_s": 0.0, "avg_ttft_s": None, "avg_tps": None, "error_count": 1}, "results": [], "errors": [last_error], "server_metrics_before": {}, "server_metrics_after": {}}, loading
            loading = wait_for_loading(base_url, model)
            continue
        if len(workload["results"]) == 1:
            warmup_devices = request_allocated_devices(warmup)
            measured_devices = request_allocated_devices(workload["results"][0])
            if warmup_devices and measured_devices and warmup_devices != measured_devices:
                last_error = (
                    "warmup and measured request used different device sets: "
                    f"{','.join(warmup_devices)} vs {','.join(measured_devices)}"
                )
                if attempt >= REQUEST_RETRY_LIMIT:
                    return {"summary": {"queries": queries, "concurrency": concurrency, "wall_time_s": 0.0, "avg_ttft_s": None, "avg_tps": None, "error_count": 1}, "results": [], "errors": [last_error], "server_metrics_before": workload.get("server_metrics_before", {}), "server_metrics_after": workload.get("server_metrics_after", {})}, loading
                loading = wait_for_loading(base_url, model)
                continue
        if not workload_only_has_503s(workload):
            return workload, loading
        last_error = annotate_failure("; ".join(workload["errors"]))
        if is_capacity_failure(last_error):
            return {"summary": {"queries": queries, "concurrency": concurrency, "wall_time_s": 0.0, "avg_ttft_s": None, "avg_tps": None, "error_count": 1}, "results": [], "errors": [last_error], "server_metrics_before": {}, "server_metrics_after": {}}, loading
        loading = wait_for_loading(base_url)
    return {"summary": {"queries": queries, "concurrency": concurrency, "wall_time_s": 0.0, "avg_ttft_s": None, "avg_tps": None, "error_count": 1}, "results": [], "errors": [last_error or "model instance failed to serve request"], "server_metrics_before": {}, "server_metrics_after": {}}, loading


def sweep_row(series_label: str, model_id: str, target: int, scenario: str, workload: dict[str, Any] | None = None, error: str = "", loading_status: dict[str, Any] | None = None) -> dict[str, Any]:
    result = (workload or {}).get("results", [{}])[0]
    metric = result.get("server_metric", {})
    return {
        "benchmark_kind": "chat_tps",
        "series_label": series_label,
        "model": model_id,
        "parallelism_target": target,
        "scenario": scenario,
        "status": "ok" if not error else "error",
        "ttft_s": result.get("ttft_s", ""),
        "generation_time_s": result.get("generation_time_s", ""),
        "total_time_s": result.get("total_time_s", ""),
        "tokens_streamed": result.get("tokens_streamed", ""),
        "tpot_s": result.get("tpot_s", ""),
        "tps": result.get("tps", ""),
        "completion_chars": result.get("completion_chars", ""),
        "reasoning_chars": result.get("reasoning_chars", ""),
        "request_id": result.get("request_id", ""),
        "loading_node_count": metric.get("allocated_node_count", (loading_status or {}).get("node_count", "")),
        "allocated_devices": ",".join(metric.get("allocated_node_ids", []) or []),
        "error": error,
    }


def grouped_means(rows: list[dict[str, Any]], x_field: str, y_field: str) -> dict[str, list[tuple[float, float]]]:
    grouped: dict[str, dict[float, list[float]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        x_raw, y_raw, series = row.get(x_field), row.get(y_field), str(row.get("series_label") or "")
        if series and x_raw not in ("", None, "None") and y_raw not in ("", None, "None"):
            grouped[series][float(x_raw)].append(float(y_raw))
    return {series: [(x, sum(values) / len(values)) for x, values in sorted(points.items())] for series, points in grouped.items()}


def write_plot(path: Path, title: str, x_label: str, y_label: str, series: dict[str, list[tuple[float, float]]]) -> None:
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(10, 6))
    if series:
        for label, points in sorted(series.items()):
            xs, ys = zip(*points)
            ax.plot(xs, ys, marker="o", label=label)
        ax.set_title(title)
        ax.set_xlabel(x_label)
        ax.set_ylabel(y_label)
        ax.legend()
        ax.grid(True, alpha=0.3)
    else:
        ax.text(0.5, 0.5, "No plottable data", ha="center", va="center", transform=ax.transAxes)
    fig.tight_layout()
    fig.savefig(path, dpi=160)
    plt.close(fig)


def render_line_plot(path: Path, title: str, x_label: str, y_label: str, series_to_points: dict[str, list[tuple[float, float]]]) -> None:
    write_plot(path, title, x_label, y_label, series_to_points)


def write_run_metadata(run_dir: Path, started_at: datetime, finished_at: datetime | None, results: dict[str, Any], status: str) -> None:
    write_json(
        run_dir / "run_metadata.json",
        {
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat() if finished_at else None,
            "status": status,
            "config": {
                "base_url": BASE_URL,
                "results_dir": RESULTS_DIR,
                "run_label": RUN_LABEL,
                "model_sweep_config": MODEL_SWEEP_CONFIG,
            },
            "results": results,
        },
    )


def run_model_sweep_section(base_url: str, run_dir: Path, config: dict[str, Any], presets: dict[str, Any]) -> dict[str, Any]:
    preset_name, prompt, temperature, max_tokens = resolve_prompt(config, presets)
    out_dir, rows, details = run_dir / "model-sweep", [], []
    plan = select_model_sweep_plan(base_url, config, RERUN_POINTS)
    scenario_info_cache: dict[str, dict[str, Any]] = {}
    current_heading: tuple[str, str] | None = None
    for scenario, model, target in plan:
        if scenario not in scenario_info_cache:
            scenario_info_cache[scenario] = validate_scenario(base_url, scenario)
        scenario_info = scenario_info_cache[scenario]
        heading = (scenario, model["label"])
        if heading != current_heading:
            print(f"== {scenario} :: {model['label']} ==", flush=True)
            current_heading = heading
        print(f"  target={target}", flush=True)
        try:
            set_parallelism_target(base_url, target)
            time.sleep(1.0)
            workload, loading = run_chat_workload_with_retries(base_url, model["id"], prompt, int(config["queries"]), int(config["concurrency"]), float(temperature), int(max_tokens), preset_name)
            error = "; ".join(workload["errors"]) if workload["errors"] or not workload["results"] else ""
            rows.append(sweep_row(model["label"], model["id"], target, scenario, workload if not error else None, error, loading or get_loading_status(base_url)))
            details.append({"series_label": model["label"], "model": model["id"], "parallelism_target": target, "scenario": scenario, "scenario_info": scenario_info, "status": rows[-1]["status"], "error": error, "loading_status_after": loading, "workload": workload})
        except Exception as exc:
            loading = get_loading_status(base_url)
            rows.append(sweep_row(model["label"], model["id"], target, scenario, None, repr(exc), loading))
            details.append({"series_label": model["label"], "model": model["id"], "parallelism_target": target, "scenario": scenario, "scenario_info": scenario_info, "status": "error", "error": repr(exc), "loading_status_after": loading})
        write_model_sweep_artifacts(out_dir, rows, details)
        print(f"    -> {rows[-1]['status']}", flush=True)
    return {"rows": rows, "points": len(rows)}


def main() -> int:
    started_at = now_utc()
    run_dir = resolve_output_root(RESULTS_DIR) / f"{timestamp_slug(started_at)}-{RUN_LABEL}"
    run_dir.mkdir(parents=True, exist_ok=False)
    mplconfigdir, base_url = Path(tempfile.mkdtemp(prefix="run-benchmarks-mpl-")), BASE_URL.rstrip("/")
    os.environ["MPLCONFIGDIR"] = str(mplconfigdir)
    results: dict[str, Any] = {}
    write_run_metadata(run_dir, started_at, None, results, "running")
    try:
        results["model_sweep"] = run_model_sweep_section(base_url, run_dir, MODEL_SWEEP_CONFIG, load_prompt_presets())
        write_run_metadata(run_dir, started_at, now_utc(), results, "completed")
    except Exception:
        write_run_metadata(run_dir, started_at, now_utc(), results, "failed")
        raise
    finally:
        shutil.rmtree(mplconfigdir, ignore_errors=True)
    print(f"Artifacts written to {run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
