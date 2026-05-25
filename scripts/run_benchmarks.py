#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import threading
import time
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any

PRESETS_PATH = Path(__file__).with_name("prompt_presets.json")


def load_prompt_presets() -> dict:
    try:
        return json.loads(PRESETS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def request_json(url: str, method: str = "GET", body: dict | None = None, headers: dict | None = None) -> dict:
    data = None
    request_headers = {"Accept": "application/json"}
    if headers:
        request_headers.update(headers)
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=request_headers, method=method)
    with urllib.request.urlopen(req, timeout=300) as response:
        return json.loads(response.read().decode("utf-8"))


def request_bytes(url: str, method: str = "GET", body: bytes | None = None, headers: dict | None = None) -> tuple[int, bytes]:
    req = urllib.request.Request(url, data=body, headers=headers or {}, method=method)
    with urllib.request.urlopen(req, timeout=600) as response:
        return response.getcode(), response.read()


def set_parallelism_target(base_url: str, target: int) -> dict:
    return request_json(
        f"{base_url}/api/ui/parallelism-target",
        method="POST",
        body={"parallelism_target": target},
    )


def get_parallelism_target(base_url: str) -> dict:
    return request_json(f"{base_url}/api/ui/parallelism-target")


def get_storage_chunk_size(base_url: str) -> dict:
    return request_json(f"{base_url}/api/ui/storage-chunk-size")


def set_storage_chunk_size(base_url: str, chunk_size_bytes: int) -> dict:
    return request_json(
        f"{base_url}/api/ui/storage-chunk-size",
        method="POST",
        body={"chunk_size_bytes": chunk_size_bytes},
    )


def get_metrics_snapshot(base_url: str) -> dict:
    return request_json(f"{base_url}/api/ui/metrics")


def iter_sse_events(response) -> str:
    for raw_line in response:
        line = raw_line.decode("utf-8", errors="replace").strip()
        if not line.startswith("data: "):
            continue
        payload = line[6:].strip()
        if payload == "[DONE]":
            return
        if payload:
            yield payload


def run_streaming_chat_request(
    base_url: str,
    model: str,
    prompt: str,
    temperature: float,
    max_completion_tokens: int,
    preset_name: str,
) -> dict:
    request_id = str(uuid.uuid4())
    started = time.time()
    payload = {
        "model": model,
        "stream": True,
        "temperature": temperature,
        "max_tokens": max_completion_tokens,
        "messages": [{"role": "user", "content": prompt}],
    }
    req = urllib.request.Request(
        f"{base_url}/v1/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Accept": "text/event-stream",
            "Content-Type": "application/json",
            "X-Benchmark-Request-Id": request_id,
        },
        method="POST",
    )

    first_token_at = None
    completion_text_parts: list[str] = []
    reasoning_text_parts: list[str] = []

    with urllib.request.urlopen(req, timeout=900) as response:
        for event_payload in iter_sse_events(response):
            parsed = json.loads(event_payload)
            for choice in parsed.get("choices", []):
                delta = choice.get("delta", {})
                content = delta.get("content") or ""
                reasoning = delta.get("reasoning_content") or ""
                if (content or reasoning) and first_token_at is None:
                    first_token_at = time.time()
                if content:
                    completion_text_parts.append(content)
                if reasoning:
                    reasoning_text_parts.append(reasoning)

    completed = time.time()
    return {
        "request_id": request_id,
        "preset": preset_name,
        "started_at": started,
        "first_token_at": first_token_at,
        "completed_at": completed,
        "ttft_s": (first_token_at - started) if first_token_at is not None else None,
        "total_time_s": completed - started,
        "completion_chars": sum(len(part) for part in completion_text_parts),
        "reasoning_chars": sum(len(part) for part in reasoning_text_parts),
        "response_excerpt": "".join(completion_text_parts)[:200],
    }


def enrich_with_server_metrics(result: dict, request_metric: dict | None) -> dict:
    enriched = dict(result)
    if request_metric is None:
        enriched["tokens_streamed"] = None
        enriched["generation_time_s"] = None
        enriched["tpot_s"] = None
        enriched["tps"] = None
        return enriched

    tokens_streamed = request_metric.get("tokens_streamed")
    first_token_at = result.get("first_token_at")
    completed_at = result.get("completed_at")
    generation_time_s = None
    if first_token_at is not None and completed_at is not None:
        generation_time_s = completed_at - first_token_at

    tpot_s = None
    if generation_time_s is not None and tokens_streamed and tokens_streamed > 1:
        tpot_s = generation_time_s / (tokens_streamed - 1)

    tps = None
    if tpot_s and tpot_s > 0:
        tps = 1.0 / tpot_s

    enriched["tokens_streamed"] = tokens_streamed
    enriched["generation_time_s"] = generation_time_s
    enriched["tpot_s"] = tpot_s
    enriched["tps"] = tps
    enriched["server_metric"] = request_metric
    return enriched


def run_parallel_chat_workload(
    base_url: str,
    model: str,
    prompt: str,
    queries: int,
    concurrency: int,
    temperature: float,
    max_completion_tokens: int,
    preset_name: str,
) -> dict:
    results: list[dict] = []
    errors: list[str] = []
    lock = threading.Lock()
    next_index = 0

    def worker() -> None:
        nonlocal next_index
        while True:
            with lock:
                if next_index >= queries:
                    return
                next_index += 1
            try:
                result = run_streaming_chat_request(
                    base_url,
                    model,
                    prompt,
                    temperature,
                    max_completion_tokens,
                    preset_name,
                )
            except Exception as exc:
                with lock:
                    errors.append(str(exc))
                continue

            with lock:
                results.append(result)

    before_snapshot = get_metrics_snapshot(base_url)
    started = time.time()
    threads = [threading.Thread(target=worker, daemon=True) for _ in range(concurrency)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    completed = time.time()
    after_snapshot = get_metrics_snapshot(base_url)

    metrics_by_request_id = {
        item.get("client_request_id"): item
        for item in after_snapshot.get("requests", [])
        if item.get("client_request_id")
    }
    enriched_results = [
        enrich_with_server_metrics(result, metrics_by_request_id.get(result["request_id"]))
        for result in sorted(results, key=lambda item: item["started_at"])
    ]

    valid_ttft = [item["ttft_s"] for item in enriched_results if item.get("ttft_s") is not None]
    valid_tps = [item["tps"] for item in enriched_results if item.get("tps") is not None]

    summary = {
        "queries": queries,
        "concurrency": concurrency,
        "wall_time_s": completed - started,
        "avg_ttft_s": (sum(valid_ttft) / len(valid_ttft)) if valid_ttft else None,
        "avg_tps": (sum(valid_tps) / len(valid_tps)) if valid_tps else None,
        "error_count": len(errors),
    }

    return {
        "summary": summary,
        "results": enriched_results,
        "errors": errors,
        "server_metrics_before": before_snapshot,
        "server_metrics_after": after_snapshot,
    }


def time_download(url: str) -> dict:
    started = time.time()
    status, payload = request_bytes(url)
    completed = time.time()
    return {
        "url": url,
        "status_code": status,
        "bytes": len(payload),
        "duration_s": completed - started,
    }


def time_upload(url: str, payload: bytes, content_type: str = "application/octet-stream") -> dict:
    started = time.time()
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": content_type},
        method="PUT",
    )
    with urllib.request.urlopen(req, timeout=600) as response:
        status = response.getcode()
        response.read()
    completed = time.time()
    return {
        "url": url,
        "status_code": status,
        "bytes": len(payload),
        "duration_s": completed - started,
    }


def write_rows_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def resolve_prompt(args: argparse.Namespace, presets: dict) -> tuple[str, str, float, int]:
    if args.prompt_preset:
        preset = presets[args.prompt_preset]
        prompt = preset["prompt"]
        temperature = args.temperature if args.temperature is not None else preset.get("temperature", 0.7)
        max_completion_tokens = (
            args.max_completion_tokens
            if args.max_completion_tokens is not None
            else preset.get("max_completion_tokens", 2048)
        )
        return args.prompt_preset, prompt, temperature, max_completion_tokens

    prompt = args.prompt or "Explain how distributed inference works in two short paragraphs."
    temperature = args.temperature if args.temperature is not None else 0.7
    max_completion_tokens = args.max_completion_tokens if args.max_completion_tokens is not None else 2048
    return "custom", prompt, temperature, max_completion_tokens


def parse_int_list(raw: str) -> list[int]:
    return [int(item.strip()) for item in raw.split(",") if item.strip()]


def current_storage_chunk_size(base_url: str) -> int:
    return int(get_storage_chunk_size(base_url)["chunk_size_bytes"])


def build_upload_url(upload_url: str, chunk_size_bytes: int, stamp: str) -> str:
    if "{" in upload_url:
        return upload_url.format(chunk_size=chunk_size_bytes, stamp=stamp)
    parsed = urllib.parse.urlparse(upload_url)
    path = parsed.path
    suffix = f"-{chunk_size_bytes}-{stamp}"
    if "/" in path:
        head, tail = path.rsplit("/", 1)
    else:
        head, tail = "", path
    if "." in tail:
        stem, ext = tail.rsplit(".", 1)
        tail = f"{stem}{suffix}.{ext}"
    else:
        tail = f"{tail}{suffix}"
    new_path = f"{head}/{tail}" if head else tail
    return urllib.parse.urlunparse(parsed._replace(path=new_path))


# ----- Sweep / model-vs-node-count logic -----

def workload_to_row(series_label: str, model_id: str, target: int, scenario: str, workload: dict, storage_chunk_size_bytes: int) -> dict:
    result = workload.get("results", [{}])[0] if workload.get("results") else {}
    metric = result.get("server_metric", {})
    return {
        "benchmark_kind": "chat_tps",
        "series_label": series_label,
        "model": model_id,
        "parallelism_target": target,
        "scenario": scenario,
        "storage_chunk_size_bytes": storage_chunk_size_bytes,
        "fixed_file_size_bytes": "",
        "status": "ok",
        "ttft_s": result.get("ttft_s", ""),
        "generation_time_s": result.get("generation_time_s", ""),
        "total_time_s": result.get("total_time_s", ""),
        "tokens_streamed": result.get("tokens_streamed", ""),
        "tpot_s": result.get("tpot_s", ""),
        "tps": result.get("tps", ""),
        "completion_chars": result.get("completion_chars", ""),
        "reasoning_chars": result.get("reasoning_chars", ""),
        "request_id": result.get("request_id", ""),
        "loading_node_count": metric.get("allocated_node_count", ""),
        "allocated_devices": ",".join(metric.get("allocated_node_ids", []) or []),
        "error": "" if workload.get("summary", {}).get("error_count", 0) == 0 else "; ".join(workload.get("errors", [])),
    }


def failure_row(series_label: str, model_id: str, target: int, scenario: str, error: str, loading_status: dict, storage_chunk_size_bytes: int) -> dict:
    return {
        "benchmark_kind": "chat_tps",
        "series_label": series_label,
        "model": model_id,
        "parallelism_target": target,
        "scenario": scenario,
        "storage_chunk_size_bytes": storage_chunk_size_bytes,
        "fixed_file_size_bytes": "",
        "status": "error",
        "ttft_s": "",
        "generation_time_s": "",
        "total_time_s": "",
        "tokens_streamed": "",
        "tpot_s": "",
        "tps": "",
        "completion_chars": "",
        "reasoning_chars": "",
        "request_id": "",
        "loading_node_count": loading_status.get("node_count", ""),
        "allocated_devices": "",
        "error": error,
    }


def write_outputs(json_path: Path, csv_path: Path, json_rows: list[dict], csv_rows: list[dict]) -> None:
    json_path.write_text(json.dumps(json_rows, indent=2), encoding="utf-8")
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        if not csv_rows:
            return
        writer = csv.DictWriter(handle, fieldnames=list(csv_rows[0].keys()))
        writer.writeheader()
        writer.writerows(csv_rows)


DEFAULT_MODELS = [
    {"label": "1.0B Llama 3.2", "id": "hf:unsloth/Llama-3.2-1B-Instruct-GGUF:Llama-3.2-1B-Instruct-Q4_K_M.gguf"},
    {"label": "1.5B DeepSeek-R1-Distill-Qwen", "id": "hf:unsloth/DeepSeek-R1-Distill-Qwen-1.5B-GGUF:DeepSeek-R1-Distill-Qwen-1.5B-Q4_K_M.gguf"},
    {"label": "1.7B Qwen3", "id": "hf:unsloth/Qwen3-1.7B-GGUF:Qwen3-1.7B-Q4_K_M.gguf"},
    {"label": "3.0B Llama 3.2", "id": "hf:unsloth/Llama-3.2-3B-Instruct-GGUF:Llama-3.2-3B-Instruct-Q4_K_M.gguf"},
    {"label": "4.0B Qwen3", "id": "hf:unsloth/Qwen3-4B-GGUF:Qwen3-4B-Q4_K_M.gguf"},
    {"label": "4.0B Gemma 3", "id": "hf:unsloth/gemma-3-4b-it-GGUF:gemma-3-4b-it-Q4_K_M.gguf"},
    {"label": "7.0B DeepSeek-R1-Distill-Qwen", "id": "hf:unsloth/DeepSeek-R1-Distill-Qwen-7B-GGUF:DeepSeek-R1-Distill-Qwen-7B-Q4_K_M.gguf"},
]


def parse_targets(raw: str) -> list[int]:
    values = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        values.append(int(item))
    if not values:
        raise SystemExit("At least one target node count is required")
    return values


def parse_scenarios(raw: str) -> list[str]:
    values = [item.strip() for item in raw.split(",") if item.strip()]
    if not values:
        raise SystemExit("At least one scenario is required")
    return values


def choose_models(mode: str) -> list[dict]:
    if mode == "large-only":
        return [item for item in DEFAULT_MODELS if item["label"][0] in {"3", "4", "7"}]
    return DEFAULT_MODELS


# ----- Plotting utilities -----

def load_csv(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def render_chat_plots(chat_csv_paths: list[str], output_dir: Path) -> None:
    import matplotlib.pyplot as plt

    rows_by_file = [(Path(csv_path), load_csv(Path(csv_path))) for csv_path in chat_csv_paths]
    metrics = [
        ("ttft_s", "Time to First Token", "TTFT (s)", "chat-ttft.png"),
        ("tpot_s", "Time per Output Token", "TPOT (s/token)", "chat-tpot.png"),
        ("tps", "Tokens per Second", "TPS", "chat-tps.png"),
        ("total_time_s", "Query Completion Time", "Total time (s)", "chat-total-time.png"),
    ]

    for field, title, ylabel, filename in metrics:
        fig, ax = plt.subplots(figsize=(10, 6))
        plotted = False
        for path, rows in rows_by_file:
            filtered = [row for row in rows if row.get(field) not in ("", "None", None)]
            if not filtered:
                continue
            xs = [int(row["query_index"]) for row in filtered]
            ys = [float(row[field]) for row in filtered]
            ax.plot(xs, ys, marker="o", label=path.stem)
            plotted = True
        if not plotted:
            plt.close(fig)
            continue
        ax.set_title(title)
        ax.set_xlabel("Query index")
        ax.set_ylabel(ylabel)
        ax.legend()
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(output_dir / filename, dpi=160)


def render_download_plot(download_csv_paths: list[str], output_dir: Path) -> None:
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(10, 6))
    plotted = False
    for csv_path in download_csv_paths:
        path = Path(csv_path)
        rows = load_csv(path)
        if not rows:
            continue
        xs = [row.get("shard_size", str(index + 1)) for index, row in enumerate(rows)]
        ys = [float(row["duration_s"]) for row in rows]
        ax.plot(xs, ys, marker="o", label=path.stem)
        plotted = True
    if not plotted:
        plt.close(fig)
        return
    ax.set_title("Download time by shard size")
    ax.set_xlabel("Shard size")
    ax.set_ylabel("Duration (s)")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(output_dir / "download-durations.png", dpi=160)


from collections import defaultdict

def render_overlay_plot(
    overlay_csv_path: str,
    output_dir: Path,
    x_field: str,
    y_field: str,
    series_field: str,
    filename: str,
    title: str,
    ylabel: str,
    exclude_x_values: set[int] | None = None,
    filter_field: str | None = None,
    filter_value: str | None = None,
) -> None:
    import matplotlib.pyplot as plt

    rows = load_csv(Path(overlay_csv_path))
    grouped: dict[str, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        if filter_field and filter_value is not None and row.get(filter_field) != filter_value:
            continue
        x_raw = row.get(x_field)
        y_raw = row.get(y_field)
        series = row.get(series_field)
        if not series or x_raw in ("", "None", None) or y_raw in ("", "None", None):
            continue
        x_value = int(x_raw)
        if exclude_x_values and x_value in exclude_x_values:
            continue
        grouped[series][x_value].append(float(y_raw))

    if not grouped:
        return

    fig, ax = plt.subplots(figsize=(10, 6))
    for series, points in sorted(grouped.items()):
        xs = sorted(points.keys())
        ys = [sum(points[x]) / len(points[x]) for x in xs]
        ax.plot(xs, ys, marker="o", label=series)

    ax.set_title(title)
    ax.set_xlabel(x_field.replace("_", " ").title())
    ax.set_ylabel(ylabel)
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(output_dir / filename, dpi=160)


def render_scenario_overlay_plots(
    overlay_csv_path: str,
    output_dir: Path,
    x_field: str,
    y_field: str,
    series_field: str,
    base_filename: str,
    base_title: str,
    ylabel: str,
    scenarios: list[str],
    exclude_x_values: set[int] | None = None,
) -> None:
    for scenario in scenarios:
        render_overlay_plot(
            overlay_csv_path,
            output_dir,
            x_field,
            y_field,
            series_field,
            f"{scenario}-{base_filename}",
            f"{base_title} ({scenario})",
            ylabel,
            exclude_x_values,
            filter_field="scenario",
            filter_value=scenario,
        )


def render_storage_plot(
    csv_paths: list[str],
    output_dir: Path,
    benchmark_kind: str,
    x_field: str,
    title: str,
    ylabel: str,
    filename: str,
) -> None:
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(10, 6))
    plotted = False
    for csv_path in csv_paths:
        rows = [row for row in load_csv(Path(csv_path)) if row.get("benchmark_kind") == benchmark_kind]
        if not rows:
            continue
        series_label = Path(csv_path).stem
        xs: list[float] = []
        ys: list[float] = []
        for row in rows:
            x_raw = row.get(x_field)
            y_raw = row.get("duration_s")
            if x_raw in ("", "None", None) or y_raw in ("", "None", None):
                continue
            xs.append(float(x_raw))
            ys.append(float(y_raw))
        if not xs:
            continue
        ax.plot(xs, ys, marker="o", label=series_label)
        plotted = True
    if not plotted:
        plt.close(fig)
        return
    ax.set_title(title)
    ax.set_xlabel(x_field.replace("_", " ").title())
    ax.set_ylabel(ylabel)
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(output_dir / filename, dpi=160)


# ----- Command implementations -----

def cmd_run(args: argparse.Namespace) -> int:
    presets = load_prompt_presets()
    if args.list_presets:
        for key, value in presets.items():
            print(f"{key}: {value.get('label', key)}")
        return 0

    wants_chat = bool(args.model)
    wants_download = bool(args.download_url)
    wants_upload = bool(args.upload_url)
    if not (wants_chat or wants_download or wants_upload):
        raise SystemExit("provide --model, --download-url, or --upload-url")

    if wants_chat:
        preset_name, prompt, temperature, max_completion_tokens = resolve_prompt(args, presets)
    else:
        preset_name = "none"
        prompt = ""
        temperature = 0.0
        max_completion_tokens = 0

    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    stamp = time.strftime("%Y%m%d-%H%M%S")
    prefix = f"{preset_name}-{args.scenario}-g{args.group_size}-p{args.parallelism_target}-{stamp}"

    if wants_chat and not args.model:
        raise SystemExit("--model is required for chat benchmarks")

    if wants_chat:
        set_parallelism_target(args.base_url, args.parallelism_target)
        chat_result = run_parallel_chat_workload(
            args.base_url,
            args.model,
            prompt,
            args.queries,
            args.concurrency,
            temperature,
            max_completion_tokens,
            preset_name,
        )
        storage_chunk_size_bytes = current_storage_chunk_size(args.base_url)
        chat_json = results_dir / f"chat-{prefix}.json"
        chat_csv = results_dir / f"chat-{prefix}.csv"
        chat_json.write_text(json.dumps(chat_result, indent=2), encoding="utf-8")
        write_rows_csv(
            chat_csv,
            [
                {
                    "benchmark_kind": "chat_tps",
                    "query_index": index,
                    "series_label": args.series_label or args.model,
                    "model": args.model,
                    "parallelism_target": args.parallelism_target,
                    "scenario": args.scenario,
                    "group_size": args.group_size,
                    "storage_chunk_size_bytes": storage_chunk_size_bytes,
                    "fixed_file_size_bytes": "",
                    "request_id": row["request_id"],
                    "preset": row["preset"],
                    "ttft_s": row["ttft_s"],
                    "generation_time_s": row["generation_time_s"],
                    "total_time_s": row["total_time_s"],
                    "tokens_streamed": row["tokens_streamed"],
                    "tpot_s": row["tpot_s"],
                    "tps": row["tps"],
                    "completion_chars": row["completion_chars"],
                    "reasoning_chars": row["reasoning_chars"],
                    "started_at": row["started_at"],
                    "first_token_at": row["first_token_at"],
                    "completed_at": row["completed_at"],
                }
                for index, row in enumerate(chat_result["results"], start=1)
            ],
        )

    if wants_download and args.shard_sizes:
        download_rows: list[dict] = []
        for shard_size in [item.strip() for item in args.shard_sizes.split(",") if item.strip()]:
            url = args.download_url.format(shard_size=urllib.parse.quote(shard_size))
            row = time_download(url)
            row.update(
                {
                    "benchmark_kind": "download_shard_sweep",
                    "scenario": args.scenario,
                    "parallelism_target": get_parallelism_target(args.base_url).get("parallelism_target", ""),
                    "storage_chunk_size_bytes": current_storage_chunk_size(args.base_url),
                    "fixed_file_size_bytes": row["bytes"],
                    "shard_size": shard_size,
                }
            )
            download_rows.append(row)
        write_rows_csv(Path(args.results_dir) / f"downloads-shards-{prefix}.csv", download_rows)

    if wants_download and args.download_targets:
        download_rows = []
        for target in parse_int_list(args.download_targets):
            set_parallelism_target(args.base_url, target)
            row = time_download(args.download_url)
            row.update(
                {
                    "benchmark_kind": "download_device_sweep",
                    "scenario": args.scenario,
                    "parallelism_target": target,
                    "storage_chunk_size_bytes": current_storage_chunk_size(args.base_url),
                    "fixed_file_size_bytes": row["bytes"],
                    "shard_size": "",
                }
            )
            download_rows.append(row)
        write_rows_csv(Path(args.results_dir) / f"downloads-devices-{prefix}.csv", download_rows)

    if wants_upload and args.storage_chunk_sizes:
        upload_rows: list[dict] = []
        payload = b"x" * args.upload_size_bytes
        for chunk_size_bytes in parse_int_list(args.storage_chunk_sizes):
            set_storage_chunk_size(args.base_url, chunk_size_bytes)
            row = time_upload(build_upload_url(args.upload_url, chunk_size_bytes, stamp), payload)
            row.update(
                {
                    "benchmark_kind": "upload_chunk_sweep",
                    "scenario": args.scenario,
                    "parallelism_target": get_parallelism_target(args.base_url).get("parallelism_target", ""),
                    "storage_chunk_size_bytes": chunk_size_bytes,
                    "fixed_file_size_bytes": args.upload_size_bytes,
                    "shard_size": "",
                }
            )
            upload_rows.append(row)
        write_rows_csv(Path(args.results_dir) / f"uploads-{prefix}.csv", upload_rows)

    return 0


def cmd_sweep(args: argparse.Namespace) -> int:
    presets = load_prompt_presets()
    prompt = presets[args.prompt_preset]["prompt"] if args.prompt_preset else ""
    targets = parse_targets(args.targets)
    scenarios = parse_scenarios(args.scenarios)
    models = choose_models(args.model_mode)
    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    for scenario in scenarios:
        json_rows: list[dict] = []
        csv_rows: list[dict] = []
        prefix = f"{args.results_prefix}-{scenario}"
        json_path = results_dir / f"{prefix}.json"
        csv_path = results_dir / f"{prefix}.csv"

        for model in models:
            print(f"== {scenario} :: {model['label']} ==")
            for target in targets:
                print(f"  target={target}")
                point_status = {
                    "series_label": model["label"],
                    "model": model["id"],
                    "parallelism_target": target,
                    "scenario": scenario,
                }
                try:
                    set_parallelism_target(args.base_url, target)
                    time.sleep(1.0)
                    storage_chunk_size_bytes = int(get_storage_chunk_size(args.base_url)["chunk_size_bytes"])
                    workload = run_parallel_chat_workload(
                        args.base_url,
                        model["id"],
                        prompt,
                        args.queries,
                        args.concurrency,
                        args.temperature,
                        args.max_completion_tokens,
                        args.prompt_preset or "",
                    )
                    if workload.get("errors") or not workload.get("results"):
                        error = "; ".join(workload.get("errors", [])) or "no results returned"
                        loading_status = get_metrics_snapshot(args.base_url)
                        row = failure_row(model["label"], model["id"], target, scenario, error, loading_status, storage_chunk_size_bytes)
                        point_status["workload"] = workload
                        point_status["loading_status_after"] = loading_status
                        point_status["status"] = "error"
                        point_status["error"] = error
                        csv_rows.append(row)
                        json_rows.append(point_status)
                        write_outputs(json_path, csv_path, json_rows, csv_rows)
                        continue

                    row = workload_to_row(model["label"], model["id"], target, scenario, workload, storage_chunk_size_bytes)
                    point_status["workload"] = workload
                    point_status["loading_status_after"] = get_metrics_snapshot(args.base_url)
                    point_status["status"] = "ok"
                    csv_rows.append(row)
                    json_rows.append(point_status)
                    write_outputs(json_path, csv_path, json_rows, csv_rows)
                except Exception as exc:
                    loading_status = {}
                    try:
                        loading_status = get_metrics_snapshot(args.base_url)
                    except Exception:
                        pass
                    storage_chunk_size_bytes = ""
                    try:
                        storage_chunk_size_bytes = int(get_storage_chunk_size(args.base_url)["chunk_size_bytes"])
                    except Exception:
                        pass
                    error = repr(exc)
                    point_status["status"] = "exception"
                    point_status["error"] = error
                    point_status["loading_status_after"] = loading_status
                    csv_rows.append(failure_row(model["label"], model["id"], target, scenario, error, loading_status, storage_chunk_size_bytes))
                    json_rows.append(point_status)
                    write_outputs(json_path, csv_path, json_rows, csv_rows)

        print(json_path)
        print(csv_path)

    return 0


def cmd_plot(args: argparse.Namespace) -> int:
    try:
        import matplotlib.pyplot as _  # noqa: F401
    except ImportError as exc:
        raise SystemExit("matplotlib is required to render plots") from exc

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.chat_csv:
        render_chat_plots(args.chat_csv, output_dir)

    if args.download_csv:
        render_download_plot(args.download_csv, output_dir)

    exclude_x_values = {
        int(value.strip())
        for value in args.overlay_exclude_x.split(",")
        if value.strip()
    }

    if args.overlay_csv:
        render_overlay_plot(
            args.overlay_csv,
            output_dir,
            args.overlay_x,
            args.overlay_y,
            args.overlay_series,
            args.overlay_filename,
            args.overlay_title,
            args.overlay_y.upper(),
            exclude_x_values,
        )

    if args.overlay_csv and args.scenario_overlays:
        render_scenario_overlay_plots(
            args.overlay_csv,
            output_dir,
            args.overlay_x,
            args.overlay_y,
            args.overlay_series,
            args.overlay_filename,
            args.overlay_title,
            args.overlay_y.upper(),
            [item.strip() for item in args.scenario_overlays.split(",") if item.strip()],
            exclude_x_values,
        )

    if args.device_download_csv:
        render_storage_plot(
            args.device_download_csv,
            output_dir,
            "download_device_sweep",
            "parallelism_target",
            "Download Time by Target Devices",
            "Duration (s)",
            "download-device-sweep.png",
        )

    if args.download_csv:
        render_storage_plot(
            args.download_csv,
            output_dir,
            "download_shard_sweep",
            "fixed_file_size_bytes",
            "Download Time by Fixed File Size",
            "Duration (s)",
            "download-fixed-size.png",
        )

    if args.upload_csv:
        render_storage_plot(
            args.upload_csv,
            output_dir,
            "upload_chunk_sweep",
            "storage_chunk_size_bytes",
            "Upload Time by Storage Chunk Size",
            "Duration (s)",
            "upload-chunk-sweep.png",
        )

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Combined benchmark tool: run chat/download/upload benchmarks, run sweeps, and render plots.")
    subparsers = parser.add_subparsers(dest="command")

    # run subcommand (original run_benchmarks)
    run_parser = subparsers.add_parser("run", help="Run chat/download/upload benchmarks")
    run_parser.add_argument("--base-url", default="http://127.0.0.1:4917")
    run_parser.add_argument("--model", required=False)
    run_parser.add_argument("--prompt", default="")
    run_parser.add_argument("--prompt-preset", choices=None)
    run_parser.add_argument("--list-presets", action="store_true")
    run_parser.add_argument("--queries", type=int, default=8)
    run_parser.add_argument("--concurrency", type=int, default=2)
    run_parser.add_argument("--parallelism-target", type=int, default=2)
    run_parser.add_argument("--temperature", type=float)
    run_parser.add_argument("--max-completion-tokens", type=int)
    run_parser.add_argument("--scenario", default="heterogeneous")
    run_parser.add_argument("--group-size", type=int, default=2)
    run_parser.add_argument("--download-url", default="")
    run_parser.add_argument("--shard-sizes", default="")
    run_parser.add_argument("--download-targets", default="")
    run_parser.add_argument("--upload-url", default="")
    run_parser.add_argument("--upload-size-bytes", type=int, default=8 * 1024 * 1024)
    run_parser.add_argument("--storage-chunk-sizes", default="")
    run_parser.add_argument("--results-dir", default="architecture/benchmarks/results")
    run_parser.add_argument("--series-label", default="")

    # sweep subcommand
    sweep_parser = subparsers.add_parser("sweep", help="Run model-vs-node-count sweep")
    sweep_parser.add_argument("--base-url", default="http://127.0.0.1:4917")
    sweep_parser.add_argument("--targets", default="1,2,3,4,5")
    sweep_parser.add_argument("--scenarios", default="heterogeneous")
    sweep_parser.add_argument("--model-mode", choices=["all", "large-only"], default="all")
    sweep_parser.add_argument("--prompt-preset", choices=None, default="endless_creative_writing")
    sweep_parser.add_argument("--max-completion-tokens", type=int, default=64)
    sweep_parser.add_argument("--temperature", type=float, default=0.7)
    sweep_parser.add_argument("--queries", type=int, default=1)
    sweep_parser.add_argument("--concurrency", type=int, default=1)
    sweep_parser.add_argument("--results-prefix", default="five_phone_all_model_sweep")
    sweep_parser.add_argument("--results-dir", default="architecture/benchmarks/results")

    # plot subcommand
    plot_parser = subparsers.add_parser("plot", help="Render plots from benchmark CSVs")
    plot_parser.add_argument("--chat-csv", action="append", default=[])
    plot_parser.add_argument("--download-csv", action="append", default=[])
    plot_parser.add_argument("--overlay-csv", default="")
    plot_parser.add_argument("--overlay-x", default="parallelism_target")
    plot_parser.add_argument("--overlay-y", default="tps")
    plot_parser.add_argument("--overlay-series", default="series_label")
    plot_parser.add_argument("--overlay-filename", default="chat-overlay.png")
    plot_parser.add_argument("--overlay-title", default="Benchmark Overlay")
    plot_parser.add_argument("--overlay-exclude-x", default="")
    plot_parser.add_argument("--scenario-overlays", default="")
    plot_parser.add_argument("--device-download-csv", action="append", default=[])
    plot_parser.add_argument("--upload-csv", action="append", default=[])
    plot_parser.add_argument("--output-dir", default="architecture/benchmarks/results")

    args = parser.parse_args()

    if args.command == "run":
        return cmd_run(args)
    if args.command == "sweep":
        return cmd_sweep(args)
    if args.command == "plot":
        return cmd_plot(args)

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
