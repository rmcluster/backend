#!/usr/bin/env python3

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import hashlib
import json
import os
import random
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


DEFAULT_SERVER_URL = "http://127.0.0.1:4917"
DEFAULT_CHUNK_SIZES_KIB = [256, 512, 1024, 2048, 4096, 8192]
DEFAULT_PLOTS = ["devices", "chunk-size"]
DEFAULT_REPETITIONS = 3
DEFAULT_FILE_SIZE_MIB = 10
REQUEST_TIMEOUT = (5, 180)


class BenchmarkError(RuntimeError):
    pass


@dataclass(frozen=True)
class Device:
    key: str
    id: str | None
    ip: str
    rpc_port: int
    storage_port: int
    hardware_model: str
    max_size: int | None

    @property
    def label(self) -> str:
        if self.id:
            return self.id
        return self.key

    @property
    def model_label(self) -> str:
        if self.hardware_model:
            return self.hardware_model
        return self.label

    @property
    def storage_base_url(self) -> str:
        return f"http://{self.ip}:{self.storage_port}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run automated storage benchmarks against rmcluster.")
    parser.add_argument("--server-url", default=DEFAULT_SERVER_URL, help="Base URL of the rmcluster server.")
    parser.add_argument(
        "--out-dir",
        default="benchmarks/results",
        help="Directory where timestamped benchmark artifacts will be written.",
    )
    parser.add_argument(
        "--file-size-mib",
        type=int,
        default=DEFAULT_FILE_SIZE_MIB,
        help="Payload size in MiB used by both experiments.",
    )
    parser.add_argument(
        "--chunk-sizes-kib",
        default=",".join(str(v) for v in DEFAULT_CHUNK_SIZES_KIB),
        help="Comma-separated chunk sizes in KiB for the chunk-size experiment.",
    )
    parser.add_argument(
        "--repetitions",
        type=int,
        default=DEFAULT_REPETITIONS,
        help="Number of repetitions per x-axis value.",
    )
    parser.add_argument(
        "--plots",
        default=",".join(DEFAULT_PLOTS),
        help="Comma-separated plots to run: devices,chunk-size.",
    )
    parser.add_argument(
        "--cleanup",
        type=parse_bool,
        default=True,
        help="Whether to delete created chunks/files after each repetition.",
    )
    parser.add_argument(
        "--device-id",
        default="",
        help="Optional device id to validate as connected before chunk-size runs. Does not override server placement.",
    )
    parser.add_argument(
        "--device-counts",
        default="",
        help="Optional comma-separated device counts to test for the devices plot.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=12345,
        help="Seed used to generate a reproducible payload.",
    )
    return parser.parse_args()


def parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"invalid boolean value: {value}")


def parse_csv_ints(raw: str) -> list[int]:
    values: list[int] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        values.append(int(item))
    return values


def parse_plots(raw: str) -> list[str]:
    plots = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        if item not in {"devices", "chunk-size"}:
            raise BenchmarkError(f"unsupported plot {item!r}; expected devices and/or chunk-size")
        plots.append(item)
    if not plots:
        raise BenchmarkError("no plots selected")
    return plots


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def resolve_output_root(path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    return repo_root() / path


def timestamp_slug(now: datetime) -> str:
    return now.strftime("%Y%m%d-%H%M%S")


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def make_payload(size_bytes: int, seed: int) -> bytes:
    rng = random.Random(seed)
    return bytes(rng.getrandbits(8) for _ in range(size_bytes))


def run_git(args: list[str]) -> str | None:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=repo_root(),
            check=True,
            capture_output=True,
            text=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None
    return result.stdout.strip() or None


def git_context() -> dict[str, Any]:
    return {
        "commit": run_git(["rev-parse", "HEAD"]),
        "branch": run_git(["rev-parse", "--abbrev-ref", "HEAD"]),
        "status_short": run_git(["status", "--short"]),
    }


def device_sort_key(device: Device) -> tuple[str, str]:
    return (device.id or "", device.key)


def normalize_devices(servers: list[dict[str, Any]]) -> list[Device]:
    devices: list[Device] = []
    for server in servers:
        storage_port = int(server.get("storage_port") or 0)
        if storage_port <= 0:
            continue
        ip = str(server["ip"])
        rpc_port = int(server["port"])
        device_id = server.get("id")
        device_key = str(device_id or f"{ip}:{rpc_port}")
        max_size = server.get("max_size")
        devices.append(
            Device(
                key=device_key,
                id=str(device_id) if device_id else None,
                ip=ip,
                rpc_port=rpc_port,
                storage_port=storage_port,
                hardware_model=str(server.get("hardware_model") or ""),
                max_size=int(max_size) if isinstance(max_size, int) else None,
            )
        )
    devices.sort(key=device_sort_key)
    return devices


def fetch_servers(server_url: str) -> list[dict[str, Any]]:
    response = requests.get(f"{server_url}/servers", timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    payload = response.json()
    return payload.get("servers", [])


def fetch_devices(server_url: str) -> list[Device]:
    return normalize_devices(fetch_servers(server_url))


def fetch_chunk_size(server_url: str) -> int:
    response = requests.get(f"{server_url}/api/ui/storage-chunk-size", timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    payload = response.json()
    return int(payload["chunk_size_bytes"])


def set_chunk_size(server_url: str, chunk_size_bytes: int) -> int:
    response = requests.post(
        f"{server_url}/api/ui/storage-chunk-size",
        json={"chunk_size_bytes": chunk_size_bytes},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    return int(payload["chunk_size_bytes"])


def ensure_webdav_directory(server_url: str, directory: str) -> None:
    response = requests.request("MKCOL", f"{server_url}{directory}", timeout=REQUEST_TIMEOUT)
    if response.status_code in {201, 405}:
        return
    if response.status_code == 409:
        raise BenchmarkError(f"failed to create WebDAV directory {directory}: parent missing")
    response.raise_for_status()


def upload_chunk_to_device(device: Device, chunk_id: str, payload: bytes) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        response = requests.put(
            f"{device.storage_base_url}/chunk/{chunk_id}",
            data=payload,
            timeout=REQUEST_TIMEOUT,
        )
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        ok = response.status_code == 200
        error = "" if ok else response.text.strip()
        return {
            "device_key": device.key,
            "elapsed_ms": elapsed_ms,
            "status_code": response.status_code,
            "ok": ok,
            "error": error,
        }
    except requests.RequestException as exc:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        return {
            "device_key": device.key,
            "elapsed_ms": elapsed_ms,
            "status_code": None,
            "ok": False,
            "error": str(exc),
        }


def verify_device_chunk(device: Device, chunk_id: str) -> tuple[bool, str]:
    try:
        response = requests.get(f"{device.storage_base_url}/chunks/list", timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        chunks = response.json()
    except (requests.RequestException, ValueError) as exc:
        return False, str(exc)
    if chunk_id not in chunks:
        return False, "chunk missing from /chunks/list"
    return True, ""


def delete_device_chunk(device: Device, chunk_id: str) -> tuple[bool, str]:
    try:
        response = requests.delete(f"{device.storage_base_url}/chunk/{chunk_id}", timeout=REQUEST_TIMEOUT)
    except requests.RequestException as exc:
        return False, str(exc)
    if response.status_code in {200, 404}:
        return True, ""
    return False, response.text.strip()


def run_devices_experiment(
    server_url: str,
    payload: bytes,
    repetitions: int,
    requested_counts: list[int] | None,
    cleanup: bool,
) -> list[dict[str, Any]]:
    devices = fetch_devices(server_url)
    if not devices:
        raise BenchmarkError("no storage-capable devices discovered from /servers")

    if requested_counts:
        device_counts = sorted(set(requested_counts))
        max_count = len(devices)
        invalid = [count for count in device_counts if count < 1 or count > max_count]
        if invalid:
            raise BenchmarkError(f"device counts out of range for {max_count} devices: {invalid}")
    else:
        device_counts = list(range(1, len(devices) + 1))

    chunk_id = sha256_hex(payload)
    rows: list[dict[str, Any]] = []

    for device_count in device_counts:
        selected = devices[:device_count]
        selected_keys = [device.label for device in selected]
        selected_models = [device.model_label for device in selected]
        print(f"[devices] device_count={device_count} selected={selected_keys}")

        for repetition in range(1, repetitions + 1):
            print(f"[devices] repetition={repetition}/{repetitions}")
            total_started = time.perf_counter()
            with concurrent.futures.ThreadPoolExecutor(max_workers=device_count) as executor:
                futures = [executor.submit(upload_chunk_to_device, device, chunk_id, payload) for device in selected]
                per_device = [future.result() for future in futures]
            total_upload_ms = (time.perf_counter() - total_started) * 1000.0

            verify_errors: list[str] = []
            all_uploads_ok = all(item["ok"] for item in per_device)
            all_verified = False
            if all_uploads_ok:
                all_verified = True
                for device in selected:
                    verified, error = verify_device_chunk(device, chunk_id)
                    if not verified:
                        all_verified = False
                        verify_errors.append(f"{device.label}: {error}")

            cleanup_errors: list[str] = []
            if cleanup:
                for device in selected:
                    cleaned, error = delete_device_chunk(device, chunk_id)
                    if not cleaned and error:
                        cleanup_errors.append(f"{device.label}: {error}")

            success = all_uploads_ok and all_verified
            error_parts = []
            for item in per_device:
                if not item["ok"]:
                    error_parts.append(f"{item['device_key']}: {item['error'] or item['status_code']}")
            error_parts.extend(verify_errors)
            error_parts.extend(cleanup_errors)

            rows.append(
                {
                    "device_count": device_count,
                    "repetition": repetition,
                    "payload_size_bytes": len(payload),
                    "chunk_id": chunk_id,
                    "selected_devices": json_dumps(selected_keys),
                    "selected_models": json_dumps(selected_models),
                    "total_upload_ms": round(total_upload_ms, 3),
                    "per_device_upload_ms": json_dumps(
                        {item["device_key"]: round(float(item["elapsed_ms"]), 3) for item in per_device}
                    ),
                    "per_device_status_codes": json_dumps(
                        {item["device_key"]: item["status_code"] for item in per_device}
                    ),
                    "success": success,
                    "error": " | ".join(part for part in error_parts if part),
                }
            )
    return rows


def webdav_put(server_url: str, remote_path: str, payload: bytes) -> float:
    started = time.perf_counter()
    response = requests.put(f"{server_url}{remote_path}", data=payload, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return (time.perf_counter() - started) * 1000.0


def webdav_get(server_url: str, remote_path: str) -> tuple[bytes, float]:
    started = time.perf_counter()
    response = requests.get(f"{server_url}{remote_path}", timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    return response.content, elapsed_ms


def webdav_delete(server_url: str, remote_path: str) -> tuple[bool, str]:
    try:
        response = requests.delete(f"{server_url}{remote_path}", timeout=REQUEST_TIMEOUT)
    except requests.RequestException as exc:
        return False, str(exc)
    if response.status_code in {200, 204, 404}:
        return True, ""
    return False, response.text.strip()


def validate_device_id(server_url: str, device_id: str) -> Device:
    for device in fetch_devices(server_url):
        if device.id == device_id or device.key == device_id:
            return device
    raise BenchmarkError(f"device-id {device_id!r} was not found among currently connected devices")


def run_chunk_size_experiment(
    server_url: str,
    payload: bytes,
    repetitions: int,
    chunk_sizes_bytes: list[int],
    cleanup: bool,
    device_id: str,
) -> list[dict[str, Any]]:
    if device_id:
        selected_device = validate_device_id(server_url, device_id)
        print(
            f"[chunk-size] validated requested device-id={selected_device.label}; "
            "server-side placement remains scheduler/GCAS managed"
        )

    ensure_webdav_directory(server_url, "/dav/benchmarks")
    original_chunk_size = fetch_chunk_size(server_url)
    rows: list[dict[str, Any]] = []

    try:
        for chunk_size_bytes in chunk_sizes_bytes:
            print(f"[chunk-size] chunk_size_bytes={chunk_size_bytes}")
            applied = set_chunk_size(server_url, chunk_size_bytes)
            if applied != chunk_size_bytes:
                raise BenchmarkError(
                    f"server reported chunk_size_bytes={applied} after setting {chunk_size_bytes}"
                )

            for repetition in range(1, repetitions + 1):
                remote_path = f"/dav/benchmarks/chunk-size-{chunk_size_bytes}-rep-{repetition}.bin"
                connected_devices = fetch_devices(server_url)
                upload_ms = 0.0
                download_ms = 0.0
                success = False
                error = ""
                downloaded = b""

                try:
                    upload_ms = webdav_put(server_url, remote_path, payload)
                    downloaded, download_ms = webdav_get(server_url, remote_path)
                    success = sha256_hex(downloaded) == sha256_hex(payload)
                    if not success:
                        error = "downloaded payload checksum mismatch"
                except requests.RequestException as exc:
                    error = str(exc)

                cleanup_errors: list[str] = []
                if cleanup:
                    cleaned, cleanup_error = webdav_delete(server_url, remote_path)
                    if not cleaned and cleanup_error:
                        cleanup_errors.append(cleanup_error)

                if cleanup_errors:
                    error = " | ".join([part for part in [error, *cleanup_errors] if part])

                rows.append(
                    {
                        "chunk_size_bytes": chunk_size_bytes,
                        "chunk_size_kib": chunk_size_bytes // 1024,
                        "repetition": repetition,
                        "file_size_bytes": len(payload),
                        "upload_ms": round(upload_ms, 3),
                        "download_ms": round(download_ms, 3),
                        "checksum_ok": success,
                        "connected_device_count": len(connected_devices),
                        "connected_devices": json_dumps([device.label for device in connected_devices]),
                        "success": success,
                        "error": error,
                    }
                )
    finally:
        restored = set_chunk_size(server_url, original_chunk_size)
        if restored != original_chunk_size:
            raise BenchmarkError(
                f"failed to restore chunk size to {original_chunk_size}; server reported {restored}"
            )
    return rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def summarize_rows(rows: list[dict[str, Any]], x_field: str, y_field: str) -> list[tuple[int, float, float, int]]:
    grouped: dict[int, list[float]] = {}
    for row in rows:
        if not row.get("success"):
            continue
        x_value = int(row[x_field])
        grouped.setdefault(x_value, []).append(float(row[y_field]))

    summary = []
    for x_value in sorted(grouped):
        values = grouped[x_value]
        mean = statistics.mean(values)
        stddev = statistics.stdev(values) if len(values) > 1 else 0.0
        summary.append((x_value, mean, stddev, len(values)))
    return summary


def write_plot(
    path: Path,
    title: str,
    subtitle: str,
    x_label: str,
    y_label: str,
    x_values: list[int],
    means: list[float],
    errors: list[float],
) -> None:
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 5))
    if x_values:
        ax.errorbar(x_values, means, yerr=errors, marker="o", capsize=5, linewidth=2)
        ax.set_xticks(x_values)
    else:
        ax.text(0.5, 0.5, "No successful samples", ha="center", va="center", transform=ax.transAxes)
    ax.set_title(title)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.grid(True, alpha=0.3)
    fig.text(0.5, 0.93, subtitle, ha="center")
    fig.tight_layout(rect=(0, 0, 1, 0.9))
    fig.savefig(path, dpi=160)
    plt.close(fig)


def plot_devices(rows: list[dict[str, Any]], path: Path, file_size_mib: int, run_slug: str) -> None:
    summary = summarize_rows(rows, "device_count", "total_upload_ms")
    write_plot(
        path=path,
        title="Devices vs Upload Time",
        subtitle=f"Run {run_slug} | fixed payload {file_size_mib} MiB",
        x_label="Connected devices used",
        y_label="Mean total upload time (ms)",
        x_values=[item[0] for item in summary],
        means=[item[1] for item in summary],
        errors=[item[2] for item in summary],
    )


def plot_chunk_sizes(rows: list[dict[str, Any]], path: Path, file_size_mib: int, run_slug: str) -> None:
    summary = summarize_rows(rows, "chunk_size_kib", "download_ms")
    write_plot(
        path=path,
        title="Chunk Size vs Download Time",
        subtitle=f"Run {run_slug} | fixed payload {file_size_mib} MiB",
        x_label="Chunk size (KiB)",
        y_label="Mean download time (ms)",
        x_values=[item[0] for item in summary],
        means=[item[1] for item in summary],
        errors=[item[2] for item in summary],
    )


def main() -> int:
    args = parse_args()
    server_url = args.server_url.rstrip("/")
    file_size_bytes = args.file_size_mib * 1024 * 1024
    chunk_sizes_kib = parse_csv_ints(args.chunk_sizes_kib)
    chunk_sizes_bytes = [value * 1024 for value in chunk_sizes_kib]
    plots = parse_plots(args.plots)
    device_counts = parse_csv_ints(args.device_counts) if args.device_counts else None

    if args.file_size_mib < 1:
        raise BenchmarkError("--file-size-mib must be >= 1")
    if args.repetitions < 1:
        raise BenchmarkError("--repetitions must be >= 1")
    if any(value < 1 for value in chunk_sizes_kib):
        raise BenchmarkError("--chunk-sizes-kib values must be >= 1")

    started_at = now_utc()
    run_slug = timestamp_slug(started_at)
    run_dir = resolve_output_root(args.out_dir) / run_slug
    run_dir.mkdir(parents=True, exist_ok=False)
    mplconfigdir = Path(tempfile.mkdtemp(prefix="storage-bench-mpl-", dir="/private/tmp"))
    os.environ["MPLCONFIGDIR"] = str(mplconfigdir)
    import matplotlib

    matplotlib.use("Agg")

    payload = make_payload(file_size_bytes, args.seed)
    payload_sha = sha256_hex(payload)
    metadata: dict[str, Any] = {
        "server_url": server_url,
        "started_at": started_at.isoformat(),
        "file_size_bytes": file_size_bytes,
        "file_size_mib": args.file_size_mib,
        "payload_sha256": payload_sha,
        "chunk_sizes_kib": chunk_sizes_kib,
        "chunk_sizes_bytes": chunk_sizes_bytes,
        "repetitions": args.repetitions,
        "plots": plots,
        "cleanup": args.cleanup,
        "seed": args.seed,
        "device_id": args.device_id or None,
        "device_counts": device_counts,
        "git": git_context(),
        "initial_devices": [
            {
                "key": device.key,
                "id": device.id,
                "ip": device.ip,
                "rpc_port": device.rpc_port,
                "storage_port": device.storage_port,
                "hardware_model": device.hardware_model,
                "max_size": device.max_size,
            }
            for device in fetch_devices(server_url)
        ],
    }

    device_rows: list[dict[str, Any]] = []
    chunk_size_rows: list[dict[str, Any]] = []

    try:
        if "devices" in plots:
            device_rows = run_devices_experiment(
                server_url=server_url,
                payload=payload,
                repetitions=args.repetitions,
                requested_counts=device_counts,
                cleanup=args.cleanup,
            )
            write_csv(run_dir / "devices_vs_upload_time.csv", device_rows)
            plot_devices(
                device_rows,
                run_dir / "devices_vs_upload_time.png",
                file_size_mib=args.file_size_mib,
                run_slug=run_slug,
            )

        if "chunk-size" in plots:
            chunk_size_rows = run_chunk_size_experiment(
                server_url=server_url,
                payload=payload,
                repetitions=args.repetitions,
                chunk_sizes_bytes=chunk_sizes_bytes,
                cleanup=args.cleanup,
                device_id=args.device_id.strip(),
            )
            write_csv(run_dir / "chunk_size_vs_download_time.csv", chunk_size_rows)
            plot_chunk_sizes(
                chunk_size_rows,
                run_dir / "chunk_size_vs_download_time.png",
                file_size_mib=args.file_size_mib,
                run_slug=run_slug,
            )
    finally:
        finished_at = now_utc()
        metadata["finished_at"] = finished_at.isoformat()
        metadata["device_rows"] = len(device_rows)
        metadata["chunk_size_rows"] = len(chunk_size_rows)
        write_json(run_dir / "run_metadata.json", metadata)
        shutil.rmtree(mplconfigdir, ignore_errors=True)

    print(f"Artifacts written to {run_dir}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except BenchmarkError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
