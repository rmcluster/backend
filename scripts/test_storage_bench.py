from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
os.environ.setdefault("MPLCONFIGDIR", "/private/tmp/mpl-test-cache")

import storage_bench as sb  # noqa: E402


class StorageBenchHelpersTest(unittest.TestCase):
    def test_parse_csv_ints(self) -> None:
        self.assertEqual(sb.parse_csv_ints("256, 512,1024"), [256, 512, 1024])

    def test_make_payload_is_deterministic(self) -> None:
        payload_a = sb.make_payload(64, seed=123)
        payload_b = sb.make_payload(64, seed=123)
        payload_c = sb.make_payload(64, seed=456)
        self.assertEqual(payload_a, payload_b)
        self.assertNotEqual(payload_a, payload_c)

    def test_normalize_devices_filters_and_sorts(self) -> None:
        servers = [
            {"ip": "10.0.0.2", "port": 9002, "storage_port": 0, "hardware_model": "skip"},
            {"id": "node-b", "ip": "10.0.0.3", "port": 9003, "storage_port": 47672, "hardware_model": "B"},
            {"ip": "10.0.0.1", "port": 9001, "storage_port": 47672, "hardware_model": "A"},
        ]
        devices = sb.normalize_devices(servers)
        self.assertEqual([device.key for device in devices], ["10.0.0.1:9001", "node-b"])

    def test_summarize_rows_ignores_failures(self) -> None:
        rows = [
            {"device_count": 1, "total_upload_ms": 100.0, "success": True},
            {"device_count": 1, "total_upload_ms": 140.0, "success": True},
            {"device_count": 2, "total_upload_ms": 500.0, "success": False},
        ]
        summary = sb.summarize_rows(rows, "device_count", "total_upload_ms")
        self.assertEqual(summary, [(1, 120.0, 28.284271247461902, 2)])

    def test_plot_writes_png(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "figure.png"
            sb.write_plot(
                path=path,
                title="Demo",
                subtitle="Subtitle",
                x_label="X",
                y_label="Y",
                x_values=[1, 2],
                means=[10.0, 20.0],
                errors=[1.0, 2.0],
            )
            self.assertTrue(path.exists())
            self.assertGreater(path.stat().st_size, 0)


if __name__ == "__main__":
    unittest.main()
