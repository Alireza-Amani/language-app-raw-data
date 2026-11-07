#!/usr/bin/env python3
"""Standalone CLI Runner for the E2E Test Suite of Scaled Multi-Agent Persian Harvesting.

Supports:
- Selective Tier Execution: --tier {1, 2, 3, 4, all}
- Verbose Logging: --verbose / -v
- Machine-Readable Reporting: --json-report <path>
- Dual-Mode Harness: Runs via pytest if installed, with standalone reflection runner fallback.
- Standard Exit Codes: 0 on all tests passing, non-zero on test failures or errors.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Auto-switch to virtual environment Python if available and not currently active
if not os.environ.get("_RUNNER_REEXEC"):
    venv_python = PROJECT_ROOT / ".venv" / "bin" / "python"
    if venv_python.is_file() and sys.prefix != str(PROJECT_ROOT / ".venv"):
        import subprocess
        env = os.environ.copy()
        env["_RUNNER_REEXEC"] = "1"
        sys.exit(subprocess.call([str(venv_python), *sys.argv], env=env))

# Standalone shim for pytest if running in an environment without pytest
try:
    import pytest
except ImportError:
    import types
    pytest_shim = types.ModuleType("pytest")
    def _fixture(*args, **kwargs):
        def dec(f):
            return f
        if args and callable(args[0]):
            return args[0]
        return dec
    class _Raises:
        def __init__(self, exc):
            self.exc = exc
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc_val, exc_tb):
            return exc_type is not None and issubclass(exc_type, self.exc)
    pytest_shim.fixture = _fixture
    pytest_shim.raises = _Raises
    sys.modules["pytest"] = pytest_shim

TIER_FILE_MAP = {
    "1": "tests/e2e/test_tier1_feature_coverage.py",
    "2": "tests/e2e/test_tier2_boundaries.py",
    "3": "tests/e2e/test_tier3_pairwise.py",
    "4": "tests/e2e/test_tier4_real_world.py",
}


def run_tests_via_pytest(
    tier: str,
    verbose: bool = False,
    extra_args: Optional[List[str]] = None,
) -> Tuple[int, Dict[str, Any]]:
    """Execute tests via pytest library interface."""
    import pytest

    args = []
    if verbose:
        args.append("-v")
    else:
        args.append("-q")

    if extra_args:
        args.extend(extra_args)

    if tier == "all":
        target_files = list(TIER_FILE_MAP.values())
    else:
        if tier not in TIER_FILE_MAP:
            raise ValueError(f"Unknown tier: {tier}")
        target_files = [TIER_FILE_MAP[tier]]

    args.extend([str(PROJECT_ROOT / f) for f in target_files])

    class ResultCollector:
        def __init__(self):
            self.passed = 0
            self.failed = 0
            self.skipped = 0
            self.errors = []

        def pytest_runtest_logreport(self, report):
            if report.when == "call":
                if report.passed:
                    self.passed += 1
                elif report.failed:
                    self.failed += 1
                    self.errors.append({"test": report.nodeid, "error": str(report.longrepr)})
                elif report.skipped:
                    self.skipped += 1
            elif report.when == "setup" and report.failed:
                self.failed += 1
                self.errors.append({"test": report.nodeid, "error": str(report.longrepr)})

    collector = ResultCollector()
    start_time = time.time()
    exit_code = pytest.main(args, plugins=[collector])
    duration = time.time() - start_time
    total = collector.passed + collector.failed + collector.skipped

    return exit_code, {
        "engine": "pytest",
        "tier": tier,
        "target_files": target_files,
        "total": total,
        "passed": collector.passed,
        "failed": collector.failed,
        "skipped": collector.skipped,
        "duration_seconds": round(duration, 3),
        "exit_code": int(exit_code),
        "errors": collector.errors,
        "success": (exit_code == 0),
    }


def run_tests_standalone(
    tier: str,
    verbose: bool = False,
) -> Tuple[int, Dict[str, Any]]:
    """Fallback runner executing test functions via inspection when pytest is not available."""
    import inspect
    import socket
    import tempfile

    # Network isolation guard: strictly intercept and block all external network calls hermetically
    orig_connect = socket.socket.connect

    def guarded_connect(self, address):
        if isinstance(address, tuple) and len(address) >= 2:
            host, port = address[0], address[1]
            if host in ("127.0.0.1", "localhost", "::1", None):
                return orig_connect(self, address)
            raise RuntimeError(
                f"External network call forbidden in hermetic E2E test harness: {host}:{port}"
            )
        return orig_connect(self, address)

    socket.socket.connect = guarded_connect

    try:
        from tests.e2e.conftest import (
            persian_vtt_colloquial,
            persian_vtt_formal,
            persian_vtt_massive,
            persian_vtt_unicode_edge,
        )

        if tier == "all":
            tiers_to_run = ["1", "2", "3", "4"]
        else:
            tiers_to_run = [tier]

        passed = 0
        failed = 0
        errors = []
        start_time = time.time()

        formal_vtt = getattr(persian_vtt_formal, "__wrapped__", persian_vtt_formal)()
        colloquial_vtt = getattr(persian_vtt_colloquial, "__wrapped__", persian_vtt_colloquial)()
        unicode_vtt = getattr(persian_vtt_unicode_edge, "__wrapped__", persian_vtt_unicode_edge)()
        massive_vtt = getattr(persian_vtt_massive, "__wrapped__", persian_vtt_massive)()

        fixture_map = {
            "canonical_catalog_path": PROJECT_ROOT / "data" / "persian_channels_catalog.json",
            "schema_sql_path": PROJECT_ROOT / "src" / "vault" / "schema.sql",
            "persian_vtt_formal": formal_vtt,
            "persian_vtt_colloquial": colloquial_vtt,
            "persian_vtt_unicode_edge": unicode_vtt,
            "persian_vtt_massive": massive_vtt,
        }

        print(f"\n[RUNNER] Running standalone test suite for Tier {tier}...")

        for t_id in tiers_to_run:
            rel_path = TIER_FILE_MAP[t_id]
            module_name = rel_path.replace("/", ".").replace(".py", "")
            module = importlib.import_module(module_name)

            # Discover test functions
            test_funcs = [
                (name, func)
                for name, func in inspect.getmembers(module, inspect.isfunction)
                if name.startswith("test_")
            ]

            for name, func in test_funcs:
                # Prepare arguments matching function signature
                sig = inspect.signature(func)
                kwargs = {}
                temp_dirs_to_clean = []

                try:
                    for param in sig.parameters.values():
                        p_name = param.name
                        if p_name in fixture_map:
                            kwargs[p_name] = fixture_map[p_name]
                        elif p_name == "sandbox_staging_dir":
                            td = tempfile.mkdtemp(prefix="standalone_staging_")
                            temp_dirs_to_clean.append(td)
                            kwargs[p_name] = Path(td)
                        elif p_name == "sandbox_vault_path":
                            td = tempfile.mkdtemp(prefix="standalone_vault_")
                            temp_dirs_to_clean.append(td)
                            kwargs[p_name] = Path(td) / "raw_vault.db"

                    func(**kwargs)
                    passed += 1
                    if verbose:
                        print(f"  [PASS] {name}")
                    else:
                        sys.stdout.write(".")
                        sys.stdout.flush()
                except Exception as e:
                    failed += 1
                    errors.append({"test": name, "error": str(e)})
                    if verbose:
                        print(f"  [FAIL] {name}: {e}")
                    else:
                        sys.stdout.write("F")
                        sys.stdout.flush()
                finally:
                    for td in temp_dirs_to_clean:
                        import shutil
                        shutil.rmtree(td, ignore_errors=True)

    finally:
        socket.socket.connect = orig_connect

    print()
    duration = time.time() - start_time
    total = passed + failed
    exit_code = 0 if failed == 0 else 1

    return exit_code, {
        "engine": "standalone",
        "tier": tier,
        "total": total,
        "passed": passed,
        "failed": failed,
        "duration_seconds": round(duration, 3),
        "exit_code": exit_code,
        "errors": errors,
        "success": (exit_code == 0),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Run requirement-driven E2E test suites for Persian Channel Harvesting."
    )
    parser.add_argument(
        "--tier",
        choices=["1", "2", "3", "4", "all"],
        default="all",
        help="Test tier to execute: 1 (Feature Coverage), 2 (Boundaries), 3 (Pairwise), 4 (Real-World), all (Default).",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose output.",
    )
    parser.add_argument(
        "--json-report",
        type=str,
        default=None,
        help="Optional path to write JSON execution report.",
    )
    parser.add_argument(
        "--no-pytest",
        action="store_true",
        help="Force standalone reflection runner even if pytest is installed.",
    )

    args = parser.parse_args()

    use_pytest = False
    if not args.no_pytest:
        try:
            import pytest
            use_pytest = True
        except ImportError:
            use_pytest = False

    if use_pytest:
        exit_code, report_data = run_tests_via_pytest(tier=args.tier, verbose=args.verbose)
    else:
        exit_code, report_data = run_tests_standalone(tier=args.tier, verbose=args.verbose)

    report_data["timestamp"] = datetime.now(timezone.utc).isoformat()

    if args.json_report:
        report_path = Path(args.json_report)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report_data, f, indent=2)
        print(f"\n[REPORT] JSON report saved to: {report_path.resolve()}")

    if exit_code == 0:
        print(f"\n[SUMMARY] All tests in Tier '{args.tier}' PASSED ({report_data.get('duration_seconds')}s).")
    else:
        print(f"\n[SUMMARY] Tests in Tier '{args.tier}' FAILED with exit code {exit_code}.")

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
