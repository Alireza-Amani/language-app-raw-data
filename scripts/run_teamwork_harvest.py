#!/usr/bin/env python3
"""Teamwork Orchestrator Runner: Scaled Multi-Agent Persian Channel Harvesting.

Coordinates end-to-end execution of:
1. Channel partitioning across balanced domain squads (scripts/partition_channels.py).
2. Parallel or sequential worker shard harvesting into isolated staging databases (scripts/harvest_channel_shard.py).
3. Consolidated, atomic merger into the canonical Raw Linguistic Vault (scripts/merge_staging_to_vault.py).
4. Optional cloud sync to Google Drive (language-app-raw-data).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.merge_staging_to_vault import merge_staging_to_vault
from scripts.partition_channels import load_catalog, partition_channels, save_partitions

# Configure logging
logger = logging.getLogger("run_teamwork_harvest")
if not logger.handlers:
    _handler = logging.StreamHandler(sys.stderr)
    _handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))
    logger.addHandler(_handler)
logger.setLevel(logging.INFO)

GDRIVE_FOLDER_ID = "1eQt8Rux2WoLSsTRs9nXxgRKbIYIEd2xg"


def run_shard_worker(
    shard_id: int,
    catalog_path: str,
    staging_dir: str,
    cap_per_channel: int,
    all_videos: bool,
    min_duration: int,
    workers: int,
    dry_run: bool,
    vault_path: str = "",
    live: bool = True,
) -> Tuple[int, int, float, str]:
    """Execute a single shard harvester in a subprocess.

    Returns:
        (shard_id, exit_code, elapsed_time_seconds, output_text)
    """
    t0 = time.time()
    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "harvest_channel_shard.py"),
        "--shard-id",
        str(shard_id),
        "--channels",
        str(Path(staging_dir) / "partitions.json"),
        "--catalog",
        str(catalog_path),
        "--staging-dir",
        str(staging_dir),
        "--cap-per-channel",
        str(cap_per_channel),
        "--min-duration",
        str(min_duration),
        "--workers",
        str(workers),
    ]
    if all_videos:
        cmd.append("--all-videos")
    if live:
        cmd.append("--live")
    if vault_path:
        cmd.extend(["--vault-path", str(vault_path)])
    if dry_run:
        cmd.append("--dry-run")

    logger.info(f"[*] Starting Shard {shard_id} worker...")
    res = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = time.time() - t0
    output = res.stdout + ("\n" + res.stderr if res.stderr else "")
    logger.info(f"[+] Shard {shard_id} worker completed in {elapsed:.2f}s (Exit code: {res.returncode})")
    return shard_id, res.returncode, elapsed, output


def sync_vault_to_gdrive(vault_path: Path, folder_id: str = GDRIVE_FOLDER_ID) -> bool:
    """Upload or update database file in Google Drive via gdrive CLI."""
    gdrive_bin = None
    for candidate in [
        "/tmp/antigravity/bin/gdrive",
        shutil.which("gdrive"),
        "/opt/antigravity/bin/gdrive",
    ]:
        if candidate and Path(candidate).is_file():
            gdrive_bin = str(candidate)
            break

    if not gdrive_bin:
        logger.warning("gdrive CLI binary not found. Skipping Google Drive sync.")
        return False

    logger.info(f"[*] Syncing vault to Google Drive folder ({folder_id})...")
    env = os.environ.copy()
    env["ANTIGRAVITY_APP_DATA_DIR"] = "/tmp/antigravity"
    env["HOME"] = str(Path.home())

    # Check if raw_vault.db already exists in the target Drive folder
    target_file_id = None
    ls_res = subprocess.run([gdrive_bin, "readonly", "ls", folder_id], capture_output=True, text=True, env=env)
    if ls_res.returncode == 0:
        for line in ls_res.stdout.splitlines():
            parts = line.split()
            if len(parts) >= 2 and parts[1] == vault_path.name:
                target_file_id = parts[0]
                break

    if target_file_id:
        cmd = [gdrive_bin, "mutate", "update", target_file_id, str(vault_path)]
    else:
        cmd = [gdrive_bin, "mutate", "upload", str(vault_path), "--parent", folder_id]

    res = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if res.returncode == 0:
        logger.info(f"[+] Successfully synced vault to Google Drive: {res.stdout.strip()}")
        return True
    else:
        logger.warning(f"[!] Google Drive sync failed: {res.stderr.strip()}")
        return False


def run_orchestration(
    shards: int = 4,
    catalog: str = "data/persian_channels_catalog.json",
    staging_dir: str = "data/staging",
    cap_per_channel: int = 25,
    all_videos: bool = False,
    min_duration: int = 600,
    workers_per_shard: int = 2,
    auto_merge: bool = True,
    target_gdrive: bool = False,
    vault_path: str = "data/vault/language-app-raw-data/raw_vault.db",
    clean_shards: bool = False,
    parallel: bool = True,
    dry_run: bool = False,
    sync_gdrive: bool = False,
    live: bool = True,
) -> Dict[str, Any]:
    """Execute complete multi-agent harvest orchestration."""
    start_time = time.time()
    staging_p = Path(staging_dir).resolve()
    staging_p.mkdir(parents=True, exist_ok=True)
    catalog_p = Path(catalog).resolve()
    vault_p = Path(vault_path).resolve()

    print("=" * 70)
    print("      TEAMWORK ORCHESTRATOR: SCALED PERSIAN HARVESTING PIPELINE")
    print("=" * 70)
    print(f"Catalog Path        : {catalog_p}")
    print(f"Target Vault        : {vault_p}")
    print(f"Staging Directory   : {staging_p}")
    print(f"Worker Shards       : {shards}")
    print(f"Cap Per Channel     : {'All Videos' if all_videos else cap_per_channel}")
    print(f"Duration Threshold  : >= {min_duration}s (>= 10 minutes)")
    print(f"Execution Mode      : {'Parallel' if parallel else 'Sequential'}")
    print(f"Auto-Merge Enabled  : {auto_merge}")
    print(f"Live Discovery      : {live}")
    print("-" * 70)

    # Step 1: Partition Channels
    logger.info("Step 1/3: Partitioning channels into balanced squads...")
    partitions_file = staging_p / "partitions.json"
    channels = load_catalog(catalog_p)
    partition_res = partition_channels(channels, shards_count=shards, balance_mode="domain")
    save_partitions(partition_res, partitions_file)
    total_channels = len(channels)
    print(f"[+] Successfully partitioned {total_channels} channels across {shards} squads.")

    # Step 2: Harvest Shards
    logger.info(f"Step 2/3: Launching {shards} worker shards...")
    shard_results = []
    if parallel and shards > 1:
        with ThreadPoolExecutor(max_workers=shards) as executor:
            futures = [
                executor.submit(
                    run_shard_worker,
                    shard_id=i,
                    catalog_path=str(catalog_p),
                    staging_dir=str(staging_p),
                    cap_per_channel=cap_per_channel,
                    all_videos=all_videos,
                    min_duration=min_duration,
                    workers=workers_per_shard,
                    dry_run=dry_run,
                    vault_path=str(vault_p),
                    live=live,
                )
                for i in range(1, shards + 1)
            ]
            for future in as_completed(futures):
                shard_results.append(future.result())
    else:
        for i in range(1, shards + 1):
            res = run_shard_worker(
                shard_id=i,
                catalog_path=str(catalog_p),
                staging_dir=str(staging_p),
                cap_per_channel=cap_per_channel,
                all_videos=all_videos,
                min_duration=min_duration,
                workers=workers_per_shard,
                dry_run=dry_run,
                vault_path=str(vault_p),
                live=live,
            )
            shard_results.append(res)

    shard_results.sort(key=lambda r: r[0])
    failed_shards = [r[0] for r in shard_results if r[1] != 0]

    print("\n--- Worker Shards Execution Summary ---")
    for s_id, code, el, out in shard_results:
        status_str = "SUCCESS" if code == 0 else f"FAILED (code {code})"
        print(f"  Shard {s_id}: {status_str:<18} | Time: {el:>6.2f}s")
    print("-" * 70)

    # Step 3: Atomic Merge
    manifest: Optional[Dict[str, Any]] = None
    if auto_merge and not dry_run:
        logger.info("Step 3/3: Merging isolated staging shards into canonical vault...")
        manifest = merge_staging_to_vault(
            staging_dir=staging_p,
            vault_path=vault_p,
            clean_shards=clean_shards,
            rebuild_fts=True,
            target_gdrive=target_gdrive,
        )
        print("\n--- Canonical Vault Ingestion Manifest ---")
        print(f"  Total Sources       : {manifest['total_sources']:,} (+{manifest['sources_added']})")
        print(f"  Total Items         : {manifest['total_items']:,} (+{manifest['items_added']})")
        print(f"  Total Payloads      : {manifest['total_payloads']:,} (+{manifest['payloads_added']})")
        print(f"  Total Persian Words : {manifest['total_persian_words']:,}")
        print(f"  Manifest Written To : {manifest.get('manifest_path', 'N/A')}")
        print("-" * 70)

        if sync_gdrive:
            sync_vault_to_gdrive(vault_p)
    else:
        logger.info("Auto-merge disabled or dry-run. Shards remain in staging directory.")

    total_elapsed = time.time() - start_time
    print(f"[+] Teamwork Orchestration finished in {total_elapsed:.2f} seconds.")
    print("=" * 70)

    return {
        "status": "SUCCESS" if not failed_shards else "PARTIAL_FAILURE",
        "total_elapsed": total_elapsed,
        "shards": shards,
        "failed_shards": failed_shards,
        "manifest": manifest,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Teamwork Orchestrator: Scaled Multi-Agent Persian Channel Harvesting."
    )
    parser.add_argument(
        "--shards",
        type=int,
        default=4,
        help="Number of worker shards / squads (default: 4)",
    )
    parser.add_argument(
        "--catalog",
        type=str,
        default="data/persian_channels_catalog.json",
        help="Path to Persian channels catalog JSON (default: data/persian_channels_catalog.json)",
    )
    parser.add_argument(
        "--staging-dir",
        type=str,
        default="data/staging",
        help="Directory for staging SQLite shard databases (default: data/staging)",
    )
    parser.add_argument(
        "--cap-per-channel",
        type=int,
        default=25,
        help="Maximum longform videos to harvest per channel (default: 25)",
    )
    parser.add_argument(
        "--all-videos",
        action="store_true",
        help="Bypass per-channel cap and harvest all eligible videos (default: False)",
    )
    parser.add_argument(
        "--min-duration",
        type=int,
        default=600,
        help="Minimum duration in seconds (default: 600s / 10 minutes)",
    )
    parser.add_argument(
        "--workers-per-shard",
        type=int,
        default=2,
        help="Worker concurrency threads inside each shard (default: 2)",
    )
    parser.add_argument(
        "--auto-merge",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Automatically merge staging shards into canonical vault (default: True)",
    )
    parser.add_argument(
        "--target-gdrive",
        action="store_true",
        help="Target Google Drive vault location and record in manifest (default: False)",
    )
    parser.add_argument(
        "--vault-path",
        type=str,
        default="data/vault/language-app-raw-data/raw_vault.db",
        help="Destination canonical raw_vault.db path (default: data/vault/language-app-raw-data/raw_vault.db)",
    )
    parser.add_argument(
        "--clean-shards",
        action="store_true",
        help="Clean up staging shard files after successful merge (default: False)",
    )
    parser.add_argument(
        "--parallel",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run worker shards in parallel (default: True)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate harvesting without writing to disk",
    )
    parser.add_argument(
        "--sync-gdrive",
        action="store_true",
        help="Upload/sync canonical vault to Google Drive after merger",
    )
    parser.add_argument(
        "--live",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Perform live flat playlist extraction from YouTube (default: True)",
    )

    args = parser.parse_args()
    try:
        res = run_orchestration(
            shards=args.shards,
            catalog=args.catalog,
            staging_dir=args.staging_dir,
            cap_per_channel=args.cap_per_channel,
            all_videos=args.all_videos,
            min_duration=args.min_duration,
            workers_per_shard=args.workers_per_shard,
            auto_merge=args.auto_merge,
            target_gdrive=args.target_gdrive,
            vault_path=args.vault_path,
            clean_shards=args.clean_shards,
            parallel=args.parallel,
            dry_run=args.dry_run,
            sync_gdrive=args.sync_gdrive,
            live=args.live,
        )
        return 0 if res["status"] == "SUCCESS" else 1
    except Exception as e:
        logger.error(f"Orchestration failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
