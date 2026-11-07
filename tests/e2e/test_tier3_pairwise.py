"""Tier 3: Pairwise Cross-Feature Interactions Test Suite.

Validates multi-feature interactions and compositional behavior:
- PW-01: Deduplication x Squad allocation
- PW-02: Duration filter x Capping policy (Shorts with high views excluded before capping)
- PW-03: Manual CC x Auto CC fallback (heterogeneous channel corpus)
- PW-04: In-memory VTT streaming x Resumption (skips re-fetching completed items)
- PW-05: Staging shards x Concurrency / parallel execution (zero lock contention)
- PW-06: Shard discovery x Corrupt shard quarantine x ATTACH merge
- PW-07: ATTACH merge x Deduplication (INSERT OR IGNORE across overlapping shards)
- PW-08: ATTACH merge x FTS5 trigger synchronization (BM25 availability)
- PW-09: Parallel execution x Signal and status management
- PW-10: Merger CLI x Target cloud storage integration
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, List

import pytest

from src.vault.db import VaultManager
from tests.e2e.contract_helpers import (
    harvest_channel_shard,
    merge_staging_to_vault,
    partition_channels,
    resolve_persian_subtitles,
    run_teamwork_harvest,
)


def test_pw01_deduplication_x_squad_allocation(canonical_catalog_path: Path):
    """PW-01: Deduplication x Squad Allocation.
    
    Verifies that deduplicating identical channels (Fiction Podcast) does not drop
    Squad 1 below its minimum threshold or duplicate entries in any shard.
    """
    res = partition_channels(catalog_path=canonical_catalog_path, num_shards=4, dedup=True)
    shards = res["shards"]
    squad_1 = shards[0]

    handles_s1 = [c["handle"].lower() for c in squad_1["channels"]]
    fiction_matches = [h for h in handles_s1 if "fiction" in h]
    assert len(fiction_matches) == 1, "Exactly 1 Fiction Podcast should remain in Squad 1"
    assert squad_1["channels_count"] >= 5, "Squad 1 must retain at least 5 channels"


def test_pw02_duration_filter_x_capping_policy(sandbox_staging_dir: Path):
    """PW-02: Duration filter x Capping policy.
    
    Viral YouTube Shorts (<60s) with 5,000,000 views must be filtered out BEFORE
    the capping policy selects the top 5 videos by view count.
    """
    videos = [
        # Viral Shorts (under 600s, huge view counts)
        {"id": "short_viral_1", "duration": 30, "view_count": 5000000},
        {"id": "short_viral_2", "duration": 45, "view_count": 4000000},
        {"id": "short_viral_3", "duration": 50, "view_count": 3000000},
        # Longform videos (duration >= 600s, moderate view counts)
        {"id": "long_1", "duration": 900, "view_count": 50000},
        {"id": "long_2", "duration": 1200, "view_count": 40000},
        {"id": "long_3", "duration": 1500, "view_count": 30000},
        {"id": "long_4", "duration": 1800, "view_count": 20000},
        {"id": "long_5", "duration": 2100, "view_count": 10000},
        {"id": "long_6", "duration": 2400, "view_count": 5000},
    ]

    res = harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        min_duration=600,
        cap_per_channel=5,
        channel_video_provider=lambda ch: videos,
    )
    assert res["eligible_videos_scanned"] == 5
    assert res["items_cataloged"] == 5

    db_path = sandbox_staging_dir / "shard_1.db"
    conn = sqlite3.connect(str(db_path))
    cataloged_ids = [r[0] for r in conn.execute("SELECT id FROM items;").fetchall()]
    conn.close()

    # None of the viral shorts should be present
    for short_id in ("yt:short_viral_1", "yt:short_viral_2", "yt:short_viral_3"):
        assert short_id not in cataloged_ids

    # Top 5 longform videos (50k down to 10k) must be present
    assert "yt:long_1" in cataloged_ids
    assert "yt:long_5" in cataloged_ids
    assert "yt:long_6" not in cataloged_ids, "6th longform video should be capped"


def test_pw03_manual_cc_x_auto_cc_fallback(sandbox_staging_dir: Path):
    """PW-03: Manual CC x Auto CC fallback in heterogeneous channel corpus.
    
    Verifies that a mix of manual-CC videos and auto-CC videos within the same channel
    are ingested with distinct, correct payload_type and origin_type provenance.
    """
    videos = [
        {
            "id": "vid_manual",
            "duration": 900,
            "view_count": 20000,
            "subtitles": {"fa": [{"ext": "vtt", "url": "http://mock/man.vtt"}]},
            "automatic_captions": {"fa": [{"ext": "vtt", "url": "http://mock/auto.vtt"}]},
            "vtt_content": "WEBVTT\n\n00:00:01.000 --> 00:00:02.000\nمتن زیرنویس دستی",
        },
        {
            "id": "vid_auto",
            "duration": 950,
            "view_count": 15000,
            "subtitles": {},
            "automatic_captions": {"fa": [{"ext": "vtt", "url": "http://mock/auto2.vtt"}]},
            "vtt_content": "WEBVTT\n\n00:00:01.000 --> 00:00:02.000\nمتن زیرنویس خودکار",
        },
    ]

    res = harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: videos,
    )
    assert res["manual_cc_count"] == 1
    assert res["auto_cc_count"] == 1

    db_path = sandbox_staging_dir / "shard_1.db"
    conn = sqlite3.connect(str(db_path))
    rows = dict(conn.execute("SELECT item_id, payload_type FROM payloads;").fetchall())
    conn.close()

    assert rows["yt:vid_manual"] == "manual_cc"
    assert rows["yt:vid_auto"] == "auto_cc"


def test_pw04_in_memory_vtt_streaming_x_resumption(sandbox_staging_dir: Path):
    """PW-04: In-memory VTT streaming x Resumption.
    
    Verifies that resuming a harvest skips HTTP/VTT streaming for already ingested items,
    ensuring 0 network overhead for resumed items.
    """
    fetch_counter = {"calls": 0}

    def counting_vtt_provider(v_id: str, url: str) -> str:
        fetch_counter["calls"] += 1
        return "WEBVTT\n\n00:00:01.000 --> 00:00:02.000\nمتن آزمایشی"

    videos = [
        {
            "id": "vid_stream_resume",
            "duration": 800,
            "view_count": 5000,
            "subtitles": {"fa": [{"ext": "vtt", "url": "http://mock/vtt"}]},
        }
    ]

    # Pass 1: Fresh harvest
    harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        vtt_provider=counting_vtt_provider,
        channel_video_provider=lambda ch: videos,
    )
    assert fetch_counter["calls"] == 1

    # Pass 2: Resume harvest
    harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        vtt_provider=counting_vtt_provider,
        channel_video_provider=lambda ch: videos,
    )
    # Counter must remain 1 because item was already present in shard
    assert fetch_counter["calls"] == 1, "VTT provider must not be invoked for already-completed items"


def test_pw05_staging_shards_x_concurrency(sandbox_staging_dir: Path):
    """PW-05: Staging shards x Concurrency / parallel execution.
    
    Verifies that 4 concurrent worker threads writing to separate staging shard databases
    execute without database locks or concurrency conflicts.
    """
    errors = []

    def run_worker(shard_id: int):
        try:
            videos = [
                {
                    "id": f"vid_s{shard_id}_{i}",
                    "duration": 700 + i * 10,
                    "view_count": 1000 * i,
                    "vtt_content": f"WEBVTT\n\n00:00:01.000 --> 00:00:02.000\nمتن شارد {shard_id} ویدیو {i}",
                }
                for i in range(1, 11)
            ]
            harvest_channel_shard(
                shard_id=shard_id,
                channels=[f"@Channel_{shard_id}"],
                staging_dir=sandbox_staging_dir,
                channel_video_provider=lambda ch: videos,
            )
        except Exception as e:
            errors.append(f"Shard {shard_id} failed: {e}")

    threads = [threading.Thread(target=run_worker, args=(i,)) for i in range(1, 5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0, f"Concurrent workers failed with errors: {errors}"
    for i in range(1, 5):
        db_path = sandbox_staging_dir / f"shard_{i}.db"
        assert db_path.is_file()
        conn = sqlite3.connect(str(db_path))
        items_count = conn.execute("SELECT COUNT(*) FROM items;").fetchone()[0]
        conn.close()
        assert items_count == 10


def test_pw06_shard_discovery_x_corrupt_quarantine_x_attach_merge(
    sandbox_staging_dir: Path, sandbox_vault_path: Path
):
    """PW-06: Shard discovery x Corrupt shard quarantine x ATTACH merge.
    
    Verifies that when staging contains 2 valid shards and 1 corrupt shard:
    1. Discovery detects all 3 shard files.
    2. Quick_check flags the corrupt shard and renames it to .corrupt.
    3. ATTACH merge consolidates only the 2 valid shards into the vault.
    """
    # Create valid shard 1 and shard 2
    harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: [{"id": "v_s1", "duration": 800, "view_count": 100}],
    )
    harvest_channel_shard(
        shard_id=2,
        channels=["@MiaPlays"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: [{"id": "v_s2", "duration": 850, "view_count": 200}],
    )

    # Create corrupted shard 3
    corrupt_shard = sandbox_staging_dir / "shard_3.db"
    corrupt_shard.write_bytes(b"INVALID TRUNCATED SQLITE FILE")

    manifest = merge_staging_to_vault(staging_dir=sandbox_staging_dir, vault_path=sandbox_vault_path)
    assert manifest["status"] == "SUCCESS"
    assert len(manifest["shards_processed"]) == 2
    assert len(manifest["quarantined_shards"]) == 1
    assert manifest["total_items"] == 2


def test_pw07_attach_merge_x_deduplication(
    sandbox_staging_dir: Path, sandbox_vault_path: Path, persian_vtt_formal: str
):
    """PW-07: ATTACH merge x Multi-table deduplication (INSERT OR IGNORE).
    
    Verifies that when the destination vault already contains records and shards contain
    overlapping records, the merge executes idempotently without duplicating sources, items, or payloads.
    """
    # Initialize vault with 1 item and payload
    vm = VaultManager(db_path=sandbox_vault_path)
    vm.init_db()
    vm.upsert_source(id="yt:@Tabaghe16", platform="youtube", language_iso1="fa", name="Tabaghe 16", url="http://t16")
    vm.upsert_item(id="yt:v_overlap", source_id="yt:@Tabaghe16", title="هم‌پوشانی", url="http://t16/v")
    vm.insert_payload(
        item_id="yt:v_overlap",
        payload_type="manual_cc",
        language_iso1="fa",
        origin_type="human_transcribed",
        format="vtt",
        raw_content=persian_vtt_formal,
    )
    vm.close()

    # Create shard containing the same item/payload plus 1 new item
    harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: [
            {"id": "v_overlap", "duration": 900, "view_count": 1000, "vtt_content": persian_vtt_formal},
            {"id": "v_new", "duration": 950, "view_count": 2000, "vtt_content": "WEBVTT\n\n00:00:01.000 --> 00:00:02.000\nجدید"},
        ],
    )

    manifest = merge_staging_to_vault(staging_dir=sandbox_staging_dir, vault_path=sandbox_vault_path)
    assert manifest["status"] == "SUCCESS"
    assert manifest["items_added"] == 1  # Only v_new added
    assert manifest["total_items"] == 2
    assert manifest["total_payloads"] == 2


def test_pw08_attach_merge_x_fts5_trigger_synchronization(
    sandbox_staging_dir: Path, sandbox_vault_path: Path, persian_vtt_formal: str
):
    """PW-08: ATTACH merge x FTS5 trigger synchronization.
    
    Verifies that rows inserted into payloads via ATTACH merge automatically synchronize
    into payloads_fts via payloads_ai triggers, enabling immediate BM25 search.
    """
    harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: [
            {"id": "v_fts_test", "duration": 1200, "view_count": 5000, "vtt_content": persian_vtt_formal}
        ],
    )

    merge_staging_to_vault(
        staging_dir=sandbox_staging_dir,
        vault_path=sandbox_vault_path,
        rebuild_fts=False,  # Rely strictly on payloads_ai trigger
    )

    vm = VaultManager(db_path=sandbox_vault_path)
    results = vm.search_text("ژئوپلیتیک")
    assert len(results) == 1
    assert "ژئوپلیتیک" in results[0]["snippet"]
    vm.close()


def test_pw09_parallel_execution_x_status_management(
    canonical_catalog_path: Path, sandbox_staging_dir: Path, sandbox_vault_path: Path
):
    """PW-09: Parallel execution x Coordinator status management.
    
    Verifies that coordinator tracks exit codes and reports comprehensive run metrics.
    """
    res = run_teamwork_harvest(
        shards=4,
        catalog=canonical_catalog_path,
        staging_dir=sandbox_staging_dir,
        vault_path=sandbox_vault_path,
        auto_merge=True,
        channel_video_provider=lambda ch: [
            {"id": f"vid_{ch}", "duration": 800, "view_count": 500}
        ],
    )
    assert res["success"] is True
    assert len(res["errors"]) == 0
    assert len(res["shard_results"]) == 4


def test_pw10_merger_cli_x_target_cloud_storage(
    sandbox_staging_dir: Path, sandbox_vault_path: Path
):
    """PW-10: Merger CLI x Target cloud storage integration.
    
    Verifies that target_gdrive=True correctly records cloud destination intent in manifest.
    """
    harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: [{"id": "v_gdrive", "duration": 800, "view_count": 100}],
    )

    manifest = merge_staging_to_vault(
        staging_dir=sandbox_staging_dir,
        vault_path=sandbox_vault_path,
        target_gdrive=True,
    )
    assert manifest["target_gdrive"] is True
    assert manifest["status"] == "SUCCESS"
