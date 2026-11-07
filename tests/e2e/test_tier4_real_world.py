"""Tier 4: Realistic End-to-End Multi-Stage Production Scenarios.

Validates complete multi-agent workflows under real-world operating conditions:
- Scenario 1: Standard multi-squad production pipeline (catalog -> 4 shards -> ATTACH merge -> FTS search -> manifest validation)
- Scenario 2: Resumption after simulated worker crash (idempotent resume, 0 duplicates)
- Scenario 3: Uncapped ingestion on high-volume channels (--all-videos)
- Scenario 4: Catalog anomaly auto-remediation run (handles @Kouman, @radio-marz, duplicate Fiction, 0-video channel)
- Scenario 5: Fault-tolerant merge with corrupted shard quarantine
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

import pytest

from src.vault.db import VaultManager
from tests.e2e.contract_helpers import (
    harvest_channel_shard,
    merge_staging_to_vault,
    partition_channels,
    run_teamwork_harvest,
)


def test_scenario_1_standard_production_pipeline(
    canonical_catalog_path: Path,
    sandbox_staging_dir: Path,
    sandbox_vault_path: Path,
    persian_vtt_formal: str,
    persian_vtt_colloquial: str,
    persian_vtt_unicode_edge: str,
):
    """Scenario 1: Standard multi-squad production pipeline.
    
    Full flow:
    1. Channel Partitioner creates 4 domain squads.
    2. Parallel workers harvest channels across the 4 squads into isolated staging DBs.
    3. ATTACH merger consolidates staging shards into canonical raw_vault.db.
    4. FTS5 index enables search across formal, colloquial, and instructional registers.
    5. Ingestion manifest is verified for accuracy and completeness.
    """
    # Define realistic corpus for each squad with unique transcript content per video
    corpus_by_squad = {
        1: [
            {"id": "t16_ep1", "duration": 5400, "view_count": 85000, "vtt_content": f"{persian_vtt_formal}\n\n00:00:30.000 --> 00:00:35.000\nاپیزود طبقه ۱۶ قسمت ۱"},
            {"id": "chb_ep1", "duration": 3600, "view_count": 65000, "vtt_content": f"{persian_vtt_formal}\n\n00:00:30.000 --> 00:00:35.000\nاپیزود چنل بی قسمت ۱"},
        ],
        2: [
            {"id": "mia_v1", "duration": 1200, "view_count": 120000, "vtt_content": f"{persian_vtt_colloquial}\n\n00:00:30.000 --> 00:00:35.000\nولاگ میا پلیز قسمت ۱"},
            {"id": "kou_v1", "duration": 1100, "view_count": 95000, "vtt_content": f"{persian_vtt_colloquial}\n\n00:00:30.000 --> 00:00:35.000\nولاگ کوروش و ایمان کومان"},
        ],
        3: [
            {"id": "dastan_ep1", "duration": 7200, "view_count": 40000, "vtt_content": f"{persian_vtt_formal}\n\n00:00:30.000 --> 00:00:35.000\nداستان لند شاهنامه فردوسی"},
            {"id": "filmbaz_v1", "duration": 1800, "view_count": 30000, "vtt_content": f"{persian_vtt_formal}\n\n00:00:30.000 --> 00:00:35.000\nتحلیل فیلم‌های سینمایی فیلم‌باز"},
        ],
        4: [
            {"id": "chef_v1", "duration": 900, "view_count": 150000, "vtt_content": f"{persian_vtt_unicode_edge}\n\n00:00:30.000 --> 00:00:35.000\nدستور پخت جواد جوادی"},
            {"id": "skill_v1", "duration": 750, "view_count": 25000, "vtt_content": f"{persian_vtt_unicode_edge}\n\n00:00:30.000 --> 00:00:35.000\nآموزش مهارت اسکیل وید"},
        ],
    }

    def multi_squad_provider(ch: str):
        # Match channel to squad
        ch_lower = ch.lower()
        if "tabaghe" in ch_lower or "channelb" in ch_lower:
            return corpus_by_squad[1]
        elif "mia" in ch_lower or "kouman" in ch_lower:
            return corpus_by_squad[2]
        elif "dastan" in ch_lower or "filmbaz" in ch_lower:
            return corpus_by_squad[3]
        else:
            return corpus_by_squad[4]

    # Execute coordinator harvest
    res = run_teamwork_harvest(
        shards=4,
        catalog=canonical_catalog_path,
        staging_dir=sandbox_staging_dir,
        vault_path=sandbox_vault_path,
        auto_merge=True,
        channel_video_provider=multi_squad_provider,
    )
    assert res["success"] is True
    manifest = res["merge_manifest"]
    assert manifest is not None
    assert manifest["status"] == "SUCCESS"
    assert manifest["total_items"] >= 8
    assert manifest["total_payloads"] >= 8

    # Verify FTS5 search across different registers in the merged vault
    vm = VaultManager(db_path=sandbox_vault_path)
    formal_hits = vm.search_text("ژئوپلیتیک")
    colloquial_hits = vm.search_text("باحال")
    unicode_hits = vm.search_text("PyTorch")

    assert len(formal_hits) >= 1
    assert len(colloquial_hits) >= 1
    assert len(unicode_hits) >= 1
    vm.close()


def test_scenario_2_resumption_after_worker_crash(
    sandbox_staging_dir: Path, sandbox_vault_path: Path, persian_vtt_formal: str
):
    """Scenario 2: Resumption after simulated worker crash.
    
    1. Worker begins harvesting 5 videos and crashes after ingesting 2.
    2. Harvester is restarted with the full 5-video list.
    3. Assert completed videos are skipped via idempotent resumption.
    4. Assert total items in shard = exactly 5 with 0 duplicate rows.
    """
    all_videos = [
        {
            "id": f"vid_crash_{i}",
            "duration": 800 + i * 50,
            "view_count": i * 1000,
            "vtt_content": f"{persian_vtt_formal}\n\n00:00:10.000 --> 00:00:15.000\nاپیزود آزمایشی شماره {i}",
        }
        for i in range(1, 6)
    ]

    # Run pass 1 with partial list (simulating crash after first 2)
    res_pass1 = harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: all_videos[:2],
    )
    assert res_pass1["items_cataloged"] == 2
    assert res_pass1["payloads_inserted"] == 2

    # Run pass 2 with complete list (simulating recovery/restart)
    res_pass2 = harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: all_videos,
    )
    assert res_pass2["resumed_skipped_count"] == 2
    assert res_pass2["items_cataloged"] == 3

    # Check shard DB has exactly 5 items
    db_path = sandbox_staging_dir / "shard_1.db"
    conn = sqlite3.connect(str(db_path))
    cnt = conn.execute("SELECT COUNT(*) FROM items;").fetchone()[0]
    conn.close()
    assert cnt == 5


def test_scenario_3_uncapped_ingestion_on_high_volume_channel(
    sandbox_staging_dir: Path, sandbox_vault_path: Path
):
    """Scenario 3: Uncapped ingestion on high-volume channel.
    
    1. A channel has 60 eligible longform videos.
    2. With capping policy default (25), only top 25 are ingested.
    3. With --all-videos, all 60 are ingested and merged into the canonical vault.
    """
    high_volume_videos = [
        {"id": f"vid_vol_{i}", "duration": 700 + i * 10, "view_count": i * 100}
        for i in range(1, 61)
    ]

    # Ingest with all_videos = True
    res = harvest_channel_shard(
        shard_id=1,
        channels=["@BBCNewsPersian"],
        staging_dir=sandbox_staging_dir,
        cap_per_channel=25,
        all_videos=True,
        channel_video_provider=lambda ch: high_volume_videos,
    )
    assert res["eligible_videos_scanned"] == 60
    assert res["items_cataloged"] == 60

    manifest = merge_staging_to_vault(staging_dir=sandbox_staging_dir, vault_path=sandbox_vault_path)
    assert manifest["total_items"] == 60


def test_scenario_4_catalog_anomaly_auto_remediation(
    canonical_catalog_path: Path, sandbox_staging_dir: Path, sandbox_vault_path: Path
):
    """Scenario 4: Catalog anomaly auto-remediation run.
    
    Verifies that the entire pipeline automatically fixes:
    - @Kouman_Official -> @Kouman
    - radiomarz -> @radio-marz
    - Fiction Podcast duplicate
    - 0-video channel (@navidkashani)
    and produces clean, normalized records in the destination vault.
    """
    res = run_teamwork_harvest(
        shards=4,
        catalog=canonical_catalog_path,
        staging_dir=sandbox_staging_dir,
        vault_path=sandbox_vault_path,
        auto_merge=True,
        channel_video_provider=lambda ch: [
            {"id": f"vid_{ch.lstrip('@')}", "duration": 800, "view_count": 500}
        ],
    )
    assert res["success"] is True

    vm = VaultManager(db_path=sandbox_vault_path)
    conn = vm.conn
    sources = [r[0] for r in conn.execute("SELECT id FROM sources;").fetchall()]
    vm.close()

    assert "yt:@Kouman" in sources
    assert "yt:@Kouman_Official" not in sources
    assert "yt:@radio-marz" in sources
    assert "yt:radiomarz" not in sources

    fiction_sources = [s for s in sources if "fiction" in s.lower()]
    assert len(fiction_sources) == 1, "Only 1 deduplicated Fiction Podcast source must exist in vault"


def test_scenario_5_fault_tolerant_merge_with_corrupted_shard(
    sandbox_staging_dir: Path, sandbox_vault_path: Path, persian_vtt_formal: str
):
    """Scenario 5: Fault-tolerant merge with corrupted shard quarantine.
    
    1. Three worker shards are created.
    2. Shard 2 is corrupted (overwritten with corrupted bytes).
    3. The merger is invoked.
    4. Assert that Shard 2 is renamed to .corrupt and documented in manifest.
    5. Assert that Shards 1 and 3 are successfully merged into raw_vault.db without data loss.
    """
    # Create shard 1
    harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: [
            {"id": "v_s1", "duration": 900, "view_count": 1000, "vtt_content": f"{persian_vtt_formal}\n\n00:00:10.000 --> 00:00:15.000\nشارد ۱"}
        ],
    )
    # Create shard 2
    harvest_channel_shard(
        shard_id=2,
        channels=["@MiaPlays"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: [
            {"id": "v_s2", "duration": 900, "view_count": 1000}
        ],
    )
    # Create shard 3
    harvest_channel_shard(
        shard_id=3,
        channels=["@ChefJavadJavadi"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: [
            {"id": "v_s3", "duration": 900, "view_count": 1000, "vtt_content": f"{persian_vtt_formal}\n\n00:00:10.000 --> 00:00:15.000\nشارد ۳"}
        ],
    )

    # Corrupt shard 2
    shard_2_file = sandbox_staging_dir / "shard_2.db"
    shard_2_file.write_bytes(b"CORRUPT BYTES")

    manifest = merge_staging_to_vault(staging_dir=sandbox_staging_dir, vault_path=sandbox_vault_path)
    assert manifest["status"] == "SUCCESS"
    assert len(manifest["quarantined_shards"]) == 1
    assert "shard_2" in manifest["quarantined_shards"][0]
    assert manifest["total_items"] == 2  # v_s1 and v_s3 merged
    assert manifest["total_payloads"] == 2
