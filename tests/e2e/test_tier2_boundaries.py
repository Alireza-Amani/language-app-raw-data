"""Tier 2: Comprehensive Boundary and Corner Case Test Suite.

Validates robust handling of boundary conditions, edge cases, and adversarial input:
- duration = 599s (excluded)
- duration = 600s (accepted)
- duration = None (safely handled and excluded)
- empty catalog (`channels: []`)
- duplicate channels (deduplicated)
- 0-video channels (@navidkashani handled gracefully)
- HTTP timeouts during VTT streaming
- missing VTT tracks (item cataloged, 0 payload)
- malformed metadata (corrupt date, None view_count)
- corrupted staging DB (`quick_check` failure, quarantined)
- SQLite busy_timeout / database locked handling
- Persian Unicode edge cases (ZWNJ, RTL/LTR, diacritics, Arabic/Persian letters)
- Extremely large WebVTT file (> 10,000 cues)
- SQL injection resistance in video titles and metadata
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict

import pytest

from src.vault.db import VaultManager
from tests.e2e.contract_helpers import (
    harvest_channel_shard,
    merge_staging_to_vault,
    partition_channels,
    resolve_persian_subtitles,
)


def test_boundary_duration_599s_excluded(sandbox_staging_dir: Path):
    """B1: Video with duration = 599s (1 second below 600s threshold) is strictly excluded."""
    videos = [
        {"id": "vid_599s", "title": "ویدیو ۵۹۹ ثانیه‌ای", "duration": 599, "view_count": 50000}
    ]
    res = harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        min_duration=600,
        channel_video_provider=lambda ch: videos,
    )
    assert res["eligible_videos_scanned"] == 0
    assert res["items_cataloged"] == 0


def test_boundary_duration_600s_accepted(sandbox_staging_dir: Path):
    """B2: Video with duration = 600s (exact threshold) is accepted."""
    videos = [
        {"id": "vid_600s", "title": "ویدیو ۶۰۰ ثانیه‌ای", "duration": 600, "view_count": 50000}
    ]
    res = harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        min_duration=600,
        channel_video_provider=lambda ch: videos,
    )
    assert res["eligible_videos_scanned"] == 1
    assert res["items_cataloged"] == 1


def test_boundary_duration_none_safely_excluded(sandbox_staging_dir: Path):
    """B3: Video with duration = None (unstarted live stream or premiere) is excluded safely."""
    videos = [
        {"id": "vid_none_dur", "title": "لایو استریم شروع نشده", "duration": None, "view_count": 100}
    ]
    res = harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        min_duration=600,
        channel_video_provider=lambda ch: videos,
    )
    assert res["eligible_videos_scanned"] == 0
    assert res["items_cataloged"] == 0


def test_boundary_empty_catalog(sandbox_staging_dir: Path):
    """B4: Empty catalog (`channels: []`) produces clean partitions without crashing."""
    empty_cat_path = sandbox_staging_dir / "empty_catalog.json"
    with open(empty_cat_path, "w", encoding="utf-8") as f:
        json.dump({"generated_at_utc": "2026-11-06T00:00:00Z", "channels": []}, f)

    res = partition_channels(catalog_path=empty_cat_path, num_shards=4)
    assert res["total_channels"] == 0
    assert len(res["shards"]) == 4
    for shard in res["shards"]:
        assert shard["channels_count"] == 0


def test_boundary_duplicate_channels_in_catalog(sandbox_staging_dir: Path):
    """B5: Multiple duplicate channel entries in catalog are deduplicated to 1."""
    dup_cat_path = sandbox_staging_dir / "dup_catalog.json"
    dup_data = {
        "channels": [
            {"channel_title": "کانال تکراری ۱", "uploader_id": "@TestDup", "channel_id": "UC_TEST"},
            {"channel_title": "کانال تکراری ۲", "uploader_id": "@TestDup", "channel_id": "UC_TEST"},
            {"channel_title": "کانال تکراری ۳", "uploader_id": "TestDup", "channel_id": "UC_TEST"},
        ]
    }
    with open(dup_cat_path, "w", encoding="utf-8") as f:
        json.dump(dup_data, f)

    res = partition_channels(catalog_path=dup_cat_path, num_shards=4, dedup=True)
    all_handles = [c["handle"] for s in res["shards"] for c in s["channels"]]
    assert all_handles.count("@TestDup") == 1
    assert len(all_handles) == 1


def test_boundary_zero_video_channel_navidkashani(canonical_catalog_path: Path):
    """B6: 0-video channels (@navidkashani) are handled gracefully without exceptions."""
    res_default = partition_channels(canonical_catalog_path, filter_empty=True)
    all_handles = {c["handle"] for s in res_default["shards"] for c in s["channels"]}
    assert "@navidkashani" not in all_handles

    res_all = partition_channels(canonical_catalog_path, filter_empty=False)
    all_handles_unfiltered = {c["handle"] for s in res_all["shards"] for c in s["channels"]}
    assert "@navidkashani" in all_handles_unfiltered


def test_boundary_http_timeout_during_vtt_streaming(sandbox_staging_dir: Path):
    """B7: HTTP timeouts during VTT streaming are caught, cataloging item with 0 payloads."""
    videos = [
        {
            "id": "vid_timeout",
            "title": "ویدیوی با تایم‌اوت",
            "duration": 900,
            "view_count": 10000,
            "subtitles": {"fa": [{"ext": "vtt", "url": "https://timeout.local/vtt"}]},
        }
    ]

    def timeout_provider(v_id: str, url: str):
        raise TimeoutError("Socket read timed out after 25 seconds")

    res = harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: videos,
        vtt_provider=timeout_provider,
    )
    assert res["items_cataloged"] == 1
    assert res["payloads_inserted"] == 0


def test_boundary_missing_vtt_tracks(sandbox_staging_dir: Path):
    """B8: Video lacking Persian VTT tracks catalogs item with 0 payload rows."""
    videos = [
        {
            "id": "vid_no_tracks",
            "title": "ویدیوی بدون هیچ زیرنویس فارسی",
            "duration": 1200,
            "view_count": 5000,
            "subtitles": {"en": [{"ext": "vtt", "url": "http://en.vtt"}]},
            "automatic_captions": {},
        }
    ]
    res = harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: videos,
    )
    assert res["items_cataloged"] == 1
    assert res["payloads_inserted"] == 0


def test_boundary_malformed_metadata(sandbox_staging_dir: Path):
    """B9: Ingest video with malformed metadata (corrupt upload_date, None view_count)."""
    videos = [
        {
            "id": "vid_malformed_meta",
            "title": "ویدیو با متادیتای ناقص",
            "duration": 750,
            "view_count": None,
            "upload_date": "INVALID-DATE-9999",
        }
    ]
    res = harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: videos,
    )
    assert res["items_cataloged"] == 1
    db_path = sandbox_staging_dir / "shard_1.db"
    conn = sqlite3.connect(str(db_path))
    row = conn.execute("SELECT published_at, view_count FROM items WHERE id = 'yt:vid_malformed_meta';").fetchone()
    conn.close()
    assert row[0] == "INVALID-DATE-9999"
    assert row[1] is None


def test_boundary_corrupted_staging_db_quarantine(
    sandbox_staging_dir: Path, sandbox_vault_path: Path
):
    """B10: Corrupted staging DB failing quick_check is quarantined and excluded from merge."""
    # Create 1 valid shard
    harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: [{"id": "v_ok", "duration": 800, "view_count": 100}],
    )

    # Create 1 corrupt shard
    corrupt_shard = sandbox_staging_dir / "shard_corrupt.db"
    corrupt_shard.write_bytes(b"CORRUPT HEADER NON SQLITE CONTENT")

    manifest = merge_staging_to_vault(staging_dir=sandbox_staging_dir, vault_path=sandbox_vault_path)
    assert manifest["status"] == "SUCCESS"
    assert len(manifest["quarantined_shards"]) == 1
    assert "shard_corrupt" in manifest["quarantined_shards"][0]
    assert manifest["total_items"] == 1


def test_boundary_sqlite_busy_timeout_locked_handling(sandbox_vault_path: Path):
    """B11: SQLite busy_timeout handles concurrent write lock contention without crashing."""
    vm = VaultManager(db_path=sandbox_vault_path)
    vm.init_db()

    # Thread 1 holds an exclusive write lock for 0.4 seconds
    lock_acquired = threading.Event()

    def slow_writer():
        c = sqlite3.connect(str(sandbox_vault_path), timeout=5.0)
        c.execute("BEGIN EXCLUSIVE;")
        c.execute(
            "INSERT INTO sources (id, platform, language_iso1, name, url, created_at) "
            "VALUES ('s_slow', 'youtube', 'fa', 'S', 'U', 'now');"
        )
        lock_acquired.set()
        time.sleep(0.4)
        c.commit()
        c.close()

    t = threading.Thread(target=slow_writer)
    t.start()
    lock_acquired.wait(timeout=2.0)

    # Thread 2 attempts to write with busy_timeout = 3000ms
    c2 = sqlite3.connect(str(sandbox_vault_path), timeout=3.0)
    c2.execute("PRAGMA busy_timeout = 3000;")
    # This will wait ~0.4s until slow_writer commits, then successfully insert
    c2.execute(
        "INSERT INTO sources (id, platform, language_iso1, name, url, created_at) "
        "VALUES ('s_fast', 'youtube', 'fa', 'F', 'U', 'now');"
    )
    c2.commit()
    c2.close()

    t.join()

    # Both records are present
    conn = vm.conn
    cnt = conn.execute("SELECT COUNT(*) FROM sources;").fetchone()[0]
    assert cnt == 2
    vm.close()


def test_boundary_persian_unicode_edge_cases(sandbox_vault_path: Path, persian_vtt_unicode_edge: str):
    """B12: Persian Unicode edge cases (ZWNJ, digits, Arabic vs Persian chars, mixed RTL/LTR) stored and searched."""
    vm = VaultManager(db_path=sandbox_vault_path)
    vm.init_db()

    vm.upsert_source(id="s_unicode", platform="youtube", language_iso1="fa", name="یونیکد", url="http://uni")
    vm.upsert_item(id="i_unicode", source_id="s_unicode", title="تست نویسه‌های فارسی و عربی", url="http://uni/item")

    pid = vm.insert_payload(
        item_id="i_unicode",
        payload_type="manual_cc",
        language_iso1="fa",
        origin_type="human_transcribed",
        format="vtt",
        raw_content=persian_vtt_unicode_edge,
    )
    assert pid is not None

    # Search for English keyword in RTL text
    res_en = vm.search_text("PyTorch")
    assert len(res_en) == 1
    assert "PyTorch" in res_en[0]["snippet"]

    # Assert verb prefix with ZWNJ
    res_verb = vm.search_text("می‌خواهم")
    assert len(res_verb) == 1
    assert "می‌خواهم" in res_verb[0]["raw_content"]

    # Assert ezafe suffix with ZWNJ
    res_noun = vm.search_text("پروژه‌ی")
    assert len(res_noun) == 1
    assert "پروژه‌ی" in res_noun[0]["raw_content"]

    # Search for Persian digits
    res_digits = vm.search_text("۱۴۰۳")
    assert len(res_digits) == 1
    vm.close()


def test_boundary_extremely_large_webvtt(sandbox_vault_path: Path, persian_vtt_massive: str):
    """B13: Extremely large WebVTT file (> 10,000 cues) ingested and indexed into SQLite without error."""
    vm = VaultManager(db_path=sandbox_vault_path)
    vm.init_db()

    vm.upsert_source(id="s_big", platform="youtube", language_iso1="fa", name="کلان داده", url="http://big")
    vm.upsert_item(id="i_big", source_id="s_big", title="پادکست فوق‌العاده طولانی", url="http://big/item")

    pid = vm.insert_payload(
        item_id="i_big",
        payload_type="manual_cc",
        language_iso1="fa",
        origin_type="human_transcribed",
        format="vtt",
        raw_content=persian_vtt_massive,
    )
    assert pid is not None

    # Verify search across 10,000 cues works with FTS5
    res = vm.search_text("داده‌کاوی")
    assert len(res) == 1
    vm.close()


def test_boundary_sql_injection_resistance(sandbox_vault_path: Path):
    """B14: SQL injection attempts in titles, metadata, and handles are safely neutralized."""
    vm = VaultManager(db_path=sandbox_vault_path)
    vm.init_db()

    malicious_source = "yt:'; DROP TABLE items; --"
    malicious_title = "'; DELETE FROM sources WHERE '1'='1"

    vm.upsert_source(
        id=malicious_source,
        platform="youtube",
        language_iso1="fa",
        name="SQL Injection Test",
        url="http://injection.test",
    )
    vm.upsert_item(
        id="yt:injection_item",
        source_id=malicious_source,
        title=malicious_title,
        url="http://injection.test/item",
        metadata={"attack": "1' OR '1' = '1"},
    )

    # Tables must still exist and contain the records
    conn = vm.conn
    src = conn.execute("SELECT name FROM sources WHERE id = ?;", (malicious_source,)).fetchone()
    assert src is not None
    assert src[0] == "SQL Injection Test"

    itm = conn.execute("SELECT title FROM items WHERE id = 'yt:injection_item';").fetchone()
    assert itm is not None
    assert itm[0] == malicious_title

    # Table count check
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()}
    assert {"sources", "items", "payloads"}.issubset(tables)
    vm.close()
