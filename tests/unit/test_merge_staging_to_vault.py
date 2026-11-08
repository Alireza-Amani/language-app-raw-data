"""Comprehensive Unit Test Suite for scripts/merge_staging_to_vault.py.

Validates all 24 functional specifications:
1. Empty staging directory handling (status NO_SHARDS, 0 added)
2. Empty staging CLI execution (exit code 0)
3. Single valid shard merge with delta and total verification
4. Multi-shard disjoint merge (independent sources, items, payloads)
5. Multi-shard overlapping records deduplication via INSERT OR IGNORE
6. Idempotent repeated merge (second merge yields 0 added)
7. Foreign key referential integrity and ON DELETE CASCADE
8. FTS5 trigger synchronization on INSERT
9. FTS5 explicit index rebuild enabled
10. FTS5 rebuild disabled (--no-rebuild-fts)
11. Corrupted shard quarantine: 0-byte file
12. Corrupted shard quarantine: bad magic header
13. Corrupted shard quarantine: truncated SQLite file (< 100 bytes)
14. Corrupted shard quarantine: missing required tables
15. Corrupted shard sidecars unlinked (-wal, -shm)
16. Locked / busy shard distinguished from corruption (NOT quarantined)
17. Default manifest path generation (<staging_dir>/ingestion_manifest_<ts>.json)
18. Custom manifest output path (--manifest-out)
19. Comprehensive manifest schema verification (all 14 fields)
20. Aggregate Persian word count accuracy
21. Shard cleanup post-merge (--clean-shards)
22. Google Drive target flag (--target-gdrive)
23. CLI argument validation and exit codes (0, 1, 3)
24. Dynamic contract helpers delegation hook
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
import time
from typing import Any, Dict, List, Optional
from unittest.mock import patch

import pytest

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.merge_staging_to_vault import (
    MergerArgumentParser,
    check_shard_health,
    count_persian_words_in_vtt,
    main,
    merge_staging_to_vault,
    quarantine_corrupt_shard,
)
from src.vault.db import VaultManager
import tests.e2e.contract_helpers as contract_helpers


# =============================================================================
# Fixtures & Helpers
# =============================================================================

@pytest.fixture
def tmp_staging_dir(tmp_path: Path) -> Path:
    staging = tmp_path / "staging"
    staging.mkdir(parents=True, exist_ok=True)
    return staging


@pytest.fixture
def tmp_vault_path(tmp_path: Path) -> Path:
    vault_dir = tmp_path / "vault"
    vault_dir.mkdir(parents=True, exist_ok=True)
    return vault_dir / "raw_vault.db"


@pytest.fixture
def sample_persian_vtt() -> str:
    return (
        "WEBVTT\n"
        "Kind: captions\n"
        "Language: fa\n\n"
        "1\n"
        "00:00:01.000 --> 00:00:04.500\n"
        "سلام به مخاطبان گرامی، به برنامه ما خوش آمدید.\n\n"
        "2\n"
        "00:00:05.000 --> 00:00:09.000\n"
        "در این قسمت پیرامون هوش مصنوعی و یادگیری عمیق گفتگو می‌کنیم.\n"
    )


def create_shard_database(
    shard_path: Path,
    sources: List[Dict[str, Any]],
    items: List[Dict[str, Any]],
    payloads: List[Dict[str, Any]],
) -> Path:
    """Populate an isolated SQLite staging shard database matching the schema."""
    vm = VaultManager(db_path=shard_path)
    vm.init_db()
    conn = vm.conn
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")

    with vm.transaction():
        for s in sources:
            conn.execute(
                "INSERT INTO sources (id, platform, language_iso1, name, url, metadata_json, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?);",
                (
                    s["id"],
                    s.get("platform", "youtube"),
                    s.get("language_iso1", "fa"),
                    s.get("name", "Test Source"),
                    s.get("url", f"https://youtube.com/{s['id']}"),
                    json.dumps(s.get("metadata", {})),
                    s.get("created_at", "2026-11-06T12:00:00Z"),
                ),
            )

        for it in items:
            conn.execute(
                "INSERT INTO items (id, source_id, title, url, published_at, duration_seconds, view_count, metadata_json, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
                (
                    it["id"],
                    it["source_id"],
                    it.get("title", f"Title {it['id']}"),
                    it.get("url", f"https://youtube.com/watch?v={it['id']}"),
                    it.get("published_at", "2026-11-06T12:00:00Z"),
                    it.get("duration_seconds", 600),
                    it.get("view_count", 1000),
                    json.dumps(it.get("metadata", {})),
                    it.get("created_at", "2026-11-06T12:00:00Z"),
                ),
            )

        for p in payloads:
            raw_content = p["raw_content"]
            payload_id = p.get("id") or hashlib.sha256(raw_content.encode("utf-8")).hexdigest()
            conn.execute(
                "INSERT OR IGNORE INTO payloads (id, item_id, payload_type, language_iso1, origin_type, format, raw_content, fetched_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
                (
                    payload_id,
                    p["item_id"],
                    p.get("payload_type", "manual_cc"),
                    p.get("language_iso1", "fa"),
                    p.get("origin_type", "human_transcribed"),
                    p.get("format", "vtt"),
                    raw_content,
                    p.get("fetched_at", "2026-11-06T12:00:00Z"),
                ),
            )

    vm.close()
    return shard_path


# =============================================================================
# Unit Tests (1 to 24)
# =============================================================================

def test_01_empty_staging_directory(tmp_staging_dir: Path, tmp_vault_path: Path):
    """Test 1: Empty staging directory returns manifest with status NO_SHARDS and 0 added."""
    manifest = merge_staging_to_vault(
        staging_dir=tmp_staging_dir,
        vault_path=tmp_vault_path,
    )
    assert manifest["status"] == "NO_SHARDS"
    assert manifest["shards_processed"] == []
    assert manifest["quarantined_shards"] == []
    assert manifest["sources_added"] == 0
    assert manifest["items_added"] == 0
    assert manifest["payloads_added"] == 0
    assert manifest["total_sources"] == 0
    assert manifest["total_items"] == 0
    assert manifest["total_payloads"] == 0
    assert Path(manifest["manifest_path"]).is_file()


def test_02_empty_staging_cli_exit_zero(tmp_staging_dir: Path, tmp_vault_path: Path):
    """Test 2: CLI execution on empty staging directory exits with code 0."""
    argv = [
        "--staging-dir",
        str(tmp_staging_dir),
        "--vault-path",
        str(tmp_vault_path),
    ]
    exit_code = main(argv)
    assert exit_code == 0


def test_03_single_shard_merge(
    tmp_staging_dir: Path, tmp_vault_path: Path, sample_persian_vtt: str
):
    """Test 3: Single valid shard merges sources, items, payloads with accurate deltas."""
    shard_file = tmp_staging_dir / "shard_1.db"
    create_shard_database(
        shard_path=shard_file,
        sources=[{"id": "yt:@Tabaghe16", "name": "Tabaghe16"}],
        items=[
            {"id": "yt:video1", "source_id": "yt:@Tabaghe16"},
            {"id": "yt:video2", "source_id": "yt:@Tabaghe16"},
        ],
        payloads=[
            {"item_id": "yt:video1", "raw_content": f"{sample_persian_vtt}\n\nویدیو ۱"},
            {"item_id": "yt:video2", "raw_content": f"{sample_persian_vtt}\n\nویدیو ۲"},
        ],
    )

    manifest = merge_staging_to_vault(
        staging_dir=tmp_staging_dir,
        vault_path=tmp_vault_path,
    )
    assert manifest["status"] == "SUCCESS"
    assert len(manifest["shards_processed"]) == 1
    assert manifest["sources_added"] == 1
    assert manifest["items_added"] == 2
    assert manifest["payloads_added"] == 2
    assert manifest["total_sources"] == 1
    assert manifest["total_items"] == 2
    assert manifest["total_payloads"] == 2

    # Direct DB verification
    conn = sqlite3.connect(str(tmp_vault_path))
    assert conn.execute("SELECT COUNT(*) FROM sources;").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM items;").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM payloads;").fetchone()[0] == 2
    conn.close()


def test_04_multi_shard_disjoint_merge(
    tmp_staging_dir: Path, tmp_vault_path: Path, sample_persian_vtt: str
):
    """Test 4: Merging multiple disjoint shards aggregates all records into vault."""
    for idx in range(1, 4):
        create_shard_database(
            shard_path=tmp_staging_dir / f"shard_{idx}.db",
            sources=[{"id": f"yt:@Channel_{idx}", "name": f"Channel {idx}"}],
            items=[
                {"id": f"yt:v_{idx}_1", "source_id": f"yt:@Channel_{idx}"},
                {"id": f"yt:v_{idx}_2", "source_id": f"yt:@Channel_{idx}"},
            ],
            payloads=[
                {"item_id": f"yt:v_{idx}_1", "raw_content": f"{sample_persian_vtt}\n\nشارد {idx} ویدیو ۱"},
                {"item_id": f"yt:v_{idx}_2", "raw_content": f"{sample_persian_vtt}\n\nشارد {idx} ویدیو ۲"},
            ],
        )

    manifest = merge_staging_to_vault(
        staging_dir=tmp_staging_dir,
        vault_path=tmp_vault_path,
    )
    assert manifest["status"] == "SUCCESS"
    assert len(manifest["shards_processed"]) == 3
    assert manifest["total_sources"] == 3
    assert manifest["total_items"] == 6
    assert manifest["total_payloads"] == 6


def test_05_multi_shard_deduplication(
    tmp_staging_dir: Path, tmp_vault_path: Path, sample_persian_vtt: str
):
    """Test 5: Identical records across shards are cleanly deduplicated via INSERT OR IGNORE."""
    shared_source = [{"id": "yt:@SharedChannel", "name": "Shared"}]
    shared_items = [{"id": "yt:shared_v1", "source_id": "yt:@SharedChannel"}]
    shared_payloads = [{"item_id": "yt:shared_v1", "raw_content": sample_persian_vtt}]

    # Shard 1 has shared records
    create_shard_database(
        shard_path=tmp_staging_dir / "shard_1.db",
        sources=shared_source,
        items=shared_items,
        payloads=shared_payloads,
    )
    # Shard 2 has duplicate shared records + 1 new item with unique payload
    create_shard_database(
        shard_path=tmp_staging_dir / "shard_2.db",
        sources=shared_source,
        items=shared_items + [{"id": "yt:unique_v2", "source_id": "yt:@SharedChannel"}],
        payloads=shared_payloads + [{"item_id": "yt:unique_v2", "raw_content": f"{sample_persian_vtt}\n\nویدیو اختصاصی"}],
    )

    manifest = merge_staging_to_vault(
        staging_dir=tmp_staging_dir,
        vault_path=tmp_vault_path,
    )
    assert manifest["status"] == "SUCCESS"
    assert manifest["total_sources"] == 1
    assert manifest["total_items"] == 2
    assert manifest["total_payloads"] == 2  # 1 shared payload + 1 unique payload


def test_06_idempotent_repeated_merge(
    tmp_staging_dir: Path, tmp_vault_path: Path, sample_persian_vtt: str
):
    """Test 6: Running merge a second time yields 0 added records (idempotent)."""
    create_shard_database(
        shard_path=tmp_staging_dir / "shard_1.db",
        sources=[{"id": "yt:@ChannelB", "name": "ChannelB"}],
        items=[{"id": "yt:cb_01", "source_id": "yt:@ChannelB"}],
        payloads=[{"item_id": "yt:cb_01", "raw_content": sample_persian_vtt}],
    )

    manifest_1 = merge_staging_to_vault(
        staging_dir=tmp_staging_dir,
        vault_path=tmp_vault_path,
    )
    assert manifest_1["sources_added"] == 1
    assert manifest_1["items_added"] == 1

    manifest_2 = merge_staging_to_vault(
        staging_dir=tmp_staging_dir,
        vault_path=tmp_vault_path,
    )
    assert manifest_2["sources_added"] == 0
    assert manifest_2["items_added"] == 0
    assert manifest_2["payloads_added"] == 0
    assert manifest_2["total_sources"] == 1
    assert manifest_2["total_items"] == 1


def test_07_foreign_key_referential_integrity(
    tmp_staging_dir: Path, tmp_vault_path: Path, sample_persian_vtt: str
):
    """Test 7: Foreign key enforcement and CASCADE delete in canonical vault."""
    create_shard_database(
        shard_path=tmp_staging_dir / "shard_1.db",
        sources=[{"id": "yt:@FKTest", "name": "FK"}],
        items=[{"id": "yt:fk_item", "source_id": "yt:@FKTest"}],
        payloads=[{"item_id": "yt:fk_item", "raw_content": sample_persian_vtt}],
    )

    merge_staging_to_vault(staging_dir=tmp_staging_dir, vault_path=tmp_vault_path)

    conn = sqlite3.connect(str(tmp_vault_path))
    conn.execute("PRAGMA foreign_keys = ON;")
    violations = conn.execute("PRAGMA foreign_key_check;").fetchall()
    assert len(violations) == 0, f"Foreign key violations found: {violations}"

    # Deleting source cascades to items and payloads
    conn.execute("DELETE FROM sources WHERE id = 'yt:@FKTest';")
    conn.commit()
    assert conn.execute("SELECT COUNT(*) FROM items;").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM payloads;").fetchone()[0] == 0
    conn.close()


def test_08_fts5_triggers_insert_synchronization(
    tmp_staging_dir: Path, tmp_vault_path: Path
):
    """Test 8: Schema triggers synchronize payloads into payloads_fts during merge."""
    vtt = (
        "WEBVTT\n\n"
        "00:00:01.000 --> 00:00:05.000\n"
        "زبان فارسی و بررسی واژگان تخصصی زبان‌شناسی\n"
    )
    create_shard_database(
        shard_path=tmp_staging_dir / "shard_1.db",
        sources=[{"id": "yt:@FTS5Test", "name": "FTS5"}],
        items=[{"id": "yt:fts_item", "source_id": "yt:@FTS5Test"}],
        payloads=[{"item_id": "yt:fts_item", "raw_content": vtt}],
    )

    merge_staging_to_vault(staging_dir=tmp_staging_dir, vault_path=tmp_vault_path)

    conn = sqlite3.connect(str(tmp_vault_path))
    res = conn.execute(
        "SELECT id, item_id FROM payloads_fts WHERE payloads_fts MATCH 'واژگان';"
    ).fetchall()
    conn.close()
    assert len(res) == 1
    assert res[0][1] == "yt:fts_item"


def test_09_fts5_rebuild_enabled(
    tmp_staging_dir: Path, tmp_vault_path: Path, sample_persian_vtt: str
):
    """Test 9: Explicit FTS5 rebuild executes without error when rebuild_fts=True."""
    create_shard_database(
        shard_path=tmp_staging_dir / "shard_1.db",
        sources=[{"id": "yt:@RebuildFTS", "name": "Rebuild"}],
        items=[{"id": "yt:rebuild_item", "source_id": "yt:@RebuildFTS"}],
        payloads=[{"item_id": "yt:rebuild_item", "raw_content": sample_persian_vtt}],
    )

    manifest = merge_staging_to_vault(
        staging_dir=tmp_staging_dir,
        vault_path=tmp_vault_path,
        rebuild_fts=True,
    )
    assert manifest["status"] == "SUCCESS"

    conn = sqlite3.connect(str(tmp_vault_path))
    matches = conn.execute(
        "SELECT count(*) FROM payloads_fts WHERE payloads_fts MATCH 'مخاطبان';"
    ).fetchone()[0]
    conn.close()
    assert matches >= 1


def test_10_fts5_rebuild_disabled(
    tmp_staging_dir: Path, tmp_vault_path: Path, sample_persian_vtt: str
):
    """Test 10: Merging with rebuild_fts=False bypasses explicit rebuild command."""
    create_shard_database(
        shard_path=tmp_staging_dir / "shard_1.db",
        sources=[{"id": "yt:@NoRebuild", "name": "NoRebuild"}],
        items=[{"id": "yt:nr_item", "source_id": "yt:@NoRebuild"}],
        payloads=[{"item_id": "yt:nr_item", "raw_content": sample_persian_vtt}],
    )

    manifest = merge_staging_to_vault(
        staging_dir=tmp_staging_dir,
        vault_path=tmp_vault_path,
        rebuild_fts=False,
    )
    assert manifest["status"] == "SUCCESS"

    # Triggers still index the text
    conn = sqlite3.connect(str(tmp_vault_path))
    matches = conn.execute(
        "SELECT count(*) FROM payloads_fts WHERE payloads_fts MATCH 'مخاطبان';"
    ).fetchone()[0]
    conn.close()
    assert matches >= 1


def test_11_corrupt_shard_zero_byte(
    tmp_staging_dir: Path, tmp_vault_path: Path, sample_persian_vtt: str
):
    """Test 11: 0-byte shard database file is quarantined and valid shards still merge."""
    # Valid shard 1
    create_shard_database(
        shard_path=tmp_staging_dir / "shard_1.db",
        sources=[{"id": "yt:@ValidOne", "name": "Valid"}],
        items=[{"id": "yt:v1", "source_id": "yt:@ValidOne"}],
        payloads=[{"item_id": "yt:v1", "raw_content": sample_persian_vtt}],
    )
    # Corrupt 0-byte shard 2
    corrupt_shard = tmp_staging_dir / "shard_2.db"
    corrupt_shard.write_bytes(b"")

    manifest = merge_staging_to_vault(staging_dir=tmp_staging_dir, vault_path=tmp_vault_path)
    assert manifest["status"] == "SUCCESS"
    assert len(manifest["quarantined_shards"]) == 1
    assert "shard_2.corrupt" in manifest["quarantined_shards"][0]
    assert manifest["total_items"] == 1
    assert not corrupt_shard.exists()
    assert (tmp_staging_dir / "shard_2.corrupt").exists()


def test_12_corrupt_shard_bad_magic_header(
    tmp_staging_dir: Path, tmp_vault_path: Path, sample_persian_vtt: str
):
    """Test 12: Shard with invalid SQLite magic header is quarantined."""
    create_shard_database(
        shard_path=tmp_staging_dir / "shard_1.db",
        sources=[{"id": "yt:@ValidOne", "name": "Valid"}],
        items=[{"id": "yt:v1", "source_id": "yt:@ValidOne"}],
        payloads=[{"item_id": "yt:v1", "raw_content": sample_persian_vtt}],
    )

    bad_header_shard = tmp_staging_dir / "shard_bad_header.db"
    bad_header_shard.write_bytes(b"THIS IS NOT A VALID SQLITE HEADER" * 10)

    manifest = merge_staging_to_vault(staging_dir=tmp_staging_dir, vault_path=tmp_vault_path)
    assert len(manifest["quarantined_shards"]) == 1
    assert "shard_bad_header" in manifest["quarantined_shards"][0]
    assert manifest["total_items"] == 1


def test_13_corrupt_shard_truncated_sqlite(
    tmp_staging_dir: Path, tmp_vault_path: Path, sample_persian_vtt: str
):
    """Test 13: Shard file starting with SQLite header but < 100 bytes is quarantined."""
    create_shard_database(
        shard_path=tmp_staging_dir / "shard_1.db",
        sources=[{"id": "yt:@ValidOne", "name": "Valid"}],
        items=[{"id": "yt:v1", "source_id": "yt:@ValidOne"}],
        payloads=[{"item_id": "yt:v1", "raw_content": sample_persian_vtt}],
    )

    truncated_shard = tmp_staging_dir / "shard_truncated.db"
    truncated_shard.write_bytes(b"SQLite format 3\x00_TRUNCATED_CONTENT")
    assert truncated_shard.stat().st_size < 100

    manifest = merge_staging_to_vault(staging_dir=tmp_staging_dir, vault_path=tmp_vault_path)
    assert len(manifest["quarantined_shards"]) == 1
    assert "shard_truncated" in manifest["quarantined_shards"][0]


def test_14_corrupt_shard_missing_required_tables(
    tmp_staging_dir: Path, tmp_vault_path: Path, sample_persian_vtt: str
):
    """Test 14: Valid SQLite file missing required tables is flagged as corrupt."""
    create_shard_database(
        shard_path=tmp_staging_dir / "shard_1.db",
        sources=[{"id": "yt:@ValidOne", "name": "Valid"}],
        items=[{"id": "yt:v1", "source_id": "yt:@ValidOne"}],
        payloads=[{"item_id": "yt:v1", "raw_content": sample_persian_vtt}],
    )

    missing_tables_shard = tmp_staging_dir / "shard_missing_tables.db"
    c = sqlite3.connect(str(missing_tables_shard))
    c.execute("CREATE TABLE dummy (id INTEGER PRIMARY KEY);")
    c.commit()
    c.close()

    manifest = merge_staging_to_vault(staging_dir=tmp_staging_dir, vault_path=tmp_vault_path)
    assert len(manifest["quarantined_shards"]) == 1
    assert "shard_missing_tables" in manifest["quarantined_shards"][0]


def test_15_corrupt_shard_sidecars_unlinked(tmp_staging_dir: Path, tmp_vault_path: Path):
    """Test 15: Quarantining a corrupt shard unlinks any -wal and -shm sidecar files."""
    corrupt_shard = tmp_staging_dir / "shard_sidecars.db"
    corrupt_shard.write_bytes(b"CORRUPT DATA" * 10)

    wal_file = tmp_staging_dir / "shard_sidecars.db-wal"
    shm_file = tmp_staging_dir / "shard_sidecars.db-shm"
    wal_file.write_bytes(b"wal content")
    shm_file.write_bytes(b"shm content")

    quarantine_corrupt_shard(corrupt_shard)

    assert not corrupt_shard.exists()
    assert (tmp_staging_dir / "shard_sidecars.corrupt").exists()
    assert not wal_file.exists(), "Sidecar WAL file must be unlinked"
    assert not shm_file.exists(), "Sidecar SHM file must be unlinked"


def test_16_locked_shard_not_quarantined(
    tmp_staging_dir: Path, tmp_vault_path: Path, sample_persian_vtt: str
):
    """Test 16: Active database lock / contention does NOT trigger quarantine."""
    shard_file = tmp_staging_dir / "shard_locked.db"
    create_shard_database(
        shard_path=shard_file,
        sources=[{"id": "yt:@Locked", "name": "Locked"}],
        items=[{"id": "yt:l1", "source_id": "yt:@Locked"}],
        payloads=[{"item_id": "yt:l1", "raw_content": sample_persian_vtt}],
    )

    lock_acquired = threading.Event()
    release_lock = threading.Event()

    def hold_exclusive_lock():
        conn = sqlite3.connect(str(shard_file), timeout=5.0)
        conn.execute("PRAGMA journal_mode = DELETE;")
        conn.execute("BEGIN EXCLUSIVE;")
        lock_acquired.set()
        release_lock.wait()
        conn.rollback()
        conn.close()

    t = threading.Thread(target=hold_exclusive_lock)
    t.start()
    assert lock_acquired.wait(timeout=3.0)

    try:
        # check_shard_health on locked db should report (is_valid=False, is_corrupt=False)
        is_valid, is_corrupt = check_shard_health(shard_file)
        assert is_valid is False
        assert is_corrupt is False, "Active lock contention must NOT be flagged as corrupt"
        assert shard_file.exists(), "Locked shard must not be renamed or removed"
    finally:
        release_lock.set()
        t.join()


def test_17_manifest_default_path_generation(
    tmp_staging_dir: Path, tmp_vault_path: Path, sample_persian_vtt: str
):
    """Test 17: Default manifest output path matches <staging_dir>/ingestion_manifest_<timestamp>.json."""
    create_shard_database(
        shard_path=tmp_staging_dir / "shard_1.db",
        sources=[{"id": "yt:@DefaultManifest", "name": "DefaultManifest"}],
        items=[{"id": "yt:dm1", "source_id": "yt:@DefaultManifest"}],
        payloads=[{"item_id": "yt:dm1", "raw_content": sample_persian_vtt}],
    )

    manifest = merge_staging_to_vault(
        staging_dir=tmp_staging_dir,
        vault_path=tmp_vault_path,
    )
    manifest_p = Path(manifest["manifest_path"])
    assert manifest_p.is_file()
    assert manifest_p.parent == tmp_staging_dir
    assert manifest_p.name.startswith("ingestion_manifest_")
    assert manifest_p.name.endswith(".json")


def test_18_manifest_custom_path(
    tmp_staging_dir: Path, tmp_vault_path: Path, sample_persian_vtt: str
):
    """Test 18: Flag --manifest-out saves manifest to specified custom destination."""
    create_shard_database(
        shard_path=tmp_staging_dir / "shard_1.db",
        sources=[{"id": "yt:@CustomPath", "name": "CustomPath"}],
        items=[{"id": "yt:cp1", "source_id": "yt:@CustomPath"}],
        payloads=[{"item_id": "yt:cp1", "raw_content": sample_persian_vtt}],
    )

    custom_out = tmp_staging_dir / "nested" / "audit_manifest.json"
    manifest = merge_staging_to_vault(
        staging_dir=tmp_staging_dir,
        vault_path=tmp_vault_path,
        manifest_out=custom_out,
    )
    assert custom_out.is_file()
    assert manifest["manifest_path"] == str(custom_out)


def test_19_manifest_all_fields_present(
    tmp_staging_dir: Path, tmp_vault_path: Path, sample_persian_vtt: str
):
    """Test 19: Generated manifest dictionary strictly contains all 14 required fields."""
    create_shard_database(
        shard_path=tmp_staging_dir / "shard_1.db",
        sources=[{"id": "yt:@FieldCheck", "name": "FieldCheck"}],
        items=[{"id": "yt:fc1", "source_id": "yt:@FieldCheck"}],
        payloads=[{"item_id": "yt:fc1", "raw_content": sample_persian_vtt}],
    )

    manifest = merge_staging_to_vault(
        staging_dir=tmp_staging_dir,
        vault_path=tmp_vault_path,
    )

    expected_keys = {
        "timestamp",
        "status",
        "vault_path",
        "shards_processed",
        "quarantined_shards",
        "sources_added",
        "items_added",
        "payloads_added",
        "total_sources",
        "total_items",
        "total_payloads",
        "total_persian_words",
        "target_gdrive",
        "manifest_path",
    }
    assert expected_keys.issubset(manifest.keys())


def test_20_persian_word_count_calculation(tmp_staging_dir: Path, tmp_vault_path: Path):
    """Test 20: Manifest computes total aggregate Persian word count across all payloads."""
    # VTT with exactly 7 Persian words
    vtt = (
        "WEBVTT\n\n"
        "00:00:01.000 --> 00:00:05.000\n"
        "یک دو سه چهار پنج شش هفت\n"
    )
    create_shard_database(
        shard_path=tmp_staging_dir / "shard_1.db",
        sources=[{"id": "yt:@WordCount", "name": "WordCount"}],
        items=[{"id": "yt:wc1", "source_id": "yt:@WordCount"}],
        payloads=[{"item_id": "yt:wc1", "raw_content": vtt}],
    )

    manifest = merge_staging_to_vault(
        staging_dir=tmp_staging_dir,
        vault_path=tmp_vault_path,
    )
    assert manifest["total_persian_words"] == 7


def test_21_clean_shards_option(
    tmp_staging_dir: Path, tmp_vault_path: Path, sample_persian_vtt: str
):
    """Test 21: Flag clean_shards=True deletes successfully merged shards, preserves corrupt."""
    valid_shard = tmp_staging_dir / "shard_1.db"
    create_shard_database(
        shard_path=valid_shard,
        sources=[{"id": "yt:@CleanTest", "name": "CleanTest"}],
        items=[{"id": "yt:ct1", "source_id": "yt:@CleanTest"}],
        payloads=[{"item_id": "yt:ct1", "raw_content": sample_persian_vtt}],
    )

    corrupt_shard = tmp_staging_dir / "shard_2.db"
    corrupt_shard.write_bytes(b"CORRUPT")

    manifest = merge_staging_to_vault(
        staging_dir=tmp_staging_dir,
        vault_path=tmp_vault_path,
        clean_shards=True,
    )
    assert manifest["status"] == "SUCCESS"
    # Merged valid shard is removed
    assert not valid_shard.exists(), "Successfully merged shard must be deleted when clean_shards=True"
    # Quarantined shard file is retained
    assert (tmp_staging_dir / "shard_2.corrupt").exists(), "Quarantined shard must not be deleted"


def test_22_target_gdrive_option(
    tmp_staging_dir: Path, tmp_vault_path: Path, sample_persian_vtt: str
):
    """Test 22: Flag target_gdrive=True records cloud storage destination intent in manifest."""
    create_shard_database(
        shard_path=tmp_staging_dir / "shard_1.db",
        sources=[{"id": "yt:@GDriveTest", "name": "GDriveTest"}],
        items=[{"id": "yt:gd1", "source_id": "yt:@GDriveTest"}],
        payloads=[{"item_id": "yt:gd1", "raw_content": sample_persian_vtt}],
    )

    manifest = merge_staging_to_vault(
        staging_dir=tmp_staging_dir,
        vault_path=tmp_vault_path,
        target_gdrive=True,
    )
    assert manifest["target_gdrive"] is True


def test_23_cli_exit_codes(tmp_staging_dir: Path, tmp_vault_path: Path):
    """Test 23: CLI exit code handling (0 for success, 3 for bad args, 1 for fatal error)."""
    # 1. Exit code 0 on success
    code = main(["--staging-dir", str(tmp_staging_dir), "--vault-path", str(tmp_vault_path)])
    assert code == 0

    # 2. Exit code 3 on argument parsing error
    with pytest.raises(SystemExit) as exc_info:
        main(["--unrecognized-option"])
    assert exc_info.value.code == 3

    # 3. Exit code 1 on fatal execution error (e.g. uncreatable vault directory)
    with patch("scripts.merge_staging_to_vault.merge_staging_to_vault", side_effect=RuntimeError("Fatal DB error")):
        fatal_code = main(["--staging-dir", str(tmp_staging_dir)])
        assert fatal_code == 1


def test_24_contract_helpers_delegation(
    tmp_staging_dir: Path, tmp_vault_path: Path, sample_persian_vtt: str
):
    """Test 24: contract_helpers.merge_staging_to_vault delegates to scripts.merge_staging_to_vault."""
    create_shard_database(
        shard_path=tmp_staging_dir / "shard_1.db",
        sources=[{"id": "yt:@Delegation", "name": "Delegation"}],
        items=[{"id": "yt:del1", "source_id": "yt:@Delegation"}],
        payloads=[{"item_id": "yt:del1", "raw_content": sample_persian_vtt}],
    )

    # Invoke via contract_helpers
    manifest = contract_helpers.merge_staging_to_vault(
        staging_dir=tmp_staging_dir,
        vault_path=tmp_vault_path,
    )
    assert manifest["status"] == "SUCCESS"
    assert manifest["total_items"] == 1
    assert "manifest_path" in manifest


# =============================================================================
# French Tri-Source Unit Tests (25 to 28)
# =============================================================================

def _populate_french_tri_source_shards(staging_dir: Path) -> Dict[str, Path]:
    """Helper to create realistic temporary shards for French Stream A, B, and C."""
    # Shard Stream A: Pre-2022 YouTube Comments (JSON)
    shard_a = staging_dir / "shard_stream_a.db"
    comments_json = json.dumps([
        {
            "comment_id": "comment_1",
            "text": "Excellente analyse syntaxique du français parlé !",
            "timestamp": 1600000000,  # September 2020 (< 1640995200)
            "author": "linguiste_fr",
        },
        {
            "comment_id": "comment_2",
            "text": "Tout à fait d'accord avec cette explication dialectale.",
            "timestamp": 1620000000,  # May 2021 (< 1640995200)
            "author": "francophone_ca",
        },
    ], ensure_ascii=False)

    vm_a = VaultManager(db_path=shard_a)
    vm_a.init_db()
    with vm_a.transaction():
        vm_a.upsert_source(
            id="yt:@FrenchGrammar",
            platform="youtube",
            language_iso1="fr",
            name="French Grammar",
            url="https://youtube.com/@FrenchGrammar",
            country_code="FR",
        )
        vm_a.upsert_item(
            id="yt_comment_vid_fr_01",
            source_id="yt:@FrenchGrammar",
            title="Les temps simples en français",
            url="https://youtube.com/watch?v=vid_fr_01",
        )
        vm_a.insert_payload(
            item_id="yt_comment_vid_fr_01",
            payload_type="comment_thread",
            language_iso1="fr",
            origin_type="human_written",
            format="json",
            raw_content=comments_json,
            locale="fr-FR",
        )
    vm_a.close()

    # Shard Stream B: Cinema Subtitles (.srt)
    shard_b = staging_dir / "shard_stream_b.db"
    srt_content = (
        "1\n00:00:01,000 --> 00:00:04,000\nC'est à moi que tu parles ?\n\n"
        "2\n00:00:05,000 --> 00:00:08,000\nJusqu'ici tout va bien dans cette banlieue.\n"
    )
    vm_b = VaultManager(db_path=shard_b)
    vm_b.init_db()
    with vm_b.transaction():
        vm_b.upsert_source(
            id="sub:la_haine_1995",
            platform="subtitle",
            language_iso1="fr",
            name="La Haine (1995)",
            url="https://opensubtitles.org/sub/3283706",
            country_code="FR",
        )
        vm_b.upsert_item(
            id="sub_opensubtitles_3283706",
            source_id="sub:la_haine_1995",
            title="La Haine - Mathieu Kassovitz",
            url="https://opensubtitles.org/sub/3283706",
        )
        vm_b.insert_payload(
            item_id="sub_opensubtitles_3283706",
            payload_type="manual_cc",
            language_iso1="fr",
            origin_type="human_transcribed",
            format="srt",
            raw_content=srt_content,
            locale="fr-FR",
        )
    vm_b.close()

    # Shard Stream C: Parliamentary Hansard Debates (.xml)
    shard_c = staging_dir / "shard_stream_c.db"
    hansard_xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<Hansard language="fr">\n'
        '  <Debate>\n'
        '    <Speech speaker="M. Dupont">\n'
        '      Monsieur le Président, nous abordons aujourd\'hui la question primordiale de la politique linguistique.\n'
        '    </Speech>\n'
        '  </Debate>\n'
        '</Hansard>'
    )
    vm_c = VaultManager(db_path=shard_c)
    vm_c.init_db()
    with vm_c.transaction():
        vm_c.upsert_source(
            id="web:ca_commons",
            platform="web",
            language_iso1="fr",
            name="Chambre des communes du Canada",
            url="https://ourcommons.ca",
            country_code="CA",
        )
        vm_c.upsert_item(
            id="ca_commons_432_han100",
            source_id="web:ca_commons",
            title="Débats parlementaires séance 100",
            url="https://ourcommons.ca/hansard/100",
        )
        vm_c.insert_payload(
            item_id="ca_commons_432_han100",
            payload_type="parliament_transcript",
            language_iso1="fr",
            origin_type="human_transcribed",
            format="xml",
            raw_content=hansard_xml,
            locale="fr-CA",
        )
    vm_c.close()

    return {"a": shard_a, "b": shard_b, "c": shard_c}


def test_25_french_tri_source_merge_simulation(tmp_staging_dir: Path, tmp_vault_path: Path):
    """Test 25: Simulated merge of French Tri-Source shards (Streams A, B, C) with full verification."""
    _populate_french_tri_source_shards(tmp_staging_dir)
    custom_manifest = tmp_staging_dir / "ingestion_manifest.json"

    manifest = merge_staging_to_vault(
        staging_dir=tmp_staging_dir,
        vault_path=tmp_vault_path,
        manifest_out=custom_manifest,
        rebuild_fts=True,
    )

    assert manifest["status"] == "SUCCESS"
    assert len(manifest["shards_processed"]) == 3
    assert manifest["total_sources"] == 3
    assert manifest["total_items"] == 3
    assert manifest["total_payloads"] == 3

    # Streams breakdown verification
    streams = manifest.get("streams", {})
    assert "stream_a" in streams
    assert "stream_b" in streams
    assert "stream_c" in streams

    # Stream A: Comments
    sa = streams["stream_a"]
    assert sa["sources_count"] == 1
    assert sa["items_count"] == 1
    assert sa["payloads_count"] == 1
    assert sa["comments_count"] == 2
    assert sa["pre_2022_compliant"] is True
    assert sa["temporal_violations"] == 0
    assert sa["max_timestamp"] < 1640995200
    assert sa["sha256_verified"] is True
    assert sa["character_count"] > 0
    assert sa["byte_size"] > 0

    # Stream B: Cinema Subtitles
    sb = streams["stream_b"]
    assert sb["sources_count"] == 1
    assert sb["items_count"] == 1
    assert sb["payloads_count"] == 1
    assert sb["sha256_verified"] is True
    assert sb["character_count"] > 0

    # Stream C: Hansard Debates
    sc = streams["stream_c"]
    assert sc["sources_count"] == 1
    assert sc["items_count"] == 1
    assert sc["payloads_count"] == 1
    assert sc["sha256_verified"] is True
    assert sc["character_count"] > 0

    # Verification block
    verif = manifest.get("verification", {})
    assert verif["all_sha256_verified"] is True
    assert verif["foreign_keys_verified"] is True
    assert verif["integrity_check_passed"] is True
    assert verif["fts5_rebuilt"] is True
    assert verif["fts5_count_match"] is True

    # Searchability across all three streams in vault
    vm = VaultManager(db_path=tmp_vault_path)
    # Search Stream A content
    res_a = vm.search_text("syntaxique")
    assert len(res_a) >= 1
    assert res_a[0]["item_id"] == "yt_comment_vid_fr_01"

    # Search Stream B content
    res_b = vm.search_text("banlieue")
    assert len(res_b) >= 1
    assert res_b[0]["item_id"] == "sub_opensubtitles_3283706"

    # Search Stream C content
    res_c = vm.search_text("linguistique")
    assert len(res_c) >= 1
    assert res_c[0]["item_id"] == "ca_commons_432_han100"
    vm.close()


def test_26_stream_a_post2022_timestamp_violation_flagged(tmp_staging_dir: Path, tmp_vault_path: Path):
    """Test 26: If Stream A contains comments >= 1640995200, manifest flags pre_2022_compliant=False."""
    shard_a = tmp_staging_dir / "shard_stream_a.db"
    post2022_comments = json.dumps([
        {
            "comment_id": "comment_bad",
            "text": "Commentaire post-2022 invalide pour le corpus historique.",
            "timestamp": 1650000000,  # April 2022 (VIOLATION >= 1640995200)
        }
    ])

    vm_a = VaultManager(db_path=shard_a)
    vm_a.init_db()
    with vm_a.transaction():
        vm_a.upsert_source("yt:@BadChannel", "youtube", "fr", "Bad Channel", "https://youtube.com/@bad")
        vm_a.upsert_item("yt_bad_item", "yt:@BadChannel", "Bad Video", "https://youtube.com/watch?v=bad")
        vm_a.insert_payload("yt_bad_item", "comment_thread", "fr", "human_written", "json", post2022_comments)
    vm_a.close()

    manifest = merge_staging_to_vault(staging_dir=tmp_staging_dir, vault_path=tmp_vault_path)
    assert manifest["status"] == "SUCCESS"
    stream_a = manifest["streams"]["stream_a"]
    assert stream_a["pre_2022_compliant"] is False
    assert stream_a["temporal_violations"] == 1
    assert manifest["verification"]["stream_a_pre2022_verified"] is False


def test_27_sha256_checksum_mismatch_detected(tmp_staging_dir: Path, tmp_vault_path: Path):
    """Test 27: Shard with tampered payload id is flagged with sha256_verified=False."""
    shard_corrupt = tmp_staging_dir / "shard_tampered.db"
    vm = VaultManager(db_path=shard_corrupt)
    vm.init_db()
    with vm.transaction():
        vm.upsert_source("yt:@Tamper", "youtube", "fr", "Tamper", "https://youtube.com/@tamper")
        vm.upsert_item("yt_tamper_item", "yt:@Tamper", "Tampered Title", "https://youtube.com/watch?v=tamper")
        # Direct SQL insert with forged ID
        vm.conn.execute(
            "INSERT INTO payloads (id, item_id, payload_type, language_iso1, origin_type, format, raw_content, fetched_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
            (
                "FORGED_HASH_THAT_DOES_NOT_MATCH_CONTENT",
                "yt_tamper_item",
                "manual_cc",
                "fr",
                "human_transcribed",
                "vtt",
                "Authentic text content",
                "2026-11-08T00:00:00Z",
            ),
        )
    vm.close()

    manifest = merge_staging_to_vault(staging_dir=tmp_staging_dir, vault_path=tmp_vault_path)
    shard_info = manifest["streams"]["shard_tampered"]
    assert shard_info["sha256_verified"] is False
    assert shard_info["sha256_mismatches"] == 1
    assert manifest["verification"]["all_sha256_verified"] is False


def test_28_foreign_keys_and_wal_enforced(tmp_staging_dir: Path, tmp_vault_path: Path):
    """Test 28: Merge enforces PRAGMA foreign_keys and PRAGMA journal_mode = WAL."""
    _populate_french_tri_source_shards(tmp_staging_dir)
    merge_staging_to_vault(staging_dir=tmp_staging_dir, vault_path=tmp_vault_path)

    conn = sqlite3.connect(str(tmp_vault_path))
    fk_violations = conn.execute("PRAGMA foreign_key_check;").fetchall()
    integrity = conn.execute("PRAGMA integrity_check;").fetchall()
    journal = conn.execute("PRAGMA journal_mode;").fetchone()[0]
    conn.close()

    assert len(fk_violations) == 0
    assert len(integrity) == 1 and integrity[0][0] == "ok"
    assert journal.lower() == "wal"

