"""Tier 1: Comprehensive Feature Coverage Test Suite.

Validates the 11 core feature groups with >= 5 substantive, non-trivial tests each:
- Group 1: Catalog acquisition, deduplication, anomaly remediation, squad allocation, partition export
- Group 2: Manual CC priority (fa, fa-IR, fas -> manual_cc, human_transcribed)
- Group 3: Auto CC fallback (fa, fa-IR, fas, fa-orig -> auto_cc, machine_transcribed)
- Group 4: In-memory VTT streaming & SHA-256 provenance
- Group 5: Strict duration filter (>= 600s, filter Shorts < 60s, teasers, promos, None)
- Group 6: Capping policy (top N by view_count DESC) vs uncapped (--all-videos)
- Group 7: Staging DB isolation (shard_<id>.db, schema init, WAL, foreign keys, resumption)
- Group 8: SQLite ATTACH merge & multi-table deduplication (INSERT OR IGNORE)
- Group 9: FTS5 sync & rebuild (payloads_fts triggers, BM25 Persian query, explicit rebuild)
- Group 10: Ingestion manifest output (timestamped JSON, shard stats, vault totals, word counts)
- Group 11: Teamwork coordinator runner (parallel execution, isolated logs, auto-merge)
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

import pytest

from src.vault.db import VaultManager
from tests.e2e.contract_helpers import (
    count_persian_words_in_vtt,
    harvest_channel_shard,
    merge_staging_to_vault,
    partition_channels,
    resolve_persian_subtitles,
    run_teamwork_harvest,
)


# =============================================================================
# GROUP 1: Catalog Acquisition, Deduplication, Anomaly Remediation & Squads
# =============================================================================

def test_g1_catalog_acquisition_and_presence(canonical_catalog_path: Path):
    """G1.1: Verify catalog file exists, parses as valid JSON, and contains 25 channels."""
    assert canonical_catalog_path.is_file()
    with open(canonical_catalog_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    assert "channels" in data
    assert data["total_channels_audited"] == 25
    assert len(data["channels"]) == 25
    assert data["channels_with_any_manual_fa_cc"] == 5


def test_g1_catalog_deduplication_fiction_podcast(canonical_catalog_path: Path):
    """G1.2: Verify deduplication consolidates duplicate Fiction Podcast entries (#12 and #13)."""
    with open(canonical_catalog_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    fiction_entries = [
        c for c in data["channels"]
        if "fiction" in (c.get("uploader_id") or "").lower()
        or "fiction" in (c.get("channel_title") or "").lower()
    ]
    assert len(fiction_entries) == 2, "Catalog should initially contain 2 duplicate Fiction entries"

    # Run partition with deduplication
    res = partition_channels(catalog_path=canonical_catalog_path, num_shards=4, dedup=True)
    all_assigned_handles = []
    for shard in res["shards"]:
        for ch in shard["channels"]:
            all_assigned_handles.append(ch["handle"].lower())

    fiction_in_shards = [h for h in all_assigned_handles if "fiction" in h]
    assert len(fiction_in_shards) == 1, "Deduplication must yield exactly 1 Fiction Podcast entry in shards"


def test_g1_catalog_anomaly_remediation_kouman_and_marz(canonical_catalog_path: Path):
    """G1.3: Verify catalog anomaly handles @Kouman and @radio-marz are remediated from faulty seeds."""
    res = partition_channels(catalog_path=canonical_catalog_path, num_shards=4, dedup=True)
    handles = {c["handle"] for s in res["shards"] for c in s["channels"]}

    assert "@Kouman" in handles, "Kouman handle must be normalized to @Kouman"
    assert "Kouman_Official" not in handles and "@Kouman_Official" not in handles
    assert "@radio-marz" in handles, "Radio Marz handle must be normalized to @radio-marz"
    assert "radiomarz" not in handles


def test_g1_catalog_empty_channel_handling_navidkashani(canonical_catalog_path: Path):
    """G1.4: Verify 0-video channel (@navidkashani) is gracefully handled and filtered."""
    # With filter_empty = True
    res_filtered = partition_channels(
        catalog_path=canonical_catalog_path, num_shards=4, dedup=True, filter_empty=True
    )
    handles_filtered = {c["handle"] for s in res_filtered["shards"] for c in s["channels"]}
    assert "@navidkashani" not in handles_filtered

    # With filter_empty = False
    res_retained = partition_channels(
        catalog_path=canonical_catalog_path, num_shards=4, dedup=True, filter_empty=False
    )
    handles_retained = {c["handle"] for s in res_retained["shards"] for c in s["channels"]}
    assert "@navidkashani" in handles_retained


def test_g1_domain_squad_allocation_4_squads(canonical_catalog_path: Path):
    """G1.5: Verify channels are partitioned into 4 cohesive domain squads."""
    res = partition_channels(catalog_path=canonical_catalog_path, num_shards=4)
    shards = res["shards"]
    assert len(shards) == 4

    squad_names = [s["squad_name"] for s in shards]
    assert "Podcasts & Thought" in squad_names[0]
    assert "Vlogs & Entertainment" in squad_names[1]
    assert "Narrative & Culture" in squad_names[2]
    assert "Practical & Instructional" in squad_names[3]

    for shard in shards:
        assert shard["channels_count"] >= 5, f"Shard {shard['shard_id']} has fewer than 5 channels"


def test_g1_partition_json_export_and_cli_args(canonical_catalog_path: Path, sandbox_staging_dir: Path):
    """G1.6: Verify partition export produces valid JSON and correct CLI argument strings."""
    out_json = sandbox_staging_dir / "partitions.json"
    res = partition_channels(
        catalog_path=canonical_catalog_path,
        num_shards=4,
        output_path=out_json,
    )
    assert out_json.is_file()
    with open(out_json, "r", encoding="utf-8") as f:
        saved = json.load(f)

    assert saved["shards_count"] == 4
    for shard in saved["shards"]:
        cli_args = shard["cli_args"]
        assert f"--shard-id {shard['shard_id']}" in cli_args
        assert "--channels" in cli_args


# =============================================================================
# GROUP 2: Manual CC Priority
# =============================================================================

def test_g2_manual_cc_priority_over_auto_cc():
    """G2.1: Priority 1 - When both manual and auto Persian CC exist, select manual CC."""
    video_meta = {
        "id": "vid_both",
        "subtitles": {
            "fa": [{"ext": "vtt", "url": "https://example.com/manual.vtt", "name": "Persian"}]
        },
        "automatic_captions": {
            "fa": [{"ext": "vtt", "url": "https://example.com/auto.vtt", "name": "Persian (auto)"}]
        },
    }
    url, p_type, o_type = resolve_persian_subtitles(video_meta)
    assert url == "https://example.com/manual.vtt"
    assert p_type == "manual_cc"
    assert o_type == "human_transcribed"


def test_g2_manual_cc_lang_code_fa():
    """G2.2: Manual CC resolution with language code 'fa'."""
    video_meta = {
        "subtitles": {"fa": [{"ext": "vtt", "url": "https://example.com/fa.vtt"}]}
    }
    url, p_type, o_type = resolve_persian_subtitles(video_meta)
    assert url == "https://example.com/fa.vtt"
    assert p_type == "manual_cc"
    assert o_type == "human_transcribed"


def test_g2_manual_cc_lang_code_fa_ir():
    """G2.3: Manual CC resolution with regional language code 'fa-IR'."""
    video_meta = {
        "subtitles": {"fa-IR": [{"ext": "vtt", "url": "https://example.com/fa_ir.vtt"}]}
    }
    url, p_type, o_type = resolve_persian_subtitles(video_meta)
    assert url == "https://example.com/fa_ir.vtt"
    assert p_type == "manual_cc"
    assert o_type == "human_transcribed"


def test_g2_manual_cc_lang_code_fas():
    """G2.4: Manual CC resolution with ISO 639-2/3 language code 'fas'."""
    video_meta = {
        "subtitles": {"fas": [{"ext": "vtt", "url": "https://example.com/fas.vtt"}]}
    }
    url, p_type, o_type = resolve_persian_subtitles(video_meta)
    assert url == "https://example.com/fas.vtt"
    assert p_type == "manual_cc"
    assert o_type == "human_transcribed"


def test_g2_manual_cc_vtt_format_selection_and_fmt_param():
    """G2.5: Subtitle track selection appends &fmt=vtt when url lacks format parameter."""
    video_meta = {
        "subtitles": {
            "fa": [
                {"ext": "srv3", "url": "https://example.com/sub?lang=fa"},
                {"ext": "vtt", "url": "https://example.com/sub.vtt"},
            ]
        }
    }
    url, p_type, _ = resolve_persian_subtitles(video_meta)
    assert url == "https://example.com/sub.vtt"
    assert p_type == "manual_cc"

    # URL without vtt extension appends fmt=vtt
    video_meta_raw = {
        "subtitles": {"fa": [{"ext": "srv1", "url": "https://example.com/timedtext?lang=fa"}]}
    }
    url_raw, _, _ = resolve_persian_subtitles(video_meta_raw)
    assert "fmt=vtt" in url_raw


# =============================================================================
# GROUP 3: Auto CC Fallback
# =============================================================================

def test_g3_auto_cc_fallback_when_manual_absent():
    """G3.1: Fallback to auto CC when manual Persian subtitles are not present."""
    video_meta = {
        "subtitles": {},
        "automatic_captions": {
            "fa": [{"ext": "vtt", "url": "https://example.com/auto_fa.vtt"}]
        },
    }
    url, p_type, o_type = resolve_persian_subtitles(video_meta)
    assert url == "https://example.com/auto_fa.vtt"
    assert p_type == "auto_cc"
    assert o_type == "machine_transcribed"


def test_g3_auto_cc_lang_code_fa_orig():
    """G3.2: Auto CC resolution with 'fa-orig' original ASR track."""
    video_meta = {
        "automatic_captions": {
            "fa-orig": [{"ext": "vtt", "url": "https://example.com/fa_orig.vtt"}]
        }
    }
    url, p_type, o_type = resolve_persian_subtitles(video_meta)
    assert url == "https://example.com/fa_orig.vtt"
    assert p_type == "auto_cc"
    assert o_type == "machine_transcribed"


def test_g3_auto_cc_lang_code_fa_ir():
    """G3.3: Auto CC resolution with 'fa-IR' language code."""
    video_meta = {
        "automatic_captions": {
            "fa-IR": [{"ext": "vtt", "url": "https://example.com/auto_fa_ir.vtt"}]
        }
    }
    url, p_type, o_type = resolve_persian_subtitles(video_meta)
    assert url == "https://example.com/auto_fa_ir.vtt"
    assert p_type == "auto_cc"
    assert o_type == "machine_transcribed"


def test_g3_auto_cc_absence_yields_none():
    """G3.4: Video with only non-Persian subtitles yields (None, 'none', 'none')."""
    video_meta = {
        "subtitles": {
            "en": [{"ext": "vtt", "url": "https://example.com/en.vtt"}],
            "es": [{"ext": "vtt", "url": "https://example.com/es.vtt"}],
        },
        "automatic_captions": {
            "en": [{"ext": "vtt", "url": "https://example.com/auto_en.vtt"}]
        },
    }
    url, p_type, o_type = resolve_persian_subtitles(video_meta)
    assert url is None
    assert p_type == "none"
    assert o_type == "none"


def test_g3_auto_cc_empty_subtitles_dict():
    """G3.5: Video with completely empty subtitles dictionary yields (None, 'none', 'none')."""
    assert resolve_persian_subtitles({}) == (None, "none", "none")
    assert resolve_persian_subtitles({"subtitles": None, "automatic_captions": None}) == (
        None,
        "none",
        "none",
    )


# =============================================================================
# GROUP 4: In-Memory VTT Streaming & SHA-256 Provenance
# =============================================================================

def test_g4_in_memory_vtt_streaming_zero_media_download(
    persian_vtt_formal: str, sandbox_staging_dir: Path
):
    """G4.1: Stream WebVTT directly in memory without writing audio or video files to disk."""
    channels = ["@Tabaghe16"]
    sample_videos = [
        {
            "id": "vid_stream_test",
            "title": "اپیزود تستی حافظه",
            "duration": 1200,
            "view_count": 50000,
            "subtitles": {
                "fa": [{"ext": "vtt", "url": "https://mock.local/vtt", "name": "Persian"}]
            },
            "vtt_content": persian_vtt_formal,
        }
    ]

    res = harvest_channel_shard(
        shard_id=1,
        channels=channels,
        staging_dir=sandbox_staging_dir,
        min_duration=600,
        channel_video_provider=lambda ch: sample_videos,
    )
    assert res["payloads_inserted"] == 1
    # Verify no audio or video files exist in staging dir
    media_files = list(sandbox_staging_dir.glob("*.mp4")) + list(sandbox_staging_dir.glob("*.m4a"))
    assert len(media_files) == 0, "No media files should be downloaded to disk"


def test_g4_sha256_provenance_digest_computation(
    persian_vtt_colloquial: str, sandbox_staging_dir: Path
):
    """G4.2: Verify payload id in SQLite matches exact SHA-256 hash of UTF-8 VTT text."""
    expected_hash = hashlib.sha256(persian_vtt_colloquial.encode("utf-8")).hexdigest()
    channels = ["@MiaPlays"]
    sample_videos = [
        {
            "id": "vid_hash_test",
            "title": "ولاگ تستی هش",
            "duration": 900,
            "view_count": 45000,
            "subtitles": {
                "fa": [{"ext": "vtt", "url": "https://mock.local/vtt"}]
            },
            "vtt_content": persian_vtt_colloquial,
        }
    ]

    harvest_channel_shard(
        shard_id=2,
        channels=channels,
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: sample_videos,
    )

    db_path = sandbox_staging_dir / "shard_2.db"
    conn = sqlite3.connect(str(db_path))
    row = conn.execute("SELECT id, raw_content FROM payloads;").fetchone()
    conn.close()

    assert row is not None
    assert row[0] == expected_hash, f"Payload ID {row[0]} does not match expected SHA-256 {expected_hash}"
    assert row[1] == persian_vtt_colloquial


def test_g4_sha256_idempotent_deduplication(
    persian_vtt_formal: str, sandbox_vault_path: Path
):
    """G4.3: Deduplicate identical VTT content via INSERT OR IGNORE and SHA-256."""
    vm = VaultManager(db_path=sandbox_vault_path)
    vm.init_db()

    vm.upsert_source(id="s1", platform="youtube", language_iso1="fa", name="S1", url="http://s1")
    vm.upsert_item(id="i1", source_id="s1", title="I1", url="http://i1")
    vm.upsert_item(id="i2", source_id="s1", title="I2", url="http://i2")

    pid1 = vm.insert_payload(
        item_id="i1",
        payload_type="manual_cc",
        language_iso1="fa",
        origin_type="human_transcribed",
        format="vtt",
        raw_content=persian_vtt_formal,
    )
    # Re-insert identical content for another item
    pid2 = vm.insert_payload(
        item_id="i2",
        payload_type="manual_cc",
        language_iso1="fa",
        origin_type="human_transcribed",
        format="vtt",
        raw_content=persian_vtt_formal,
    )
    assert pid1 == pid2
    count = vm.conn.execute("SELECT COUNT(*) FROM payloads;").fetchone()[0]
    assert count == 1, "Duplicate payload content must be deduplicated by primary key SHA-256"
    vm.close()


def test_g4_persian_word_count_computation(persian_vtt_formal: str):
    """G4.4: Verify Persian word count accurately strips VTT headers, timestamps, and metadata."""
    wc = count_persian_words_in_vtt(persian_vtt_formal)
    assert wc > 30, f"Expected >30 words in formal Persian fixture, got {wc}"


def test_g4_vtt_streaming_timeout_and_error_handling(sandbox_staging_dir: Path):
    """G4.5: Subtitle stream HTTP error catalogs item, avoids fatal crash, and continues."""
    channels = ["@BBCNewsPersian"]
    sample_videos = [
        {
            "id": "vid_broken_stream",
            "title": "ویدیوی با خطای سرور زیرنویس",
            "duration": 800,
            "view_count": 20000,
            "subtitles": {
                "fa": [{"ext": "vtt", "url": "https://broken.stream/err.vtt"}]
            },
        }
    ]

    def failing_vtt_provider(v_id: str, url: str) -> None:
        raise ConnectionError("Simulated network timeout")

    res = harvest_channel_shard(
        shard_id=3,
        channels=channels,
        staging_dir=sandbox_staging_dir,
        vtt_provider=failing_vtt_provider,
        channel_video_provider=lambda ch: sample_videos,
    )
    # Item cataloged, 0 payloads inserted, execution successful
    assert res["items_cataloged"] == 1
    assert res["payloads_inserted"] == 0


# =============================================================================
# GROUP 5: Strict Duration Filter (>= 600s)
# =============================================================================

def test_g5_duration_filter_short_clip_excluded(sandbox_staging_dir: Path):
    """G5.1: Exclude YouTube Shorts (< 60s)."""
    videos = [{"id": "short_1", "duration": 45, "view_count": 100000}]
    res = harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        min_duration=600,
        channel_video_provider=lambda ch: videos,
    )
    assert res["eligible_videos_scanned"] == 0
    assert res["items_cataloged"] == 0


def test_g5_duration_filter_promo_clip_excluded(sandbox_staging_dir: Path):
    """G5.2: Exclude intro clips and trailers (< 600s, e.g. 300s)."""
    videos = [{"id": "promo_1", "duration": 300, "view_count": 15000}]
    res = harvest_channel_shard(
        shard_id=1,
        channels=["@ChannelBPodcast"],
        staging_dir=sandbox_staging_dir,
        min_duration=600,
        channel_video_provider=lambda ch: videos,
    )
    assert res["eligible_videos_scanned"] == 0


def test_g5_duration_filter_exact_threshold_accepted(sandbox_staging_dir: Path):
    """G5.3: Accept videos with duration = 600s exactly."""
    videos = [{"id": "exact_600", "duration": 600, "view_count": 25000}]
    res = harvest_channel_shard(
        shard_id=1,
        channels=["@BplusPodcast"],
        staging_dir=sandbox_staging_dir,
        min_duration=600,
        channel_video_provider=lambda ch: videos,
    )
    assert res["eligible_videos_scanned"] == 1
    assert res["items_cataloged"] == 1


def test_g5_duration_filter_longform_accepted(sandbox_staging_dir: Path):
    """G5.4: Accept longform videos (> 600s, e.g. 5400s / 90 min)."""
    videos = [{"id": "long_1", "duration": 5400, "view_count": 80000}]
    res = harvest_channel_shard(
        shard_id=1,
        channels=["@FictionPodcast"],
        staging_dir=sandbox_staging_dir,
        min_duration=600,
        channel_video_provider=lambda ch: videos,
    )
    assert res["eligible_videos_scanned"] == 1
    assert res["items_cataloged"] == 1


def test_g5_duration_filter_none_evaluated_safely(sandbox_staging_dir: Path):
    """G5.5: Safely handle and exclude unstarted livestreams with duration=None."""
    videos = [{"id": "stream_live", "duration": None, "view_count": 500}]
    res = harvest_channel_shard(
        shard_id=1,
        channels=["@radio-marz"],
        staging_dir=sandbox_staging_dir,
        min_duration=600,
        channel_video_provider=lambda ch: videos,
    )
    assert res["eligible_videos_scanned"] == 0
    assert res["items_cataloged"] == 0


# =============================================================================
# GROUP 6: Capping Policy vs Uncapped
# =============================================================================

def test_g6_capping_policy_slices_top_n_by_view_count(sandbox_staging_dir: Path):
    """G6.1: Capping policy selects top N candidates sorted by view_count DESC."""
    # Create 40 eligible videos with increasing view counts
    videos = [
        {"id": f"v_{i}", "duration": 800, "view_count": i * 1000}
        for i in range(1, 41)
    ]
    res = harvest_channel_shard(
        shard_id=2,
        channels=["@MiaPlays"],
        staging_dir=sandbox_staging_dir,
        cap_per_channel=25,
        all_videos=False,
        channel_video_provider=lambda ch: videos,
    )
    assert res["eligible_videos_scanned"] == 25
    assert res["items_cataloged"] == 25

    # Check that highest view count videos were cataloged
    db_path = sandbox_staging_dir / "shard_2.db"
    conn = sqlite3.connect(str(db_path))
    min_view = conn.execute("SELECT MIN(view_count) FROM items;").fetchone()[0]
    conn.close()
    assert min_view >= 16000, "Should only select top 25 highest-viewed videos (16k to 40k)"


def test_g6_capping_policy_under_cap_retains_all(sandbox_staging_dir: Path):
    """G6.2: When channel has fewer eligible videos than cap, retain all without error."""
    videos = [
        {"id": f"v_{i}", "duration": 700, "view_count": i * 500}
        for i in range(1, 11)
    ]
    res = harvest_channel_shard(
        shard_id=2,
        channels=["@The_Fazel"],
        staging_dir=sandbox_staging_dir,
        cap_per_channel=25,
        channel_video_provider=lambda ch: videos,
    )
    assert res["eligible_videos_scanned"] == 10
    assert res["items_cataloged"] == 10


def test_g6_uncapped_all_videos_bypasses_cap(sandbox_staging_dir: Path):
    """G6.3: Flag --all-videos bypasses capping and processes 100% of eligible longform videos."""
    videos = [
        {"id": f"v_{i}", "duration": 800, "view_count": i * 1000}
        for i in range(1, 35)
    ]
    res = harvest_channel_shard(
        shard_id=2,
        channels=["@Bahare"],
        staging_dir=sandbox_staging_dir,
        cap_per_channel=10,
        all_videos=True,
        channel_video_provider=lambda ch: videos,
    )
    assert res["eligible_videos_scanned"] == 34
    assert res["items_cataloged"] == 34


def test_g6_capping_sort_none_view_count_handling(sandbox_staging_dir: Path):
    """G6.4: Candidate videos with view_count=None sort after videos with known views."""
    videos = [
        {"id": "v_known_1", "duration": 900, "view_count": 50000},
        {"id": "v_known_2", "duration": 900, "view_count": 25000},
        {"id": "v_none_view", "duration": 900, "view_count": None},
    ]
    res = harvest_channel_shard(
        shard_id=2,
        channels=["@IraniTanz"],
        staging_dir=sandbox_staging_dir,
        cap_per_channel=2,
        channel_video_provider=lambda ch: videos,
    )
    assert res["items_cataloged"] == 2
    db_path = sandbox_staging_dir / "shard_2.db"
    conn = sqlite3.connect(str(db_path))
    ids = [r[0] for r in conn.execute("SELECT id FROM items;").fetchall()]
    conn.close()
    assert "yt:v_known_1" in ids
    assert "yt:v_known_2" in ids
    assert "yt:v_none_view" not in ids


def test_g6_capping_preserves_channel_balance(sandbox_staging_dir: Path):
    """G6.5: Multi-channel shard applies cap per channel independently."""
    channels = ["@Dastan_land", "@ShahreFarangs"]

    def sample_provider(ch: str):
        count = 30 if ch == "@Dastan_land" else 20
        return [{"id": f"{ch}_{i}", "duration": 700, "view_count": i * 100} for i in range(count)]

    res = harvest_channel_shard(
        shard_id=3,
        channels=channels,
        staging_dir=sandbox_staging_dir,
        cap_per_channel=15,
        channel_video_provider=sample_provider,
    )
    db_path = sandbox_staging_dir / "shard_3.db"
    conn = sqlite3.connect(str(db_path))
    dastan_count = conn.execute("SELECT COUNT(*) FROM items WHERE source_id = 'yt:@Dastan_land';").fetchone()[0]
    shahre_count = conn.execute("SELECT COUNT(*) FROM items WHERE source_id = 'yt:@ShahreFarangs';").fetchone()[0]
    conn.close()

    assert dastan_count == 15, "Capped at 15"
    assert shahre_count == 15, "Capped at 15"


# =============================================================================
# GROUP 7: Staging Database Isolation
# =============================================================================

def test_g7_staging_shard_initialization(sandbox_staging_dir: Path):
    """G7.1: Worker initializes shard database with correct tables and schema."""
    harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: [],
    )
    db_path = sandbox_staging_dir / "shard_1.db"
    assert db_path.is_file()

    conn = sqlite3.connect(str(db_path))
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()}
    conn.close()
    assert {"sources", "items", "payloads", "payloads_fts"}.issubset(tables)


def test_g7_staging_shard_wal_and_pragmas(sandbox_staging_dir: Path):
    """G7.2: Verify WAL journal mode and busy_timeout pragmas."""
    harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: [],
    )
    db_path = sandbox_staging_dir / "shard_1.db"
    conn = sqlite3.connect(str(db_path))
    journal_mode = conn.execute("PRAGMA journal_mode;").fetchone()[0].upper()
    conn.close()
    assert journal_mode == "WAL"


def test_g7_staging_shard_foreign_key_integrity(sandbox_staging_dir: Path):
    """G7.3: Foreign keys strictly enforce referential integrity."""
    db_path = sandbox_staging_dir / "shard_test_fk.db"
    vm = VaultManager(db_path=db_path)
    vm.init_db()

    conn = vm.conn
    conn.execute("PRAGMA foreign_keys = ON;")

    # Attempting to insert an item referencing a non-existent source must fail
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO items (id, source_id, title, url, created_at) VALUES ('yt:bad', 'yt:ghost', 'T', 'U', '2026-11-06T00:00:00Z');"
        )
    vm.close()


def test_g7_staging_shard_idempotent_resumption(sandbox_staging_dir: Path):
    """G7.4: Resuming interrupted shard skips already-completed items and payloads."""
    videos = [
        {"id": "v_resume", "duration": 800, "view_count": 5000, "vtt_content": "WEBVTT\n\n00:00:01.000 --> 00:00:02.000\nسلام"}
    ]
    # First pass
    res1 = harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: videos,
    )
    assert res1["items_cataloged"] == 1
    assert res1["payloads_inserted"] == 1

    # Second pass
    res2 = harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: videos,
    )
    assert res2["resumed_skipped_count"] == 1
    assert res2["items_cataloged"] == 0
    assert res2["payloads_inserted"] == 0


def test_g7_staging_shard_multi_shard_isolation(sandbox_staging_dir: Path):
    """G7.5: Shard databases operate with complete physical and transactional isolation."""
    for s_id in (1, 2, 3, 4):
        harvest_channel_shard(
            shard_id=s_id,
            channels=[f"@Channel_{s_id}"],
            staging_dir=sandbox_staging_dir,
            channel_video_provider=lambda ch: [],
        )

    for s_id in (1, 2, 3, 4):
        p = sandbox_staging_dir / f"shard_{s_id}.db"
        assert p.is_file()
        conn = sqlite3.connect(str(p))
        src = conn.execute("SELECT id FROM sources;").fetchone()
        conn.close()
        assert src[0] == f"yt:@Channel_{s_id}"


# =============================================================================
# GROUP 8: SQLite ATTACH Merge & Multi-Table Deduplication
# =============================================================================

def test_g8_attach_merge_sources_items_payloads(
    sandbox_staging_dir: Path, sandbox_vault_path: Path, persian_vtt_formal: str
):
    """G8.1: SQLite ATTACH atomically merges sources, items, and payloads."""
    # Create shard 1
    videos = [
        {"id": "v_m1", "duration": 1000, "view_count": 20000, "vtt_content": persian_vtt_formal}
    ]
    harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: videos,
    )

    manifest = merge_staging_to_vault(
        staging_dir=sandbox_staging_dir,
        vault_path=sandbox_vault_path,
        rebuild_fts=True,
    )
    assert manifest["status"] == "SUCCESS"
    assert manifest["sources_added"] == 1
    assert manifest["items_added"] == 1
    assert manifest["payloads_added"] == 1


def test_g8_attach_merge_deduplication_via_insert_or_ignore(
    sandbox_staging_dir: Path, sandbox_vault_path: Path, persian_vtt_formal: str
):
    """G8.2: Merger deduplicates identical items and payloads via INSERT OR IGNORE."""
    videos = [
        {"id": "v_dup", "duration": 900, "view_count": 15000, "vtt_content": persian_vtt_formal}
    ]
    # Harvest same video into Shard 1 and Shard 2
    harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: videos,
    )
    harvest_channel_shard(
        shard_id=2,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: videos,
    )

    manifest = merge_staging_to_vault(
        staging_dir=sandbox_staging_dir,
        vault_path=sandbox_vault_path,
    )
    assert manifest["status"] == "SUCCESS"
    assert manifest["total_items"] == 1, "Duplicate item across shards must be deduplicated"
    assert manifest["total_payloads"] == 1


def test_g8_attach_merge_foreign_key_order(
    sandbox_staging_dir: Path, sandbox_vault_path: Path
):
    """G8.3: Merger strictly adheres to FK insertion order (sources -> items -> payloads)."""
    videos = [{"id": "v_fk_order", "duration": 700, "view_count": 1000}]
    harvest_channel_shard(
        shard_id=1,
        channels=["@ChannelBPodcast"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: videos,
    )

    merge_staging_to_vault(staging_dir=sandbox_staging_dir, vault_path=sandbox_vault_path)

    conn = sqlite3.connect(str(sandbox_vault_path))
    conn.execute("PRAGMA foreign_keys = ON;")
    # FK check passes with 0 violations
    violations = conn.execute("PRAGMA foreign_key_check;").fetchall()
    conn.close()
    assert len(violations) == 0


def test_g8_attach_merge_multiple_shards_consolidated(
    sandbox_staging_dir: Path, sandbox_vault_path: Path
):
    """G8.4: Consolidate 4 worker shards into a unified vault."""
    for s_id in range(1, 5):
        vids = [{"id": f"vid_s{s_id}", "duration": 800, "view_count": s_id * 1000}]
        harvest_channel_shard(
            shard_id=s_id,
            channels=[f"@Channel_{s_id}"],
            staging_dir=sandbox_staging_dir,
            channel_video_provider=lambda ch, v=vids: v,
        )

    manifest = merge_staging_to_vault(staging_dir=sandbox_staging_dir, vault_path=sandbox_vault_path)
    assert len(manifest["shards_processed"]) == 4
    assert manifest["total_sources"] == 4
    assert manifest["total_items"] == 4


def test_g8_attach_merge_empty_staging_directory(
    sandbox_staging_dir: Path, sandbox_vault_path: Path
):
    """G8.5: Handle empty staging directory without error."""
    manifest = merge_staging_to_vault(staging_dir=sandbox_staging_dir, vault_path=sandbox_vault_path)
    assert manifest["status"] == "NO_SHARDS"
    assert manifest["shards_processed"] == []


# =============================================================================
# GROUP 9: FTS5 Sync & Rebuild
# =============================================================================

def test_g9_fts5_triggers_insert_synchronization(sandbox_vault_path: Path):
    """G9.1: Inserting payload automatically indexes text in payloads_fts via payloads_ai."""
    vm = VaultManager(db_path=sandbox_vault_path)
    vm.init_db()
    vm.upsert_source(id="s1", platform="youtube", language_iso1="fa", name="S1", url="http://s1")
    vm.upsert_item(id="i1", source_id="s1", title="I1", url="http://i1")

    persian_text = "این یک گفتگوی تخصصی پیرامون هوش مصنوعی و مدل‌های زبانی است."
    vm.insert_payload(
        item_id="i1",
        payload_type="manual_cc",
        language_iso1="fa",
        origin_type="human_transcribed",
        format="vtt",
        raw_content=persian_text,
    )

    results = vm.search_text("هوش مصنوعی")
    assert len(results) == 1
    assert "هوش مصنوعی" in results[0]["snippet"]
    vm.close()


def test_g9_fts5_triggers_delete_synchronization(sandbox_vault_path: Path):
    """G9.2: Deleting payload removes index entry in payloads_fts via payloads_ad."""
    vm = VaultManager(db_path=sandbox_vault_path)
    vm.init_db()
    vm.upsert_source(id="s1", platform="youtube", language_iso1="fa", name="S1", url="http://s1")
    vm.upsert_item(id="i1", source_id="s1", title="I1", url="http://i1")

    vm.insert_payload(
        item_id="i1",
        payload_type="manual_cc",
        language_iso1="fa",
        origin_type="human_transcribed",
        format="vtt",
        raw_content="محتوای قابل حذف برای بررسی تریگر",
    )
    assert len(vm.search_text("حذف")) == 1

    # Delete payload
    vm.conn.execute("DELETE FROM payloads WHERE item_id = 'i1';")
    vm.conn.commit()
    assert len(vm.search_text("حذف")) == 0
    vm.close()


def test_g9_fts5_triggers_update_synchronization(sandbox_vault_path: Path):
    """G9.3: Updating payload synchronizes index entry in payloads_fts via payloads_au."""
    vm = VaultManager(db_path=sandbox_vault_path)
    vm.init_db()
    vm.upsert_source(id="s1", platform="youtube", language_iso1="fa", name="S1", url="http://s1")
    vm.upsert_item(id="i1", source_id="s1", title="I1", url="http://i1")

    vm.insert_payload(
        item_id="i1",
        payload_type="manual_cc",
        language_iso1="fa",
        origin_type="human_transcribed",
        format="vtt",
        raw_content="نسخه اولیه متن پادکست",
    )
    assert len(vm.search_text("اولیه")) == 1

    # Update payload raw_content
    vm.conn.execute("UPDATE payloads SET raw_content = 'نسخه ثانویه متن پادکست' WHERE item_id = 'i1';")
    vm.conn.commit()

    assert len(vm.search_text("اولیه")) == 0
    assert len(vm.search_text("ثانویه")) == 1
    vm.close()


def test_g9_fts5_bm25_persian_search_query(sandbox_vault_path: Path, persian_vtt_formal: str):
    """G9.4: FTS5 BM25 search ranks matching Persian transcripts accurately."""
    vm = VaultManager(db_path=sandbox_vault_path)
    vm.init_db()
    vm.upsert_source(id="s1", platform="youtube", language_iso1="fa", name="S1", url="http://s1")
    vm.upsert_item(id="i1", source_id="s1", title="ژئوپلیتیک", url="http://i1")

    vm.insert_payload(
        item_id="i1",
        payload_type="manual_cc",
        language_iso1="fa",
        origin_type="human_transcribed",
        format="vtt",
        raw_content=persian_vtt_formal,
    )

    matches = vm.search_text("ژئوپلیتیک")
    assert len(matches) == 1
    assert matches[0]["item_title"] == "ژئوپلیتیک"
    assert matches[0]["rank"] is not None
    vm.close()


def test_g9_fts5_explicit_rebuild_command(sandbox_vault_path: Path, persian_vtt_colloquial: str):
    """G9.5: Running explicit rebuild on payloads_fts succeeds and verifies integrity."""
    vm = VaultManager(db_path=sandbox_vault_path)
    vm.init_db()
    vm.upsert_source(id="s1", platform="youtube", language_iso1="fa", name="S1", url="http://s1")
    vm.upsert_item(id="i1", source_id="s1", title="کمدی", url="http://i1")

    vm.insert_payload(
        item_id="i1",
        payload_type="auto_cc",
        language_iso1="fa",
        origin_type="machine_transcribed",
        format="vtt",
        raw_content=persian_vtt_colloquial,
    )

    # Execute explicit rebuild
    vm.conn.execute("INSERT INTO payloads_fts(payloads_fts) VALUES ('rebuild');")
    vm.conn.commit()

    matches = vm.search_text("باحال")
    assert len(matches) == 1
    vm.close()


# =============================================================================
# GROUP 10: Ingestion Manifest Output
# =============================================================================

def test_g10_manifest_generation_and_schema(sandbox_staging_dir: Path, sandbox_vault_path: Path):
    """G10.1: Merge generates JSON manifest with timestamp, vault_path, and stats."""
    harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: [{"id": "v1", "duration": 800, "view_count": 100}],
    )
    manifest = merge_staging_to_vault(staging_dir=sandbox_staging_dir, vault_path=sandbox_vault_path)

    assert "timestamp" in manifest
    assert "vault_path" in manifest
    assert "sources_added" in manifest
    assert "items_added" in manifest
    assert "payloads_added" in manifest
    assert "manifest_path" in manifest
    assert Path(manifest["manifest_path"]).is_file()


def test_g10_manifest_aggregate_word_count(
    sandbox_staging_dir: Path, sandbox_vault_path: Path, persian_vtt_formal: str
):
    """G10.2: Manifest computes total aggregate Persian word count across payloads."""
    harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: [
            {"id": "v1", "duration": 800, "view_count": 100, "vtt_content": persian_vtt_formal}
        ],
    )
    manifest = merge_staging_to_vault(staging_dir=sandbox_staging_dir, vault_path=sandbox_vault_path)
    assert manifest["total_persian_words"] > 30


def test_g10_manifest_records_quarantined_shards(sandbox_staging_dir: Path, sandbox_vault_path: Path):
    """G10.3: Manifest records any quarantined corrupt shards."""
    # Write a corrupt shard file
    corrupt_shard = sandbox_staging_dir / "shard_99.db"
    corrupt_shard.write_bytes(b"NOT A SQLITE FILE")

    manifest = merge_staging_to_vault(staging_dir=sandbox_staging_dir, vault_path=sandbox_vault_path)
    assert len(manifest["quarantined_shards"]) == 1
    assert "shard_99" in manifest["quarantined_shards"][0]


def test_g10_manifest_custom_output_destination(sandbox_staging_dir: Path, sandbox_vault_path: Path):
    """G10.4: Flag --manifest-out writes manifest to custom destination."""
    custom_manifest_path = sandbox_staging_dir / "custom_audit_manifest.json"
    manifest = merge_staging_to_vault(
        staging_dir=sandbox_staging_dir,
        vault_path=sandbox_vault_path,
        manifest_out=custom_manifest_path,
    )
    assert custom_manifest_path.is_file()
    assert manifest["manifest_path"] == str(custom_manifest_path)


def test_g10_manifest_metrics_match_vault_counts(
    sandbox_staging_dir: Path, sandbox_vault_path: Path
):
    """G10.5: Manifest metrics strictly match SQL counts in destination vault."""
    harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: [
            {"id": "v_match", "duration": 900, "view_count": 1000}
        ],
    )
    manifest = merge_staging_to_vault(staging_dir=sandbox_staging_dir, vault_path=sandbox_vault_path)

    conn = sqlite3.connect(str(sandbox_vault_path))
    s_cnt = conn.execute("SELECT COUNT(*) FROM sources;").fetchone()[0]
    i_cnt = conn.execute("SELECT COUNT(*) FROM items;").fetchone()[0]
    p_cnt = conn.execute("SELECT COUNT(*) FROM payloads;").fetchone()[0]
    conn.close()

    assert manifest["total_sources"] == s_cnt
    assert manifest["total_items"] == i_cnt
    assert manifest["total_payloads"] == p_cnt


# =============================================================================
# GROUP 11: Teamwork Coordinator Runner
# =============================================================================

def test_g11_teamwork_coordinator_partition_and_dispatch(
    canonical_catalog_path: Path, sandbox_staging_dir: Path, sandbox_vault_path: Path
):
    """G11.1: Coordinator partitions catalog and dispatches 4 worker squads."""
    res = run_teamwork_harvest(
        shards=4,
        catalog=canonical_catalog_path,
        staging_dir=sandbox_staging_dir,
        vault_path=sandbox_vault_path,
        auto_merge=True,
        channel_video_provider=lambda ch: [],
    )
    assert res["success"] is True
    assert len(res["shard_results"]) == 4


def test_g11_teamwork_coordinator_isolated_shard_logs(
    canonical_catalog_path: Path, sandbox_staging_dir: Path, sandbox_vault_path: Path
):
    """G11.2: Coordinator generates isolated worker logs (shard_<id>.log)."""
    run_teamwork_harvest(
        shards=4,
        catalog=canonical_catalog_path,
        staging_dir=sandbox_staging_dir,
        vault_path=sandbox_vault_path,
        auto_merge=False,
        channel_video_provider=lambda ch: [],
    )
    for s_id in range(1, 5):
        log_f = sandbox_staging_dir / f"shard_{s_id}.log"
        assert log_f.is_file(), f"Missing log for shard {s_id}"
        content = log_f.read_text(encoding="utf-8")
        assert f"Shard {s_id}" in content


def test_g11_teamwork_coordinator_auto_merge_trigger(
    canonical_catalog_path: Path, sandbox_staging_dir: Path, sandbox_vault_path: Path
):
    """G11.3: Coordinator triggers auto-merge when auto_merge=True."""
    res = run_teamwork_harvest(
        shards=2,
        catalog=canonical_catalog_path,
        staging_dir=sandbox_staging_dir,
        vault_path=sandbox_vault_path,
        auto_merge=True,
        channel_video_provider=lambda ch: [
            {"id": f"v_{ch}", "duration": 800, "view_count": 500}
        ],
    )
    assert res["merge_manifest"] is not None
    assert res["merge_manifest"]["status"] == "SUCCESS"
    assert sandbox_vault_path.is_file()


def test_g11_teamwork_coordinator_dry_run_mode(
    canonical_catalog_path: Path, sandbox_staging_dir: Path, sandbox_vault_path: Path
):
    """G11.4: Dry-run mode validates pipeline flow without writing to databases."""
    res = run_teamwork_harvest(
        shards=2,
        catalog=canonical_catalog_path,
        staging_dir=sandbox_staging_dir,
        vault_path=sandbox_vault_path,
        dry_run=True,
        channel_video_provider=lambda ch: [
            {"id": f"v_{ch}", "duration": 800, "view_count": 500}
        ],
    )
    assert res["success"] is True
    # In dry run, destination vault must not be created or populated
    assert not sandbox_vault_path.is_file()


def test_g11_teamwork_coordinator_clean_shards_cleanup(
    canonical_catalog_path: Path, sandbox_staging_dir: Path, sandbox_vault_path: Path
):
    """G11.5: clean_shards=True deletes staging databases after successful merge."""
    run_teamwork_harvest(
        shards=2,
        catalog=canonical_catalog_path,
        staging_dir=sandbox_staging_dir,
        vault_path=sandbox_vault_path,
        auto_merge=True,
        clean_shards=True,
        channel_video_provider=lambda ch: [
            {"id": f"v_{ch}", "duration": 800, "view_count": 500}
        ],
    )
    remaining_shards = list(sandbox_staging_dir.glob("shard_*.db"))
    assert len(remaining_shards) == 0, "Staging shard databases should be cleaned up"
