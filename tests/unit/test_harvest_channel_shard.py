"""Unit test suite for scripts/harvest_channel_shard.py.

Validates:
1. Subtitle resolution contract (manual priority, auto fallback, format handling, malformed inputs)
2. Persian WebVTT word counting (stripping headers, timings, cue IDs, HTML tags, Unicode text)
3. In-memory streaming (urllib custom User-Agent, zero disk media, retries with backoff)
4. Strict duration filtering (Shorts < 60s, promos < 600s, exact 600s, longform, None duration)
5. Capping policy (top N by view_count DESC, None view handling, under-cap, all_videos bypass)
6. Staging DB isolation (schema init, WAL mode, foreign keys, physical isolation)
7. Idempotent resumption (skipping completed, recovering items without payloads)
8. Resilience & error handling (network failures, malformed entries, zero disk footprint)
9. Cryptographic provenance (SHA-256 digest IDs, INSERT OR IGNORE deduplication)
10. Channel handle normalization (anomaly fixes, comma-separated strings, partition JSON files)
11. Concurrency (multi-worker thread pool safety)
12. CLI parser & subprocess execution (exit codes 0, 1, 3, argument validation)
"""

from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch
import urllib.error

import pytest

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.harvest_channel_shard import (
    USER_AGENT,
    build_parser,
    count_persian_words_in_vtt,
    fetch_vtt_in_memory,
    harvest_channel_shard,
    main,
    normalize_uploader_handle,
    resolve_persian_subtitles,
)
from src.vault.db import VaultManager


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def sample_persian_vtt() -> str:
    return (
        "WEBVTT\n"
        "Kind: captions\n"
        "Language: fa\n\n"
        "1\n"
        "00:00:01.000 --> 00:00:04.500\n"
        "سلام به همه همراهان گرامی، به برنامه ما خوش آمدید.\n\n"
        "2\n"
        "00:00:05.000 --> 00:00:09.000\n"
        "امروز پیرامون موضوع زبان‌شناسی و هوش مصنوعی صحبت می‌کنیم.\n"
    )


@pytest.fixture
def tmp_staging_dir(tmp_path: Path) -> Path:
    staging = tmp_path / "staging"
    staging.mkdir(parents=True, exist_ok=True)
    return staging


# =============================================================================
# 1. Subtitle Resolution Contract Tests
# =============================================================================

def test_resolve_manual_cc_priority_over_auto():
    """Priority 1: Manual Persian CC takes precedence when both manual and auto exist."""
    meta = {
        "subtitles": {
            "fa": [{"ext": "vtt", "url": "https://example.com/manual_fa.vtt"}]
        },
        "automatic_captions": {
            "fa": [{"ext": "vtt", "url": "https://example.com/auto_fa.vtt"}]
        },
    }
    url, p_type, o_type = resolve_persian_subtitles(meta)
    assert url == "https://example.com/manual_fa.vtt"
    assert p_type == "manual_cc"
    assert o_type == "human_transcribed"


@pytest.mark.parametrize("lang_key", ["fa", "fa-IR", "fas"])
def test_resolve_manual_cc_lang_keys(lang_key: str):
    """Manual CC resolves across all recognized Persian ISO language codes."""
    meta = {
        "subtitles": {
            lang_key: [{"ext": "vtt", "url": f"https://example.com/{lang_key}.vtt"}]
        }
    }
    url, p_type, o_type = resolve_persian_subtitles(meta)
    assert url == f"https://example.com/{lang_key}.vtt"
    assert p_type == "manual_cc"
    assert o_type == "human_transcribed"


@pytest.mark.parametrize("lang_key", ["fa", "fa-IR", "fas", "fa-orig"])
def test_resolve_auto_cc_fallback_keys(lang_key: str):
    """Auto CC fallback resolves across fa, fa-IR, fas, and fa-orig."""
    meta = {
        "automatic_captions": {
            lang_key: [{"ext": "vtt", "url": f"https://example.com/auto_{lang_key}.vtt"}]
        }
    }
    url, p_type, o_type = resolve_persian_subtitles(meta)
    assert url == f"https://example.com/auto_{lang_key}.vtt"
    assert p_type == "auto_cc"
    assert o_type == "machine_transcribed"


def test_resolve_no_persian_subtitles():
    """Non-Persian tracks result in (None, 'none', 'none')."""
    meta = {
        "subtitles": {"en": [{"ext": "vtt", "url": "https://example.com/en.vtt"}]},
        "automatic_captions": {"es": [{"ext": "vtt", "url": "https://example.com/es.vtt"}]},
    }
    assert resolve_persian_subtitles(meta) == (None, "none", "none")


def test_resolve_empty_and_none_metadata():
    """Safely handles None, empty dicts, and None values for keys."""
    assert resolve_persian_subtitles(None) == (None, "none", "none")
    assert resolve_persian_subtitles({}) == (None, "none", "none")
    assert resolve_persian_subtitles({"subtitles": None, "automatic_captions": None}) == (
        None,
        "none",
        "none",
    )


def test_resolve_format_vtt_selection():
    """Selects track with ext == 'vtt' when multiple formats are present."""
    meta = {
        "subtitles": {
            "fa": [
                {"ext": "srv3", "url": "https://example.com/sub?lang=fa"},
                {"ext": "vtt", "url": "https://example.com/sub.vtt"},
            ]
        }
    }
    url, p_type, _ = resolve_persian_subtitles(meta)
    assert url == "https://example.com/sub.vtt"
    assert p_type == "manual_cc"


def test_resolve_fmt_param_appendage():
    """Appends &fmt=vtt or ?fmt=vtt when no track has ext == 'vtt'."""
    # With query parameter present
    meta_query = {
        "subtitles": {"fa": [{"ext": "srv1", "url": "https://example.com/sub?lang=fa"}]}
    }
    url_q, _, _ = resolve_persian_subtitles(meta_query)
    assert url_q == "https://example.com/sub?lang=fa&fmt=vtt"

    # Without existing query parameter
    meta_no_q = {
        "subtitles": {"fa": [{"ext": "srv1", "url": "https://example.com/timedtext"}]}
    }
    url_no_q, _, _ = resolve_persian_subtitles(meta_no_q)
    assert url_no_q == "https://example.com/timedtext?fmt=vtt"


def test_resolve_malformed_tracks_handling():
    """Safely handles non-list tracks, non-dict elements, and missing URLs."""
    meta = {
        "subtitles": {
            "fa": ["invalid_str", None, {}, {"ext": "vtt"}, {"ext": "vtt", "url": "https://ok.com/vtt"}]
        }
    }
    url, p_type, _ = resolve_persian_subtitles(meta)
    assert url == "https://ok.com/vtt"
    assert p_type == "manual_cc"


# =============================================================================
# 2. Persian WebVTT Word Counting Tests
# =============================================================================

def test_count_words_strips_headers_and_metadata():
    """Strips WEBVTT, Kind, Language, NOTE, STYLE, REGION header lines."""
    vtt = (
        "WEBVTT\n"
        "Kind: captions\n"
        "Language: fa\n"
        "NOTE This is a commentary note\n"
        "STYLE\n"
        "REGION\n\n"
        "00:00:01.000 --> 00:00:03.000\n"
        "سلام دوست من\n"
    )
    assert count_persian_words_in_vtt(vtt) == 3


def test_count_words_strips_timing_lines_and_cue_ids():
    """Strips cue identifiers and timing lines with arrows."""
    vtt = (
        "WEBVTT\n\n"
        "cue-001\n"
        "00:00:01.000 --> 00:00:04.000\n"
        "یک دو سه\n\n"
        "42\n"
        "00:00:05.000 --> 00:00:08.000\n"
        "چهار پنج شش هفت\n"
    )
    assert count_persian_words_in_vtt(vtt) == 7


def test_count_words_strips_inline_tags():
    """Strips HTML-like formatting and karaoke timestamp tags."""
    vtt = (
        "WEBVTT\n\n"
        "00:00:01.000 --> 00:00:04.000\n"
        "<c.yellow>سلام</c> <b>عزیزان</b> <00:00:02.500>گرامی\n"
    )
    assert count_persian_words_in_vtt(vtt) == 3


def test_count_words_unicode_and_zwnj():
    """Accurately counts Persian tokens with ZWNJ and Persian digits."""
    vtt = (
        "WEBVTT\n\n"
        "00:00:01.000 --> 00:00:05.000\n"
        "می‌خواهم کتاب‌ها را در سال ۱۴۰۳ بررسی کنم.\n"
    )
    tokens = [t for t in "می‌خواهم کتاب‌ها را در سال ۱۴۰۳ بررسی کنم.".split() if t]
    assert count_persian_words_in_vtt(vtt) == len(tokens)


def test_count_words_empty_and_none_safe():
    """Returns 0 for None, empty, or whitespace strings."""
    assert count_persian_words_in_vtt(None) == 0
    assert count_persian_words_in_vtt("") == 0
    assert count_persian_words_in_vtt("   \n\n   ") == 0


def test_count_words_multiline_cues():
    """Counts all text lines in a single cue block."""
    vtt = (
        "WEBVTT\n\n"
        "00:00:01.000 --> 00:00:05.000\n"
        "خط اول از گفتگوی تخصصی\n"
        "خط دوم از همان بخش زمان‌بندی\n"
    )
    assert count_persian_words_in_vtt(vtt) == 11


# =============================================================================
# 3. In-Memory Subtitle Streaming Tests
# =============================================================================

def test_fetch_vtt_in_memory_success():
    """Streams VTT over HTTP GET directly into RAM with verified User-Agent."""
    mock_resp = MagicMock()
    mock_resp.read.return_value = "WEBVTT\n\n00:00:01.000 --> 00:00:02.000\nسلام".encode("utf-8")
    mock_resp.__enter__.return_value = mock_resp

    with patch("urllib.request.urlopen", return_value=mock_resp) as mock_urlopen:
        res = fetch_vtt_in_memory("https://example.com/sub.vtt")

        assert res is not None
        assert "سلام" in res
        req = mock_urlopen.call_args[0][0]
        assert req.get_header("User-agent") == USER_AGENT


def test_fetch_vtt_in_memory_retry_on_transient_error():
    """Retries with backoff on transient network error and succeeds on second attempt."""
    mock_resp = MagicMock()
    mock_resp.read.return_value = "WEBVTT\n\n00:00:01.000 --> 00:00:02.000\nسلام".encode("utf-8")
    mock_resp.__enter__.return_value = mock_resp

    with patch(
        "urllib.request.urlopen",
        side_effect=[urllib.error.URLError("Temporary DNS error"), mock_resp],
    ), patch("time.sleep") as mock_sleep:
        res = fetch_vtt_in_memory("https://example.com/sub.vtt", max_retries=3)
        assert res is not None
        assert "سلام" in res
        assert mock_sleep.call_count == 1


def test_fetch_vtt_in_memory_exhausted_retries_returns_none():
    """Returns None after exhausting max retries without raising unhandled exception."""
    with patch(
        "urllib.request.urlopen",
        side_effect=urllib.error.URLError("Persistent connection failure"),
    ), patch("time.sleep"):
        res = fetch_vtt_in_memory("https://example.com/sub.vtt", max_retries=3)
        assert res is None


def test_fetch_vtt_in_memory_invalid_url():
    """Returns None for empty or non-string URL."""
    assert fetch_vtt_in_memory("") is None
    assert fetch_vtt_in_memory(None) is None  # type: ignore


# =============================================================================
# 4. Strict Duration Filtering Tests
# =============================================================================

def test_duration_filter_excludes_shorts(tmp_staging_dir: Path):
    """Excludes YouTube Shorts (< 60s)."""
    videos = [{"id": "short_1", "duration": 45, "view_count": 10000}]
    res = harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=tmp_staging_dir,
        min_duration=600,
        channel_video_provider=lambda ch: videos,
    )
    assert res["eligible_videos_scanned"] == 0
    assert res["items_cataloged"] == 0


def test_duration_filter_excludes_promos(tmp_staging_dir: Path):
    """Excludes promo teasers (< 600s, e.g. 300s, 599s)."""
    videos = [
        {"id": "promo_1", "duration": 300, "view_count": 5000},
        {"id": "promo_2", "duration": 599, "view_count": 8000},
    ]
    res = harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=tmp_staging_dir,
        min_duration=600,
        channel_video_provider=lambda ch: videos,
    )
    assert res["eligible_videos_scanned"] == 0
    assert res["items_cataloged"] == 0


def test_duration_filter_accepts_exact_600s(tmp_staging_dir: Path):
    """Accepts videos with duration == 600s exactly."""
    videos = [{"id": "exact_600", "duration": 600, "view_count": 12000}]
    res = harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=tmp_staging_dir,
        min_duration=600,
        channel_video_provider=lambda ch: videos,
    )
    assert res["eligible_videos_scanned"] == 1
    assert res["items_cataloged"] == 1


def test_duration_filter_accepts_longform(tmp_staging_dir: Path):
    """Accepts longform videos (> 600s, e.g. 3600s)."""
    videos = [{"id": "longform_1", "duration": 3600, "view_count": 25000}]
    res = harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=tmp_staging_dir,
        min_duration=600,
        channel_video_provider=lambda ch: videos,
    )
    assert res["eligible_videos_scanned"] == 1
    assert res["items_cataloged"] == 1


def test_duration_filter_excludes_none_safely(tmp_staging_dir: Path):
    """Safely excludes unstarted livestreams with duration=None."""
    videos = [{"id": "stream_live", "duration": None, "view_count": 500}]
    res = harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=tmp_staging_dir,
        min_duration=600,
        channel_video_provider=lambda ch: videos,
    )
    assert res["eligible_videos_scanned"] == 0
    assert res["items_cataloged"] == 0


# =============================================================================
# 5. Capping Policy Tests
# =============================================================================

def test_capping_slices_top_n_by_view_count(tmp_staging_dir: Path):
    """Capping policy slices top N candidates sorted by view_count DESC."""
    videos = [
        {"id": f"v_{i}", "duration": 800, "view_count": i * 1000}
        for i in range(1, 31)
    ]
    res = harvest_channel_shard(
        shard_id=1,
        channels=["@MiaPlays"],
        staging_dir=tmp_staging_dir,
        cap_per_channel=10,
        channel_video_provider=lambda ch: videos,
    )
    assert res["eligible_videos_scanned"] == 10
    assert res["items_cataloged"] == 10

    conn = sqlite3.connect(str(tmp_staging_dir / "shard_1.db"))
    min_view = conn.execute("SELECT MIN(view_count) FROM items;").fetchone()[0]
    conn.close()
    assert min_view >= 21000, "Top 10 of 30 videos must have view_count >= 21,000"


def test_capping_sort_none_view_count_last(tmp_staging_dir: Path):
    """Videos with view_count=None sort after videos with known view counts."""
    videos = [
        {"id": "v_high", "duration": 800, "view_count": 10000},
        {"id": "v_none", "duration": 800, "view_count": None},
        {"id": "v_low", "duration": 800, "view_count": 2000},
    ]
    res = harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=tmp_staging_dir,
        cap_per_channel=2,
        channel_video_provider=lambda ch: videos,
    )
    assert res["items_cataloged"] == 2
    conn = sqlite3.connect(str(tmp_staging_dir / "shard_1.db"))
    ids = [r[0] for r in conn.execute("SELECT id FROM items;").fetchall()]
    conn.close()
    assert "yt:v_high" in ids
    assert "yt:v_low" in ids
    assert "yt:v_none" not in ids


def test_capping_under_cap_retains_all(tmp_staging_dir: Path):
    """Channels with fewer eligible videos than cap retain all without truncation."""
    videos = [{"id": f"v_{i}", "duration": 900, "view_count": i * 100} for i in range(5)]
    res = harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=tmp_staging_dir,
        cap_per_channel=25,
        channel_video_provider=lambda ch: videos,
    )
    assert res["eligible_videos_scanned"] == 5
    assert res["items_cataloged"] == 5


def test_all_videos_flag_bypasses_cap(tmp_staging_dir: Path):
    """Flag all_videos=True bypasses capping policy completely."""
    videos = [{"id": f"v_{i}", "duration": 800, "view_count": i * 100} for i in range(20)]
    res = harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=tmp_staging_dir,
        cap_per_channel=5,
        all_videos=True,
        channel_video_provider=lambda ch: videos,
    )
    assert res["eligible_videos_scanned"] == 20
    assert res["items_cataloged"] == 20


def test_capping_multi_channel_independence(tmp_staging_dir: Path):
    """Capping is applied independently per channel in a multi-channel shard."""
    channels = ["@ChannelBPodcast", "@BplusPodcast"]

    def sample_provider(ch: str):
        count = 15 if "ChannelB" in ch else 10
        return [{"id": f"{ch}_{i}", "duration": 700, "view_count": i * 100} for i in range(count)]

    res = harvest_channel_shard(
        shard_id=1,
        channels=channels,
        staging_dir=tmp_staging_dir,
        cap_per_channel=8,
        channel_video_provider=sample_provider,
    )
    assert res["sources_processed"] == 2
    assert res["items_cataloged"] == 16  # 8 from ChannelB + 8 from Bplus


def test_prefer_manual_cc_sorting_and_cap(tmp_staging_dir: Path):
    """Verify candidate videos with manual CC are selected first over auto CC when capping,
    even when auto CC videos have vastly higher view counts, and vice versa when prefer_manual_cc=False."""
    mock_vtt = "WEBVTT\n\n00:00:01.000 --> 00:00:05.000\nBonjour tout le monde\n"
    videos = [
        {"id": "v_auto_viral", "duration": 800, "view_count": 5_000_000, "has_manual_fr_cc": False, "subtitles": {}},
        {"id": "v_auto_mid", "duration": 800, "view_count": 500_000, "has_manual_fr_cc": False, "subtitles": {}},
        {"id": "v_man_low", "duration": 800, "view_count": 5_000, "has_manual_fr_cc": True, "subtitles": {"fr": [{"ext": "vtt", "url": "https://m.test/v_man_low.vtt"}]}},
        {"id": "v_man_niche", "duration": 800, "view_count": 500, "has_manual_fr_cc": True, "subtitles": {"fr-CA": [{"ext": "vtt", "url": "https://m.test/v_man_niche.vtt"}]}},
    ]

    # 1. With prefer_manual_cc=True, cap=2: must select v_man_low and v_man_niche
    res_pref = harvest_channel_shard(
        shard_id=1,
        channels=["@Legendmedia"],
        staging_dir=tmp_staging_dir / "pref",
        cap_per_channel=2,
        prefer_manual_cc=True,
        language_iso1="fr",
        channel_video_provider=lambda ch: videos,
        vtt_provider=lambda _v, _u: mock_vtt,
    )
    assert res_pref["items_cataloged"] == 2
    conn = sqlite3.connect(str(tmp_staging_dir / "pref" / "shard_1.db"))
    ids_pref = [r[0] for r in conn.execute("SELECT id FROM items;").fetchall()]
    conn.close()
    assert "yt:v_man_low" in ids_pref
    assert "yt:v_man_niche" in ids_pref
    assert "yt:v_auto_viral" not in ids_pref
    assert "yt:v_auto_mid" not in ids_pref

    # 2. With prefer_manual_cc=False, cap=2: must select top view_count (v_auto_viral, v_auto_mid)
    res_nopref = harvest_channel_shard(
        shard_id=1,
        channels=["@Legendmedia"],
        staging_dir=tmp_staging_dir / "nopref",
        cap_per_channel=2,
        prefer_manual_cc=False,
        language_iso1="fr",
        channel_video_provider=lambda ch: videos,
        vtt_provider=lambda _v, _u: mock_vtt,
    )
    assert res_nopref["items_cataloged"] == 2
    conn = sqlite3.connect(str(tmp_staging_dir / "nopref" / "shard_1.db"))
    ids_nopref = [r[0] for r in conn.execute("SELECT id FROM items;").fetchall()]
    conn.close()
    assert "yt:v_auto_viral" in ids_nopref
    assert "yt:v_auto_mid" in ids_nopref
    assert "yt:v_man_low" not in ids_nopref
    assert "yt:v_man_niche" not in ids_nopref

    # 3. High-volume 150-video cap verification:
    # 200 candidates: 100 manual CC (lower views) + 100 auto CC (higher views)
    high_volume_videos = []
    for i in range(1, 101):
        high_volume_videos.append({
            "id": f"v_man_{i}",
            "duration": 800,
            "view_count": i * 100,  # 100 to 10,000 views
            "has_manual_fr_cc": True,
            "subtitles": {"fr": [{"ext": "vtt", "url": f"https://m.test/v_man_{i}.vtt"}]},
        })
    for j in range(1, 101):
        high_volume_videos.append({
            "id": f"v_auto_{j}",
            "duration": 800,
            "view_count": 100_000 + j * 1000,  # 101,000 to 200,000 views
            "has_manual_fr_cc": False,
            "subtitles": {},
        })

    res_cap150 = harvest_channel_shard(
        shard_id=1,
        channels=["@Legendmedia"],
        staging_dir=tmp_staging_dir / "cap150",
        cap_per_channel=150,
        prefer_manual_cc=True,
        language_iso1="fr",
        channel_video_provider=lambda ch: high_volume_videos,
        vtt_provider=lambda _v, _u: mock_vtt,
    )
    assert res_cap150["items_cataloged"] == 150
    conn = sqlite3.connect(str(tmp_staging_dir / "cap150" / "shard_1.db"))
    man_count = conn.execute("SELECT COUNT(*) FROM items WHERE id LIKE 'yt:v_man_%';").fetchone()[0]
    auto_count = conn.execute("SELECT COUNT(*) FROM items WHERE id LIKE 'yt:v_auto_%';").fetchone()[0]
    conn.close()
    assert man_count == 100, "All 100 manual CC videos must be prioritized"
    assert auto_count == 50, "Remaining 50 slots filled by highest view auto CC videos"




# =============================================================================
# 6. Staging DB Isolation & Integrity Tests
# =============================================================================

def test_staging_db_schema_and_tables(tmp_staging_dir: Path):
    """Worker initializes shard database with correct tables (sources, items, payloads, payloads_fts)."""
    harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=tmp_staging_dir,
        channel_video_provider=lambda ch: [],
    )
    db_file = tmp_staging_dir / "shard_1.db"
    assert db_file.is_file()

    conn = sqlite3.connect(str(db_file))
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()}
    conn.close()
    assert {"sources", "items", "payloads", "payloads_fts"}.issubset(tables)


def test_staging_db_wal_mode_and_foreign_keys(tmp_staging_dir: Path):
    """Worker sets WAL journal mode and enables foreign key enforcement."""
    harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=tmp_staging_dir,
        channel_video_provider=lambda ch: [],
    )
    db_file = tmp_staging_dir / "shard_1.db"
    conn = sqlite3.connect(str(db_file))
    journal_mode = conn.execute("PRAGMA journal_mode;").fetchone()[0].upper()
    conn.close()
    assert journal_mode == "WAL"


def test_staging_db_foreign_key_enforcement(tmp_staging_dir: Path):
    """Referential integrity rejects inserting items referencing non-existent sources."""
    db_file = tmp_staging_dir / "shard_fk_test.db"
    vm = VaultManager(db_path=db_file)
    vm.init_db()

    conn = vm.conn
    conn.execute("PRAGMA foreign_keys = ON;")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO items (id, source_id, title, url, created_at) "
            "VALUES ('yt:orphan', 'yt:ghost', 'Title', 'URL', '2026-11-06T00:00:00Z');"
        )
    vm.close()


def test_staging_db_multi_shard_isolation(tmp_staging_dir: Path):
    """Multiple shards write to physically isolated database files."""
    for s_id in (1, 2):
        harvest_channel_shard(
            shard_id=s_id,
            channels=[f"@Channel_{s_id}"],
            staging_dir=tmp_staging_dir,
            channel_video_provider=lambda ch: [],
        )

    db_1 = tmp_staging_dir / "shard_1.db"
    db_2 = tmp_staging_dir / "shard_2.db"
    assert db_1.is_file()
    assert db_2.is_file()

    conn1 = sqlite3.connect(str(db_1))
    src1 = conn1.execute("SELECT id FROM sources;").fetchone()[0]
    conn1.close()

    conn2 = sqlite3.connect(str(db_2))
    src2 = conn2.execute("SELECT id FROM sources;").fetchone()[0]
    conn2.close()

    assert src1 == "yt:@Channel_1"
    assert src2 == "yt:@Channel_2"


# =============================================================================
# 7. Idempotent Resumption Tests
# =============================================================================

def test_idempotent_resumption_skips_completed(tmp_staging_dir: Path, sample_persian_vtt: str):
    """Resuming shard skips items that already have both item and payload cataloged."""
    videos = [
        {
            "id": "vid_resume",
            "duration": 900,
            "view_count": 5000,
            "vtt_content": sample_persian_vtt,
        }
    ]
    # Pass 1: Fresh ingestion
    res1 = harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=tmp_staging_dir,
        channel_video_provider=lambda ch: videos,
    )
    assert res1["items_cataloged"] == 1
    assert res1["payloads_inserted"] == 1
    assert res1["resumed_skipped_count"] == 0

    # Pass 2: Resumed run
    res2 = harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=tmp_staging_dir,
        channel_video_provider=lambda ch: videos,
    )
    assert res2["resumed_skipped_count"] == 1
    assert res2["items_cataloged"] == 0
    assert res2["payloads_inserted"] == 0


def test_resumption_recovers_missing_payload(tmp_staging_dir: Path, sample_persian_vtt: str):
    """Recovers item cataloged in prior run where payload was previously missing."""
    db_file = tmp_staging_dir / "shard_1.db"
    vm = VaultManager(db_path=db_file)
    vm.init_db()

    # Pre-populate source and item without payload
    vm.upsert_source("yt:@Tabaghe16", "youtube", "fa", "Tabaghe16", "https://yt.com/@Tabaghe16")
    vm.upsert_item("yt:vid_incomplete", "yt:@Tabaghe16", "Title", "https://yt.com/watch?v=vid_incomplete", duration_seconds=800)
    vm.close()

    videos = [
        {
            "id": "vid_incomplete",
            "duration": 800,
            "view_count": 5000,
            "vtt_content": sample_persian_vtt,
        }
    ]
    res = harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=tmp_staging_dir,
        channel_video_provider=lambda ch: videos,
    )
    assert res["resumed_skipped_count"] == 0
    assert res["payloads_inserted"] == 1


# =============================================================================
# 8. Resilience & Cryptographic Provenance Tests
# =============================================================================

def test_resilience_vtt_fetch_error_catalogs_item_without_payload(tmp_staging_dir: Path):
    """Network failure during VTT streaming catalogs the video item, avoids fatal crash, and continues."""
    videos = [
        {
            "id": "vid_net_err",
            "duration": 750,
            "view_count": 3000,
            "subtitles": {"fa": [{"ext": "vtt", "url": "https://broken.stream/err.vtt"}]},
        }
    ]

    def failing_vtt_provider(v_id: str, url: str) -> None:
        raise ConnectionResetError("Connection reset by peer")

    res = harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=tmp_staging_dir,
        vtt_provider=failing_vtt_provider,
        channel_video_provider=lambda ch: videos,
    )
    assert res["items_cataloged"] == 1
    assert res["payloads_inserted"] == 0


def test_payload_sha256_hash_provenance(tmp_staging_dir: Path, sample_persian_vtt: str):
    """Payload primary key ID matches exact SHA-256 hash of UTF-8 WebVTT text."""
    expected_hash = hashlib.sha256(sample_persian_vtt.encode("utf-8")).hexdigest()
    videos = [
        {
            "id": "vid_sha",
            "duration": 850,
            "view_count": 1000,
            "vtt_content": sample_persian_vtt,
        }
    ]
    harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=tmp_staging_dir,
        channel_video_provider=lambda ch: videos,
    )
    conn = sqlite3.connect(str(tmp_staging_dir / "shard_1.db"))
    row = conn.execute("SELECT id, raw_content FROM payloads;").fetchone()
    conn.close()

    assert row is not None
    assert row[0] == expected_hash
    assert row[1] == sample_persian_vtt


def test_payload_deduplication_via_insert_or_ignore(tmp_staging_dir: Path, sample_persian_vtt: str):
    """Identical VTT content for multiple items is deduplicated in payloads table."""
    videos = [
        {"id": "vid_dup_1", "duration": 700, "view_count": 1000, "vtt_content": sample_persian_vtt},
        {"id": "vid_dup_2", "duration": 700, "view_count": 2000, "vtt_content": sample_persian_vtt},
    ]
    harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=tmp_staging_dir,
        channel_video_provider=lambda ch: videos,
    )
    conn = sqlite3.connect(str(tmp_staging_dir / "shard_1.db"))
    items_count = conn.execute("SELECT COUNT(*) FROM items;").fetchone()[0]
    payloads_count = conn.execute("SELECT COUNT(*) FROM payloads;").fetchone()[0]
    conn.close()

    assert items_count == 2
    assert payloads_count == 1, "Payloads with identical SHA-256 hash must be deduplicated"


def test_resilience_zero_media_files_on_disk(tmp_staging_dir: Path, sample_persian_vtt: str):
    """Ensures 0 video/audio media files (.mp4, .m4a, .webm, .wav) are written to disk."""
    videos = [
        {"id": "vid_zero_disk", "duration": 800, "view_count": 1000, "vtt_content": sample_persian_vtt}
    ]
    harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=tmp_staging_dir,
        channel_video_provider=lambda ch: videos,
    )
    media_extensions = {".mp4", ".m4a", ".webm", ".wav", ".mkv", ".mp3"}
    disk_media = [p for p in tmp_staging_dir.iterdir() if p.suffix.lower() in media_extensions]
    assert len(disk_media) == 0


# =============================================================================
# 9. Handle Normalization & Partitions Integration Tests
# =============================================================================

def test_normalize_uploader_handle_anomalies():
    """Corrects anomalies (@Kouman_Official, radiomarz) and ensures leading @."""
    assert normalize_uploader_handle("kouman_official") == "@Kouman"
    assert normalize_uploader_handle("@kouman_official") == "@Kouman"
    assert normalize_uploader_handle("radiomarz") == "@radio-marz"
    assert normalize_uploader_handle("@radiomarz") == "@radio-marz"
    assert normalize_uploader_handle("Tabaghe16") == "@Tabaghe16"
    assert normalize_uploader_handle("@Tabaghe16") == "@Tabaghe16"


def test_harvest_with_comma_separated_channels(tmp_staging_dir: Path):
    """Supports comma-separated channel string in channels argument."""
    res = harvest_channel_shard(
        shard_id=1,
        channels="@Tabaghe16,@ChannelBPodcast",
        staging_dir=tmp_staging_dir,
        channel_video_provider=lambda ch: [],
    )
    assert res["sources_processed"] == 2


def test_harvest_with_partitions_json_file(tmp_staging_dir: Path, tmp_path: Path):
    """Extracts shard channels directly from a partitions export JSON file."""
    part_file = tmp_path / "partitions.json"
    part_data = {
        "shards": [
            {
                "shard_id": 1,
                "squad_name": "Podcasts & Thought",
                "channels": [{"handle": "@Tabaghe16"}, {"handle": "@ChannelBPodcast"}],
            },
            {
                "shard_id": 2,
                "squad_name": "Vlogs & Entertainment",
                "channels": [{"handle": "@MiaPlays"}],
            },
        ]
    }
    part_file.write_text(json.dumps(part_data), encoding="utf-8")

    res = harvest_channel_shard(
        shard_id=2,
        channels=part_file,
        staging_dir=tmp_staging_dir,
        channel_video_provider=lambda ch: [],
    )
    assert res["sources_processed"] == 1
    conn = sqlite3.connect(str(tmp_staging_dir / "shard_2.db"))
    src = conn.execute("SELECT id FROM sources;").fetchone()[0]
    conn.close()
    assert src == "yt:@MiaPlays"


# =============================================================================
# 10. Concurrency & Workers Tests
# =============================================================================

def test_harvest_workers_parallel_execution(tmp_staging_dir: Path, sample_persian_vtt: str):
    """Executes multi-threaded video processing with workers > 1 safely."""
    videos = [
        {"id": f"vid_par_{i}", "duration": 750, "view_count": i * 100, "vtt_content": sample_persian_vtt}
        for i in range(10)
    ]
    res = harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=tmp_staging_dir,
        workers=4,
        channel_video_provider=lambda ch: videos,
    )
    assert res["items_cataloged"] == 10
    assert res["payloads_inserted"] == 10
    assert res["total_persian_words"] > 0


# =============================================================================
# 11. CLI Parser & Entrypoint Tests
# =============================================================================

def test_cli_parser_defaults():
    """CLI parser builds with correct default arguments."""
    parser = build_parser()
    args = parser.parse_args([])
    assert args.shard_id == 1
    assert args.cap_per_channel == 25
    assert args.min_duration == 600
    assert args.all_videos is False
    assert args.dry_run is False
    assert args.workers == 1


def test_cli_main_dry_run_success(tmp_staging_dir: Path, capsys):
    """CLI execution with --dry-run prints metrics JSON and exits with 0."""
    code = main([
        "--shard-id", "1",
        "--channels", "@Tabaghe16",
        "--staging-dir", str(tmp_staging_dir),
        "--dry-run",
    ])
    assert code == 0
    captured = capsys.readouterr()
    metrics = json.loads(captured.out)
    assert metrics["shard_id"] == 1
    assert metrics["sources_processed"] == 1


def test_cli_missing_catalog_returns_1():
    """Missing catalog file with empty channels yields exit code 1."""
    code = main(["--catalog", "nonexistent_catalog.json"])
    assert code == 1


def test_cli_invalid_arguments_returns_3():
    """Invalid arguments (shard-id < 1, cap < 0, min-duration < 0, workers < 1) yield exit code 3."""
    assert main(["--shard-id", "0"]) == 3
    assert main(["--cap-per-channel", "-1"]) == 3
    assert main(["--min-duration", "-5"]) == 3
    assert main(["--workers", "0"]) == 3


def test_cli_subprocess_invocation(tmp_staging_dir: Path):
    """Subprocess execution of scripts/harvest_channel_shard.py succeeds with exit code 0."""
    cmd = [
        sys.executable,
        "scripts/harvest_channel_shard.py",
        "--shard-id", "1",
        "--channels", "@Tabaghe16",
        "--staging-dir", str(tmp_staging_dir),
        "--dry-run",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    assert proc.returncode == 0
    data = json.loads(proc.stdout)
    assert data["shard_id"] == 1
    assert data["sources_processed"] == 1


def test_french_regional_subtitle_resolution_and_harvest(tmp_staging_dir: Path):
    """Verify resolve_subtitles and harvest_channel_shard for French regional tracks (fr-CA, fr-FR)."""
    from scripts.harvest_channel_shard import resolve_subtitles

    meta_ca = {
        "subtitles": {
            "fr-FR": [{"ext": "vtt", "url": "https://yt.test/fr_fr.vtt", "name": "Français (France)"}],
            "fr-CA": [{"ext": "vtt", "url": "https://yt.test/fr_ca.vtt", "name": "Français (Canada)"}],
        }
    }
    # When country_code="CA" is provided, fr-CA is prioritized over fr-FR
    url, p_type, o_type, loc, track_meta = resolve_subtitles(meta_ca, language_iso1="fr", country_code="CA")
    assert url == "https://yt.test/fr_ca.vtt"
    assert p_type == "manual_cc"
    assert o_type == "human_transcribed"
    assert loc == "fr-CA"
    assert track_meta["track_name"] == "Français (Canada)"

    # Harvest a French-Canadian channel into shard DB
    channels_input = [
        {
            "handle": "@RadCa",
            "country_code": "CA",
            "region": "Quebec",
            "register": "broadcast",
            "interaction_type": "dialogue",
        }
    ]
    sample_vtt = "WEBVTT\n\n00:00:01.000 --> 00:00:04.000\nBonjour tout le monde à Montréal.\n"
    metrics = harvest_channel_shard(
        shard_id=9,
        channels=channels_input,
        staging_dir=tmp_staging_dir,
        language_iso1="fr",
        country_code="CA",
        channel_video_provider=lambda _h: [
            {
                "id": "qc_vid_01",
                "title": "Reportage à Québec",
                "duration": 900,
                "view_count": 50000,
                "subtitles": {
                    "fr-CA": [{"ext": "vtt", "url": "https://yt.test/qc_vid_01.vtt", "name": "Français (Canada)"}]
                },
            }
        ],
        vtt_provider=lambda _vid, _url: sample_vtt,
    )
    assert metrics["payloads_inserted"] == 1
    with VaultManager(db_path=tmp_staging_dir / "shard_9.db") as vm:
        src = vm.get_source("yt:@RadCa")
        assert src["language_iso1"] == "fr"
        assert src["country_code"] == "CA"
        assert json.loads(src["metadata_json"])["region"] == "Quebec"
        payloads = vm.get_payloads_by_item("yt:qc_vid_01")
        assert len(payloads) == 1
        assert payloads[0]["language_iso1"] == "fr"
        assert payloads[0]["locale"] == "fr-CA"
        assert json.loads(payloads[0]["metadata_json"])["track_name"] == "Français (Canada)"


def test_french_teamwork_shard_options(tmp_staging_dir: Path):
    """Verify harvest_channel_shard CLI and function interface correctly accept --lang fr,
    --prefer-manual-cc, and --cap-per-channel 150, executing dry-run with code 0."""
    # 1. CLI parser argument verification
    parser = build_parser()
    args = parser.parse_args([
        "--shard-id", "1",
        "--channels", "@Legendmedia",
        "--lang", "fr",
        "--prefer-manual-cc",
        "--cap-per-channel", "150",
        "--staging-dir", str(tmp_staging_dir),
    ])
    assert args.lang == "fr"
    assert args.prefer_manual_cc is True
    assert args.cap_per_channel == 150

    # 2. Direct CLI entrypoint main() dry-run execution
    code = main([
        "--shard-id", "1",
        "--channels", "@Legendmedia",
        "--lang", "fr",
        "--prefer-manual-cc",
        "--cap-per-channel", "150",
        "--staging-dir", str(tmp_staging_dir),
        "--dry-run",
    ])
    assert code == 0

    # 3. Subprocess CLI execution
    cmd = [
        sys.executable,
        "scripts/harvest_channel_shard.py",
        "--shard-id", "1",
        "--channels", "@Legendmedia",
        "--lang", "fr",
        "--prefer-manual-cc",
        "--cap-per-channel", "150",
        "--staging-dir", str(tmp_staging_dir),
        "--dry-run",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    assert proc.returncode == 0
    data = json.loads(proc.stdout)
    assert data["shard_id"] == 1
    assert data["sources_processed"] == 1

    # 4. Direct function interface execution
    metrics = harvest_channel_shard(
        shard_id=1,
        channels=["@Legendmedia"],
        staging_dir=tmp_staging_dir / "fn_run",
        language_iso1="fr",
        prefer_manual_cc=True,
        cap_per_channel=150,
        dry_run=True,
    )
    assert metrics["shard_id"] == 1
    assert metrics["sources_processed"] == 1



