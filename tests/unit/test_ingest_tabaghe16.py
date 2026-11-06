"""Unit tests for Tabaghe 16 ingestion pipeline."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from scripts.ingest_tabaghe16 import (
    count_persian_words,
    extract_persian_vtt,
    resolve_vault_path,
    run_ingestion,
)
from src.vault.db import VaultManager


def test_resolve_vault_path(tmp_path: Path):
    """Test resolution of database path with various options."""
    # 1. Explicit db_path
    custom_db = tmp_path / "custom" / "my_vault.db"
    assert resolve_vault_path(db_path=str(custom_db)) == custom_db
    assert custom_db.parent.is_dir()

    # 2. Explicit vault_dir
    custom_dir = tmp_path / "custom_dir"
    expected = custom_dir / "raw_vault.db"
    assert resolve_vault_path(vault_dir=str(custom_dir)) == expected
    assert custom_dir.is_dir()

    # 3. Fallback when Google Drive mount not found on Linux
    fallback = resolve_vault_path()
    assert fallback.name == "raw_vault.db"
    assert "language-app-raw-data" in str(fallback)


def test_count_persian_words():
    """Verify word count parsing excludes VTT headers, timestamps, and empty lines."""
    vtt_sample = """WEBVTT
Kind: captions
Language: fa

00:00:01.000 --> 00:00:05.000
سلام و درود به همه شنوندگان عزیز

00:00:05.500 --> 00:00:10.000
در این قسمت با مهمان برنامه گفتگو می‌کنیم
"""
    # 7 words in cue 1 + 8 words in cue 2 = 15 words
    assert count_persian_words(vtt_sample) == 15


def test_extract_persian_vtt_strict_filtering():
    """Test that extract_persian_vtt ignores auto-captions and non-Persian manual subtitles."""
    # Case 1: Video with only English subtitles
    with patch("yt_dlp.YoutubeDL") as mock_ydl_class:
        mock_ydl = MagicMock()
        mock_ydl_class.return_value.__enter__.return_value = mock_ydl
        mock_ydl.extract_info.return_value = {
            "subtitles": {"en": [{"ext": "vtt", "url": "https://yt.com/en.vtt"}]},
            "automatic_captions": {"fa": [{"ext": "vtt", "url": "https://yt.com/auto_fa.vtt"}]},
        }
        res = extract_persian_vtt("test_en_only")
        assert res is None  # Auto captions must be rejected, English manual must be ignored

    # Case 2: Video with manual Persian subtitles
    with patch("yt_dlp.YoutubeDL") as mock_ydl_class:
        mock_ydl = MagicMock()
        mock_ydl_class.return_value.__enter__.return_value = mock_ydl
        mock_ydl.extract_info.return_value = {
            "upload_date": "20240215",
            "tags": ["podcast", "interview"],
            "subtitles": {
                "en": [{"ext": "vtt", "url": "https://yt.com/en.vtt"}],
                "fa": [{"ext": "vtt", "url": "https://yt.com/fa.vtt"}],
            },
        }

        mock_vtt_data = "WEBVTT\n\n00:00.000 --> 00:05.000\nسلام دنیا".encode("utf-8")
        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_resp = MagicMock()
            mock_resp.read.return_value = mock_vtt_data
            mock_urlopen.return_value.__enter__.return_value = mock_resp

            res = extract_persian_vtt("test_fa_manual")
            assert res is not None
            vtt_text, url, info = res
            assert "سلام دنیا" in vtt_text
            assert url == "https://yt.com/fa.vtt"


def test_run_ingestion_end_to_end_mocked(tmp_path: Path):
    """Verify full ingestion workflow: duration filtering, manual CC filtering, and DB storage."""
    db_file = tmp_path / "mock_vault.db"

    mock_entries = [
        # Short teaser (< 600s) -> should be excluded by duration filter
        {"id": "short_001", "title": "Short Teaser", "duration": 59, "view_count": 5000},
        # Longform video with Persian CC -> should be ingested
        {"id": "long_001", "title": "Episode 100", "duration": 3600, "view_count": 80000},
        # Longform video without Persian CC -> should be skipped
        {"id": "long_002", "title": "Episode 101 No CC", "duration": 4200, "view_count": 60000},
    ]

    mock_vtt = "WEBVTT\n\n00:01.000 --> 00:06.000\nگفتگوی مفصل درباره فناوری و کارآفرینی"

    def mock_extract_vtt(vid_id: str):
        if vid_id == "long_001":
            return mock_vtt, "https://yt.com/vtt_100", {"upload_date": "20240101", "tags": ["tech"]}
        return None

    with patch("scripts.ingest_tabaghe16.extract_channel_videos", return_value=mock_entries):
        with patch("scripts.ingest_tabaghe16.extract_persian_vtt", side_effect=mock_extract_vtt):
            result = run_ingestion(
                db_path=str(db_file),
                min_duration=600,
                workers=2,
                dry_run=False,
                sync_gdrive=False,
            )

    assert result["total_scanned"] == 2  # Only the 2 longform videos are scanned
    assert result["newly_ingested"] == 1
    assert result["no_persian_cc"] == 1

    # Verify database contents
    with VaultManager(db_path=db_file) as vm:
        stats = vm.get_vault_stats()
        assert stats["total_sources"] == 1
        assert stats["total_items"] == 1
        assert stats["total_payloads"] == 1
        assert stats["payloads_by_language"]["fa"] == 1
        assert stats["payloads_by_origin"]["human_transcribed"] == 1

        item = vm.get_item("yt:long_001")
        assert item is not None
        assert item["duration_seconds"] == 3600

        # Verify FTS5 indexing works on the Persian transcript
        matches = vm.search_text("کارآفرینی")
        assert len(matches) == 1
        assert matches[0]["item_id"] == "yt:long_001"
