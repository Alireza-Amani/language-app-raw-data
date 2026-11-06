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


def test_extract_persian_vtt_manual_priority():
    """Test that manual CC takes priority and is tagged as human_transcribed."""
    with patch("yt_dlp.YoutubeDL") as mock_ydl_class:
        mock_ydl = MagicMock()
        mock_ydl_class.return_value.__enter__.return_value = mock_ydl
        mock_ydl.extract_info.return_value = {
            "upload_date": "20240215",
            "tags": ["podcast"],
            "subtitles": {
                "fa": [{"ext": "vtt", "url": "https://yt.com/manual_fa.vtt"}],
            },
            "automatic_captions": {
                "fa": [{"ext": "vtt", "url": "https://yt.com/auto_fa.vtt"}],
            },
        }

        mock_vtt_data = "WEBVTT\n\n00:00.000 --> 00:05.000\nمتن دستی".encode("utf-8")
        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_resp = MagicMock()
            mock_resp.read.return_value = mock_vtt_data
            mock_urlopen.return_value.__enter__.return_value = mock_resp

            res = extract_persian_vtt("test_priority", include_auto_cc=True)
            vtt_text, url, info, p_type, origin_type = res
            assert vtt_text is not None
            assert p_type == "manual_cc"
            assert origin_type == "human_transcribed"
            assert url == "https://yt.com/manual_fa.vtt"


def test_extract_persian_vtt_auto_cc_fallback():
    """Test that auto-captions are used as fallback when manual CC is missing and include_auto_cc=True."""
    with patch("yt_dlp.YoutubeDL") as mock_ydl_class:
        mock_ydl = MagicMock()
        mock_ydl_class.return_value.__enter__.return_value = mock_ydl
        mock_ydl.extract_info.return_value = {
            "upload_date": "20240215",
            "tags": ["podcast"],
            "subtitles": {
                "en": [{"ext": "vtt", "url": "https://yt.com/en.vtt"}],  # Only English manual
            },
            "automatic_captions": {
                "fa-orig": [{"ext": "vtt", "url": "https://yt.com/auto_fa_orig.vtt"}],
            },
        }

        mock_vtt_data = "WEBVTT\n\n00:00.000 --> 00:05.000\nمتن اتوماتیک".encode("utf-8")
        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_resp = MagicMock()
            mock_resp.read.return_value = mock_vtt_data
            mock_urlopen.return_value.__enter__.return_value = mock_resp

            # With auto CC enabled -> returns auto_cc
            res = extract_persian_vtt("test_fallback", include_auto_cc=True)
            vtt_text, url, info, p_type, origin_type = res
            assert vtt_text is not None
            assert p_type == "auto_cc"
            assert origin_type == "machine_transcribed"
            assert url == "https://yt.com/auto_fa_orig.vtt"

            # With auto CC disabled -> returns None for payload
            res_disabled = extract_persian_vtt("test_fallback", include_auto_cc=False)
            vtt_text_d, url_d, info_d, p_type_d, origin_d = res_disabled
            assert vtt_text_d is None
            assert p_type_d is None


def test_run_ingestion_end_to_end_mocked(tmp_path: Path):
    """Verify full ingestion workflow: manual CC, auto CC fallback, and cataloging in SQLite."""
    db_file = tmp_path / "mock_vault.db"

    mock_entries = [
        # Short teaser (< 600s) -> excluded by duration filter
        {"id": "short_001", "title": "Short Teaser", "duration": 59, "view_count": 5000},
        # Longform video with manual Persian CC
        {"id": "long_001", "title": "Episode 100 Manual", "duration": 3600, "view_count": 80000},
        # Longform video with auto Persian CC
        {"id": "long_002", "title": "Episode 101 Auto", "duration": 4200, "view_count": 60000},
        # Longform video with NO CC at all -> cataloged without payload
        {"id": "long_003", "title": "Episode 102 No CC", "duration": 5000, "view_count": 40000},
    ]

    def mock_extract_vtt(vid_id: str, include_auto_cc: bool = True):
        if vid_id == "long_001":
            return "WEBVTT\n\n00:01.000 --> 00:06.000\nمتن دستی اول", "https://yt.com/vtt_1", {}, "manual_cc", "human_transcribed"
        elif vid_id == "long_002":
            return "WEBVTT\n\n00:01.000 --> 00:06.000\nمتن اتوماتیک دوم", "https://yt.com/vtt_2", {}, "auto_cc", "machine_transcribed"
        return None, None, {}, None, None

    with patch("scripts.ingest_tabaghe16.extract_channel_videos", return_value=mock_entries):
        with patch("scripts.ingest_tabaghe16.extract_persian_vtt", side_effect=mock_extract_vtt):
            result = run_ingestion(
                db_path=str(db_file),
                min_duration=600,
                include_auto_cc=True,
                workers=2,
                dry_run=False,
                sync_gdrive=False,
            )

    assert result["total_eligible"] == 3  # All 3 longform videos
    assert result["newly_manual"] == 1
    assert result["newly_auto"] == 1
    assert result["no_cc"] == 1

    # Verify database contents
    with VaultManager(db_path=db_file) as vm:
        stats = vm.get_vault_stats()
        assert stats["total_sources"] == 1
        assert stats["total_items"] == 3  # All 3 longform videos cataloged!
        assert stats["total_payloads"] == 2  # 1 manual + 1 auto
        assert stats["payloads_by_origin"]["human_transcribed"] == 1
        assert stats["payloads_by_origin"]["machine_transcribed"] == 1
        assert stats["payloads_by_type"]["manual_cc"] == 1
        assert stats["payloads_by_type"]["auto_cc"] == 1

        # Check item without payload exists in items table
        no_cc_item = vm.get_item("yt:long_003")
        assert no_cc_item is not None
        assert vm.get_payloads_by_item("yt:long_003") == []
