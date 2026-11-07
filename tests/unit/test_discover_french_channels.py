"""Unit tests for the French YouTube channel discovery and subtitle auditing engine."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import pytest

from scripts.discover_french_channels import (
    ALLOWED_COUNTRY_CODES,
    SEED_CHANNELS,
    audit_single_channel,
    calculate_channel_soft_score,
    discover_french_channels,
    inspect_video_subtitles,
)


def test_allowed_country_codes_boundaries():
    """Verify strictly allowed country codes: FR, CA, BE, CH (Africa and Maghreb excluded)."""
    assert ALLOWED_COUNTRY_CODES == {"FR", "CA", "BE", "CH"}
    for seed in SEED_CHANNELS:
        assert seed["country_code"] in ALLOWED_COUNTRY_CODES
        assert seed["region"] in {"Metropolitan France", "Quebec", "Wallonia", "Brussels", "Romandy"}


def test_inspect_video_subtitles_detection():
    """Verify subtitle inspection handles manual CC, auto CC, and non-French tracks."""
    # Video 1: Manual French (France)
    v1 = {
        "subtitles": {"fr-FR": [{"ext": "vtt"}]},
        "automatic_captions": {"fr-orig": [{"ext": "vtt"}]},
    }
    has_man, has_auto, man_langs, auto_langs = inspect_video_subtitles(v1)
    assert has_man is True
    assert has_auto is True
    assert "fr-FR" in man_langs
    assert "fr-orig" in auto_langs

    # Video 2: Manual French (Canada)
    v2 = {
        "subtitles": {"fr-CA": [{"ext": "vtt"}]},
        "automatic_captions": {},
    }
    has_man, has_auto, man_langs, auto_langs = inspect_video_subtitles(v2)
    assert has_man is True
    assert has_auto is False
    assert "fr-CA" in man_langs

    # Video 3: Only Auto CC
    v3 = {
        "subtitles": {},
        "automatic_captions": {"fr": [{"ext": "vtt"}]},
    }
    has_man, has_auto, man_langs, auto_langs = inspect_video_subtitles(v3)
    assert has_man is False
    assert has_auto is True

    # Video 4: Non-French tracks
    v4 = {
        "subtitles": {"en": [{"ext": "vtt"}], "es": [{"ext": "vtt"}]},
        "automatic_captions": {"en-orig": [{"ext": "vtt"}]},
    }
    has_man, has_auto, man_langs, auto_langs = inspect_video_subtitles(v4)
    assert has_man is False
    assert has_auto is False
    assert len(man_langs) == 0

    # Video 5: Defensive empty
    has_man, has_auto, _, _ = inspect_video_subtitles({})
    assert has_man is False
    assert has_auto is False


def test_calculate_channel_soft_score():
    """Verify soft score ranks manual CC higher for comparable view counts."""
    score_manual = calculate_channel_soft_score(view_count=50000, manual_cc_pct=1.0, video_count=5)
    score_auto = calculate_channel_soft_score(view_count=50000, manual_cc_pct=0.0, video_count=5)

    assert score_manual > score_auto
    # Difference should reflect the 2.5 multiplier for manual CC
    assert pytest.approx(score_manual - score_auto, rel=1e-2) == 2.5

    # High view counts should still give reasonable positive values
    score_huge = calculate_channel_soft_score(view_count=10_000_000, manual_cc_pct=0.5, video_count=5)
    assert score_huge > score_manual


def test_disallowed_country_rejected():
    """Verify channels from non-allowed countries are rejected during audit."""
    disallowed_spec = {
        "handle": "@AfricanChannel",
        "country_code": "SN",
        "region": "Senegal",
    }
    result = audit_single_channel(disallowed_spec)
    assert result is None


def test_discover_french_channels_mock(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Verify catalog structure and metadata generation with mock auditing."""
    mock_channel_record = {
        "channel_title": "Mock French Channel",
        "channel_url": "https://www.youtube.com/@MockFrench",
        "channel_id": "UC_mock123",
        "uploader_id": "@MockFrench",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "conversational",
        "interaction_type": "dialogue",
        "domain": "interview_podcast",
        "audited_videos_count": 4,
        "manual_fr_cc_count": 2,
        "manual_fr_cc_pct": 50.0,
        "auto_fr_cc_count": 4,
        "any_manual_cc": True,
        "status": "audited",
        "soft_score": 7.25,
        "videos": [
            {
                "video_id": "v1",
                "video_url": "https://www.youtube.com/watch?v=v1",
                "title": "Mock Video 1",
                "duration_seconds": 900,
                "view_count": 20000,
                "has_manual_fr_cc": True,
                "has_auto_fr_cc": True,
                "all_manual_sub_langs": ["fr"],
            }
        ],
    }

    def mock_audit(channel_spec: Dict[str, Any], **kwargs: Any) -> Dict[str, Any]:
        rec = dict(mock_channel_record)
        rec["uploader_id"] = channel_spec["handle"]
        rec["country_code"] = channel_spec["country_code"]
        return rec

    monkeypatch.setattr("scripts.discover_french_channels.audit_single_channel", mock_audit)

    catalog_path = tmp_path / "test_french_catalog.json"
    seeds = [
        {"handle": "@MockFR", "country_code": "FR", "region": "Metropolitan France"},
        {"handle": "@MockCA", "country_code": "CA", "region": "Quebec"},
    ]

    catalog = discover_french_channels(
        seed_channels=seeds,
        output_path=catalog_path,
        workers=1,
    )

    assert catalog["language_iso1"] == "fr"
    assert catalog["total_channels_audited"] == 2
    assert catalog["channels_with_any_manual_fr_cc"] == 2
    assert catalog["total_videos_audited"] == 8
    assert catalog["total_manual_fr_cc_videos"] == 4

    assert catalog_path.is_file()
    with open(catalog_path, "r", encoding="utf-8") as f:
        loaded = json.load(f)
    assert len(loaded["channels"]) == 2
    assert loaded["channels"][0]["country_code"] in {"FR", "CA"}
