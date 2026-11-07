"""Unit tests for the French channel partitioner."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pytest

from scripts.partition_french_channels import (
    FRENCH_DOMAIN_SQUADS,
    export_shards,
    format_summary_table,
    get_shard_cli_command,
    load_french_catalog,
    partition_by_shards,
    partition_by_squads,
)


@pytest.fixture
def sample_catalog_file(tmp_path: Path) -> Path:
    """Create a temporary French catalog JSON file with representative channels."""
    catalog_data = {
        "generated_at_utc": "2026-11-07T12:00:00+00:00",
        "language_iso1": "fr",
        "allowed_country_codes": ["BE", "CA", "CH", "FR"],
        "total_channels_audited": 6,
        "channels": [
            {
                "channel_title": "LEGEND",
                "uploader_id": "@LegendOfficiel",  # Should trigger substitution
                "country_code": "FR",
                "region": "Metropolitan France",
                "register": "conversational",
                "interaction_type": "dialogue",
                "domain": "interview_talkshow",
                "audited_videos_count": 4,
                "manual_fr_cc_count": 1,
                "auto_fr_cc_count": 4,
                "soft_score": 7.5,
                "videos": [],
            },
            {
                "channel_title": "Loris Giuliano",
                "uploader_id": "@LorisGiuliano",
                "country_code": "FR",
                "region": "Metropolitan France",
                "register": "colloquial",
                "interaction_type": "street_interview",
                "domain": "street_interview",
                "audited_videos_count": 4,
                "manual_fr_cc_count": 0,
                "auto_fr_cc_count": 4,
                "soft_score": 6.8,
                "videos": [],
            },
            {
                "channel_title": "Sous Écoute",
                "uploader_id": "@SousEcoute",
                "country_code": "CA",
                "region": "Quebec",
                "register": "colloquial",
                "interaction_type": "dialogue",
                "domain": "comedy_podcast",
                "audited_videos_count": 4,
                "manual_fr_cc_count": 0,
                "auto_fr_cc_count": 4,
                "soft_score": 6.0,
                "videos": [],
            },
            {
                "channel_title": "Radio-Canada Info",
                "uploader_id": "@RadioCanadaInfo",
                "country_code": "CA",
                "region": "Quebec",
                "register": "broadcast",
                "interaction_type": "monologue",
                "domain": "public_journalism",
                "audited_videos_count": 4,
                "manual_fr_cc_count": 0,
                "auto_fr_cc_count": 4,
                "soft_score": 5.5,
                "videos": [],
            },
            {
                "channel_title": "MATH",
                "uploader_id": "@Mathsefaitdesfilms",
                "country_code": "BE",
                "region": "Wallonia",
                "register": "conversational",
                "interaction_type": "monologue",
                "domain": "cinema_critique_humor",
                "audited_videos_count": 4,
                "manual_fr_cc_count": 0,
                "auto_fr_cc_count": 4,
                "soft_score": 6.5,
                "videos": [],
            },
            {
                "channel_title": "LE GRAND JD",
                "uploader_id": "@LeGrandJD",
                "country_code": "CH",
                "region": "Romandy",
                "register": "conversational",
                "interaction_type": "monologue",
                "domain": "exploration_adventure",
                "audited_videos_count": 4,
                "manual_fr_cc_count": 0,
                "auto_fr_cc_count": 4,
                "soft_score": 7.1,
                "videos": [],
            },
            {
                # Channel from unauthorized region (must be skipped)
                "channel_title": "African Media",
                "uploader_id": "@AfricanMedia",
                "country_code": "SN",
                "region": "Senegal",
                "audited_videos_count": 2,
                "soft_score": 4.0,
                "videos": [],
            },
        ],
    }
    p = tmp_path / "test_french_catalog.json"
    with open(p, "w", encoding="utf-8") as f:
        json.dump(catalog_data, f, indent=2)
    return p


def test_load_french_catalog_validation(sample_catalog_file: Path):
    """Verify loading correctly normalizes handles, applies substitutions, and filters disallowed regions."""
    channels = load_french_catalog(sample_catalog_file)
    assert len(channels) == 6  # 7 minus 1 disallowed (SN)

    handles = {c["uploader_id"] for c in channels}
    assert "@Legendmedia" in handles  # @LegendOfficiel substituted
    assert "@LegendOfficiel" not in handles
    assert "@AfricanMedia" not in handles

    for ch in channels:
        assert ch["country_code"] in {"FR", "CA", "BE", "CH"}


def test_partition_by_squads_routing(sample_catalog_file: Path):
    """Verify channels route to appropriate regional and domain squads."""
    channels = load_french_catalog(sample_catalog_file)
    shards = partition_by_squads(channels)

    assert len(shards) == 8
    # Shard 1: FR Interviews -> @Legendmedia
    assert any(c["uploader_id"] == "@Legendmedia" for c in shards[0]["channels"])
    # Shard 2: FR Banter/Street -> @LorisGiuliano
    assert any(c["uploader_id"] == "@LorisGiuliano" for c in shards[1]["channels"])
    # Shard 5: CA Podcasts -> @SousEcoute
    assert any(c["uploader_id"] == "@SousEcoute" for c in shards[4]["channels"])
    # Shard 6: CA Media -> @RadioCanadaInfo
    assert any(c["uploader_id"] == "@RadioCanadaInfo" for c in shards[5]["channels"])
    # Shard 7: BE -> @Mathsefaitdesfilms
    assert any(c["uploader_id"] == "@Mathsefaitdesfilms" for c in shards[6]["channels"])
    # Shard 8: CH -> @LeGrandJD
    assert any(c["uploader_id"] == "@LeGrandJD" for c in shards[7]["channels"])

    # Total channel count across shards equals total loaded channels
    total_assigned = sum(s["channel_count"] for s in shards)
    assert total_assigned == 6


def test_partition_by_shards_balanced(sample_catalog_file: Path):
    """Verify balanced round-robin sharding distributes channels evenly by soft score."""
    channels = load_french_catalog(sample_catalog_file)
    shards = partition_by_shards(channels, num_shards=3)

    assert len(shards) == 3
    for s in shards:
        assert s["channel_count"] == 2
        assert s["total_videos"] == 8

    # Highest score channel (@Legendmedia with 7.5) should be in Shard 1
    assert shards[0]["channels"][0]["uploader_id"] == "@Legendmedia"


def test_export_shards_and_cli_command(sample_catalog_file: Path, tmp_path: Path):
    """Verify export to disk and CLI command formatting."""
    channels = load_french_catalog(sample_catalog_file)
    shards = partition_by_squads(channels)

    out_dir = tmp_path / "shards"
    exported = export_shards(shards, output_dir=out_dir)

    assert len(exported) == 8
    shard_1_path = out_dir / "shard_1.json"
    assert shard_1_path.is_file()

    with open(shard_1_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert data["shard_id"] == 1
    assert data["language_iso1"] == "fr"

    cmd = get_shard_cli_command(1, shard_1_path, lang="fr")
    assert "--shard-id 1" in cmd
    assert "--lang fr" in cmd
    assert "shard_1.json" in cmd


def test_format_summary_table(sample_catalog_file: Path):
    """Verify formatting summary table renders header and summary rows."""
    channels = load_french_catalog(sample_catalog_file)
    shards = partition_by_squads(channels)
    table = format_summary_table(shards)

    assert "Shard ID" in table
    assert "Squad Name" in table
    assert "FR - Interviews & Deep Dialogue" in table
    assert "TOTAL" in table
