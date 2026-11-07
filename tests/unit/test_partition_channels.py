"""Unit test suite for scripts/partition_channels.py.

Verifies catalog acquisition, schema validation, anomaly fixes, deduplication,
domain squad allocations, count fallback, output formats, CLI execution, and exit codes.
"""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import pytest

from scripts.partition_channels import (
    DOMAIN_SQUADS,
    format_partitions,
    get_shard_cli_args,
    load_catalog,
    main,
    partition_channels,
    save_partitions,
)

CATALOG_PATH = Path("data/persian_channels_catalog.json")


# ---------------------------------------------------------------------------
# 1. Catalog Acquisition and Schema Validation
# ---------------------------------------------------------------------------


def test_catalog_file_exists_and_is_valid() -> None:
    """Verify that canonical catalog file exists, is non-empty, and has expected top-level keys."""
    assert CATALOG_PATH.exists(), f"Catalog not found at {CATALOG_PATH}"
    assert CATALOG_PATH.stat().st_size > 0

    with open(CATALOG_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    assert isinstance(data, dict)
    assert "channels" in data
    assert isinstance(data["channels"], list)
    assert len(data["channels"]) == 25


def test_load_catalog_missing_file_raises(tmp_path: Path) -> None:
    """Missing catalog file raises FileNotFoundError."""
    missing = tmp_path / "missing.json"
    with pytest.raises(FileNotFoundError, match="not found"):
        load_catalog(missing)


def test_load_catalog_malformed_json_raises(tmp_path: Path) -> None:
    """Malformed JSON in catalog file raises ValueError."""
    bad_json = tmp_path / "bad.json"
    bad_json.write_text("{bad_json: missing_quotes", encoding="utf-8")
    with pytest.raises(ValueError, match="Malformed JSON"):
        load_catalog(bad_json)


def test_load_catalog_missing_channels_key_raises(tmp_path: Path) -> None:
    """Catalog missing 'channels' key raises ValueError."""
    bad_schema = tmp_path / "no_channels.json"
    bad_schema.write_text(json.dumps({"wrong_key": []}), encoding="utf-8")
    with pytest.raises(ValueError, match="missing 'channels' key"):
        load_catalog(bad_schema)


def test_load_catalog_channels_not_list_raises(tmp_path: Path) -> None:
    """Catalog where 'channels' is not a list raises ValueError."""
    bad_schema = tmp_path / "channels_not_list.json"
    bad_schema.write_text(json.dumps({"channels": "not-a-list"}), encoding="utf-8")
    with pytest.raises(ValueError, match="'channels' must be a list"):
        load_catalog(bad_schema)


def test_load_catalog_channel_item_not_dict_raises(tmp_path: Path) -> None:
    """Catalog where a channel entry is not a dictionary raises ValueError."""
    bad_schema = tmp_path / "channel_not_dict.json"
    bad_schema.write_text(json.dumps({"channels": ["not-a-dict"]}), encoding="utf-8")
    with pytest.raises(ValueError, match="is not a dictionary"):
        load_catalog(bad_schema)


# ---------------------------------------------------------------------------
# 2. Anomaly Corrections and Handle Normalization
# ---------------------------------------------------------------------------


def test_handle_anomaly_substitutions() -> None:
    """Verify Kouman_Official -> @Kouman and radiomarz -> @radio-marz substitutions."""
    channels = load_catalog(CATALOG_PATH, dedup=True, filter_empty=False)
    handles = {c["handle"] for c in channels}

    assert "@Kouman" in handles
    assert "@radio-marz" in handles
    assert "@Kouman_Official" not in handles
    assert "Kouman_Official" not in handles
    assert "@radiomarz" not in handles
    assert "radiomarz" not in handles

    # Check URL updates
    kouman_entry = next(c for c in channels if c["handle"] == "@Kouman")
    assert "https://www.youtube.com/@Kouman" in kouman_entry.get("channel_url", "")

    radiomarz_entry = next(c for c in channels if c["handle"] == "@radio-marz")
    assert "https://www.youtube.com/@radio-marz" in radiomarz_entry.get("channel_url", "")


def test_handle_leading_at_normalization() -> None:
    """All channels in the normalized catalog must have handles starting with @."""
    channels = load_catalog(CATALOG_PATH, dedup=True, filter_empty=False)
    for ch in channels:
        assert "handle" in ch
        assert ch["handle"].startswith("@"), f"Handle {ch['handle']} missing leading @"


# ---------------------------------------------------------------------------
# 3. Deduplication Logic (Fiction Podcast)
# ---------------------------------------------------------------------------


def test_fiction_podcast_deduplication() -> None:
    """Verify Fiction Podcast index 11 (seed) and 12 (search hit) deduplicate to canonical."""
    # Without deduplication: 25 channels, 2 Fiction Podcasts
    raw_channels = load_catalog(CATALOG_PATH, dedup=False, filter_empty=False)
    assert len(raw_channels) == 25
    fiction_raw = [c for c in raw_channels if c["handle"].lower() == "@fictionpodcast"]
    assert len(fiction_raw) == 2

    # With deduplication: 24 channels, exactly 1 canonical Fiction Podcast
    deduped_channels = load_catalog(CATALOG_PATH, dedup=True, filter_empty=False)
    assert len(deduped_channels) == 24
    fiction_deduped = [c for c in deduped_channels if c["handle"].lower() == "@fictionpodcast"]
    assert len(fiction_deduped) == 1

    canonical = fiction_deduped[0]
    assert canonical["handle"] == "@FictionPodcast"
    assert canonical["channel_id"] == "UCH_Bp7VV_o3Ur21vww3ktcQ"


# ---------------------------------------------------------------------------
# 4. Filter Empty Channels Behavior
# ---------------------------------------------------------------------------


def test_filter_empty_behavior() -> None:
    """Verify @navidkashani is retained by default and excluded when filter_empty=True."""
    # Retained by default: 24 channels total
    channels_default = load_catalog(CATALOG_PATH, dedup=True, filter_empty=False)
    assert len(channels_default) == 24
    navid_matches = [c for c in channels_default if c["handle"].lower() == "@navidkashani"]
    assert len(navid_matches) == 1

    # Excluded when filter_empty is True: 23 channels total
    channels_filtered = load_catalog(CATALOG_PATH, dedup=True, filter_empty=True)
    assert len(channels_filtered) == 23
    navid_filtered = [c for c in channels_filtered if c["handle"].lower() == "@navidkashani"]
    assert len(navid_filtered) == 0

    # Ensure other channels (including Kouman and Radio Marz) are NOT dropped
    handles_filtered = {c["handle"] for c in channels_filtered}
    assert "@Kouman" in handles_filtered
    assert "@radio-marz" in handles_filtered


# ---------------------------------------------------------------------------
# 5. Domain Squad Allocation (4 Shards)
# ---------------------------------------------------------------------------


def test_domain_squad_allocations_default() -> None:
    """Verify exact memberships for all 4 domain squads with default catalog."""
    channels = load_catalog(CATALOG_PATH, dedup=True, filter_empty=False)
    partitions = partition_channels(channels, shards_count=4, balance_mode="domain")

    assert partitions["total_channels"] == 24
    assert partitions["shards_count"] == 4
    assert partitions["balance_mode"] == "domain"

    shards = partitions["shards"]
    assert len(shards) == 4

    expected_squads = [
        {
            "shard_id": 1,
            "squad_name": "Podcasts & Thought",
            "channels": [
                "@Tabaghe16",
                "@ChannelBPodcast",
                "@BplusPodcast",
                "@FictionPodcast",
                "@WayneDyerFarsi",
                "@radio-marz",
            ],
        },
        {
            "shard_id": 2,
            "squad_name": "Vlogs & Entertainment",
            "channels": [
                "@MiaPlays",
                "@Kouman",
                "@The_Fazel",
                "@Bahare",
                "@IraniTanz",
                "@KhandeBazi",
            ],
        },
        {
            "shard_id": 3,
            "squad_name": "Narrative & Culture",
            "channels": [
                "@Dastan_land",
                "@ShahreFarangs",
                "@FilmNama",
                "@Filmbaz",
                "@Vistamediaa",
                "@BBCNewsPersian",
            ],
        },
        {
            "shard_id": 4,
            "squad_name": "Practical & Instructional",
            "channels": [
                "@ChefJavadJavadi",
                "@ECO_COOK",
                "@SkillVid",
                "@Amin.Farzad",
                "@MehranRowshan",
                "@navidkashani",
            ],
        },
    ]

    for shard, expected in zip(shards, expected_squads):
        assert shard["shard_id"] == expected["shard_id"]
        assert shard["squad_name"] == expected["squad_name"]
        assert shard["channels_count"] == 6
        handles = [ch["handle"] for ch in shard["channels"]]
        assert handles == expected["channels"]
        assert shard["cli_args"] == f"--shard-id {expected['shard_id']} --channels {','.join(expected['channels'])}"


def test_domain_squad_allocations_with_filter_empty() -> None:
    """Verify Squad 4 has 5 channels when filter_empty=True (Navid Kashani excluded)."""
    channels = load_catalog(CATALOG_PATH, dedup=True, filter_empty=True)
    partitions = partition_channels(channels, shards_count=4, balance_mode="domain")

    assert partitions["total_channels"] == 23
    shards = partitions["shards"]
    assert shards[0]["channels_count"] == 6
    assert shards[1]["channels_count"] == 6
    assert shards[2]["channels_count"] == 6
    assert shards[3]["channels_count"] == 5

    squad4_handles = [ch["handle"] for ch in shards[3]["channels"]]
    assert squad4_handles == [
        "@ChefJavadJavadi",
        "@ECO_COOK",
        "@SkillVid",
        "@Amin.Farzad",
        "@MehranRowshan",
    ]
    assert (
        shards[3]["cli_args"]
        == "--shard-id 4 --channels @ChefJavadJavadi,@ECO_COOK,@SkillVid,@Amin.Farzad,@MehranRowshan"
    )


# ---------------------------------------------------------------------------
# 6. Count Balance Mode and Fallbacks
# ---------------------------------------------------------------------------


def test_count_balance_mode_explicit() -> None:
    """Explicit balance_mode='count' partitions channels evenly."""
    channels = load_catalog(CATALOG_PATH, dedup=True, filter_empty=False)
    partitions = partition_channels(channels, shards_count=4, balance_mode="count")

    assert partitions["balance_mode"] == "count"
    assert partitions["shards_count"] == 4
    for shard in partitions["shards"]:
        assert shard["channels_count"] == 6


def test_count_balance_mode_arbitrary_shards() -> None:
    """Arbitrary shards_count partitions channels evenly."""
    channels = load_catalog(CATALOG_PATH, dedup=True, filter_empty=False)
    partitions = partition_channels(channels, shards_count=3, balance_mode="count")

    assert partitions["shards_count"] == 3
    for shard in partitions["shards"]:
        assert shard["channels_count"] == 8


def test_domain_fallback_to_count_when_shards_not_four() -> None:
    """Domain balance mode with shards_count != 4 falls back to count balancing."""
    channels = load_catalog(CATALOG_PATH, dedup=True, filter_empty=False)
    partitions = partition_channels(channels, shards_count=2, balance_mode="domain")

    assert partitions["shards_count"] == 2
    for shard in partitions["shards"]:
        assert shard["channels_count"] == 12


def test_invalid_parameters_raise_value_error() -> None:
    """Invalid shards_count or balance_mode raises ValueError."""
    channels = [{"handle": "@test"}]
    with pytest.raises(ValueError, match="shards_count must be at least 1"):
        partition_channels(channels, shards_count=0)

    with pytest.raises(ValueError, match="Invalid balance_mode"):
        partition_channels(channels, balance_mode="invalid_mode")


# ---------------------------------------------------------------------------
# 7. Shard CLI Arguments Generation
# ---------------------------------------------------------------------------


def test_get_shard_cli_args() -> None:
    """Verify CLI args string matches harvester format contract."""
    shard = {
        "shard_id": 1,
        "channels": [
            {"handle": "@Tabaghe16"},
            {"handle": "ChannelBPodcast"},  # missing @ normalized
            {"uploader_id": "@BplusPodcast"},
        ],
    }
    args = get_shard_cli_args(shard)
    assert args == "--shard-id 1 --channels @Tabaghe16,@ChannelBPodcast,@BplusPodcast"

    # Handles as strings
    shard_str = {
        "shard_id": 2,
        "channels": ["@MiaPlays", "Kouman"],
    }
    args_str = get_shard_cli_args(shard_str)
    assert args_str == "--shard-id 2 --channels @MiaPlays,@Kouman"


# ---------------------------------------------------------------------------
# 8. Output Formatting: JSON, CLI, Summary
# ---------------------------------------------------------------------------


def test_format_partitions_json() -> None:
    """Format type 'json' returns valid JSON string with expected keys."""
    channels = load_catalog(CATALOG_PATH, dedup=True, filter_empty=False)
    partitions = partition_channels(channels, shards_count=4, balance_mode="domain")
    output = format_partitions(partitions, format_type="json")

    data = json.loads(output)
    assert data["total_channels"] == 24
    assert len(data["shards"]) == 4


def test_format_partitions_cli() -> None:
    """Format type 'cli' returns 4 lines matching harvester contract."""
    channels = load_catalog(CATALOG_PATH, dedup=True, filter_empty=False)
    partitions = partition_channels(channels, shards_count=4, balance_mode="domain")
    output = format_partitions(partitions, format_type="cli")

    lines = output.strip().splitlines()
    assert len(lines) == 4
    assert lines[0].startswith("--shard-id 1 --channels @Tabaghe16")
    assert lines[1].startswith("--shard-id 2 --channels @MiaPlays")
    assert lines[2].startswith("--shard-id 3 --channels @Dastan_land")
    assert lines[3].startswith("--shard-id 4 --channels @ChefJavadJavadi")


def test_format_partitions_summary() -> None:
    """Format type 'summary' returns structured human-readable text."""
    channels = load_catalog(CATALOG_PATH, dedup=True, filter_empty=False)
    partitions = partition_channels(channels, shards_count=4, balance_mode="domain")
    summary = format_partitions(partitions, format_type="summary")

    assert "Persian Channels Partitioning Summary" in summary
    assert "Shard 1: Podcasts & Thought" in summary
    assert "Shard 2: Vlogs & Entertainment" in summary
    assert "Shard 3: Narrative & Culture" in summary
    assert "Shard 4: Practical & Instructional" in summary


def test_format_partitions_invalid_type_raises() -> None:
    """Unsupported format_type raises ValueError."""
    with pytest.raises(ValueError, match="Unsupported format_type"):
        format_partitions({}, format_type="xml")


# ---------------------------------------------------------------------------
# 9. Save Partitions to Disk
# ---------------------------------------------------------------------------


def test_save_partitions_to_disk(tmp_path: Path) -> None:
    """save_partitions creates parent dirs and writes valid JSON file."""
    channels = load_catalog(CATALOG_PATH, dedup=True, filter_empty=False)
    partitions = partition_channels(channels, shards_count=4, balance_mode="domain")

    out_file = tmp_path / "deep" / "nested" / "partitions.json"
    result_path = save_partitions(partitions, out_file)

    assert result_path.exists()
    with open(result_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert data["total_channels"] == 24
    assert len(data["shards"]) == 4


# ---------------------------------------------------------------------------
# 10. CLI Execution and Exit Codes (0, 1, 2, 3)
# ---------------------------------------------------------------------------


def test_cli_success_exit_code_0() -> None:
    """CLI exits with code 0 on successful run."""
    code = main(["--format", "cli"])
    assert code == 0


def test_cli_catalog_not_found_exit_code_1() -> None:
    """CLI exits with code 1 when catalog file does not exist."""
    code = main(["--catalog", "nonexistent_file_abc123.json"])
    assert code == 1


def test_cli_malformed_catalog_exit_code_2(tmp_path: Path) -> None:
    """CLI exits with code 2 when catalog is malformed JSON or has invalid schema."""
    bad_json = tmp_path / "bad.json"
    bad_json.write_text("{not valid json", encoding="utf-8")
    code = main(["--catalog", str(bad_json)])
    assert code == 2

    bad_schema = tmp_path / "no_channels.json"
    bad_schema.write_text("{}", encoding="utf-8")
    code_schema = main(["--catalog", str(bad_schema)])
    assert code_schema == 2


def test_cli_invalid_parameters_exit_code_3() -> None:
    """CLI exits with code 3 on invalid parameters."""
    # Shards < 1
    assert main(["--shards", "0"]) == 3
    assert main(["--shards", "-5"]) == 3

    # Invalid choice for format
    assert main(["--format", "unsupported_fmt"]) == 3

    # Invalid choice for balance-mode
    assert main(["--balance-mode", "unsupported_mode"]) == 3


def test_cli_subprocess_invocation() -> None:
    """Subprocess CLI invocations execute correctly and respect exit codes."""
    # Success invocation
    proc = subprocess.run(
        [sys.executable, "scripts/partition_channels.py", "--format", "cli"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0
    assert "--shard-id 1" in proc.stdout

    # Code 1: Missing catalog
    proc_1 = subprocess.run(
        [sys.executable, "scripts/partition_channels.py", "--catalog", "missing.json"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc_1.returncode == 1

    # Code 3: Invalid shards
    proc_3 = subprocess.run(
        [sys.executable, "scripts/partition_channels.py", "--shards", "0"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc_3.returncode == 3


def test_cli_shards_and_balance_mode_matrix() -> None:
    """Verify --shards (1, 2, 8) with both 'count' and 'domain' balance modes."""
    for shards in [1, 2, 8]:
        for mode in ["count", "domain"]:
            proc = subprocess.run(
                [
                    sys.executable,
                    "scripts/partition_channels.py",
                    "--shards",
                    str(shards),
                    "--balance-mode",
                    mode,
                    "--format",
                    "json",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            assert proc.returncode == 0
            data = json.loads(proc.stdout)
            assert data["shards_count"] == shards
            assert len(data["shards"]) == shards
            expected_per_shard = 24 // shards
            for s in data["shards"]:
                assert s["channels_count"] == expected_per_shard


def test_cli_no_dedup_flag() -> None:
    """Verify --no-dedup retains duplicates resulting in 25 channels."""
    proc = subprocess.run(
        [sys.executable, "scripts/partition_channels.py", "--no-dedup", "--format", "json"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0
    data = json.loads(proc.stdout)
    assert data["total_channels"] == 25
    fiction_entries = [c for s in data["shards"] for c in s["channels"] if "fiction" in c["handle"].lower()]
    assert len(fiction_entries) == 2


def test_cli_filter_empty_flag() -> None:
    """Verify --filter-empty drops 0-video channels resulting in 23 channels."""
    proc = subprocess.run(
        [sys.executable, "scripts/partition_channels.py", "--filter-empty", "--format", "json"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0
    data = json.loads(proc.stdout)
    assert data["total_channels"] == 23
    assert not any("navidkashani" in c["handle"].lower() for s in data["shards"] for c in s["channels"])


def test_cli_output_nested_nonexistent_dir(tmp_path: Path) -> None:
    """Verify --output creates parent dirs and writes valid partitions file."""
    nested_out = tmp_path / "deep" / "nested" / "dir" / "shards.json"
    proc = subprocess.run(
        [sys.executable, "scripts/partition_channels.py", "--output", str(nested_out)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0
    assert nested_out.is_file()
    with open(nested_out, "r", encoding="utf-8") as f:
        disk_data = json.load(f)
    assert disk_data["total_channels"] == 24
    assert len(disk_data["shards"]) == 4


def test_cli_stress_1000_channels(tmp_path: Path) -> None:
    """Stress test with 1,000 synthetic channels across 16 shards."""
    large_catalog = tmp_path / "large_catalog.json"
    synth_channels = [
        {"handle": f"@persian_synth_{i}", "channel_title": f"Synth {i}", "channel_id": f"UC_SYNTH_{i}"}
        for i in range(1000)
    ]
    large_catalog.write_text(json.dumps({"channels": synth_channels}), encoding="utf-8")

    proc = subprocess.run(
        [
            sys.executable,
            "scripts/partition_channels.py",
            "--catalog",
            str(large_catalog),
            "--shards",
            "16",
            "--balance-mode",
            "count",
            "--format",
            "json",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0
    data = json.loads(proc.stdout)
    assert data["total_channels"] == 1000
    assert len(data["shards"]) == 16

