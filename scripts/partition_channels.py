#!/usr/bin/env python3
"""Channel Partitioner Engine and CLI for Scaled Multi-Agent Persian Speech Harvesting.

Partitions audited Persian YouTube channels into balanced worker shards with deduplication,
handle anomaly correction, 0-video channel handling, and multiple export formats (JSON, CLI, summary).
"""

from __future__ import annotations

import argparse
import copy
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any


# Canonical domain squads specification
DOMAIN_SQUADS = [
    {
        "shard_id": 1,
        "squad_name": "Podcasts & Thought",
        "expected_handles": [
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
        "expected_handles": [
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
        "expected_handles": [
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
        "expected_handles": [
            "@ChefJavadJavadi",
            "@ECO_COOK",
            "@SkillVid",
            "@Amin.Farzad",
            "@MehranRowshan",
            "@navidkashani",
        ],
    },
]

# Anomaly mappings for handles and channel IDs
HANDLE_SUBSTITUTIONS = {
    "kouman_official": "@Kouman",
    "@kouman_official": "@Kouman",
    "radiomarz": "@radio-marz",
    "@radiomarz": "@radio-marz",
}

URL_SUBSTITUTIONS = {
    "kouman_official": "https://www.youtube.com/@Kouman",
    "@kouman_official": "https://www.youtube.com/@Kouman",
    "radiomarz": "https://www.youtube.com/@radio-marz",
    "@radiomarz": "https://www.youtube.com/@radio-marz",
}

FICTION_PODCAST_CANONICAL_ID = "UCH_Bp7VV_o3Ur21vww3ktcQ"
FICTION_PODCAST_HANDLE = "@FictionPodcast"


class CLIParamError(Exception):
    """Raised when command-line parameters are invalid."""

    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class PartitionerArgumentParser(argparse.ArgumentParser):
    """Custom argument parser that converts validation errors into exit code 3."""

    def exit(self, status: int = 0, message: str | None = None) -> None:
        if message:
            self._print_message(message, sys.stderr)
        code = 3 if status != 0 else 0
        raise CLIParamError(code, message or "")

    def error(self, message: str) -> None:
        raise CLIParamError(3, message)


def normalize_channel(raw_ch: dict[str, Any]) -> dict[str, Any]:
    """Normalize channel fields, apply handle substitutions, and ensure leading @."""
    ch = copy.deepcopy(raw_ch)

    handle = (
        ch.get("handle")
        or ch.get("uploader_id")
        or ch.get("channel_id")
        or ""
    ).strip()

    # Apply known anomaly substitutions (case-insensitive)
    handle_lower = handle.lower()
    if handle_lower in HANDLE_SUBSTITUTIONS:
        handle = HANDLE_SUBSTITUTIONS[handle_lower]
        ch["channel_url"] = URL_SUBSTITUTIONS.get(handle_lower, ch.get("channel_url", ""))
    elif handle and not handle.startswith("@"):
        handle = f"@{handle}"

    ch["handle"] = handle
    if "uploader_id" in ch:
        ch["uploader_id"] = handle

    return ch


def is_empty_channel(ch: dict[str, Any]) -> bool:
    """Detect if channel has 0 public videos."""
    handle = (ch.get("handle") or ch.get("uploader_id") or "").strip().lower()
    channel_id = (ch.get("channel_id") or "").strip().lower()

    if handle in ("@navidkashani", "navidkashani") or channel_id == "navidkashani":
        return True

    if ch.get("is_empty") is True or ch.get("has_public_videos") is False:
        return True

    if "public_videos_count" in ch and ch["public_videos_count"] == 0:
        return True

    # Exclude canonical seed channels that lacked sample video arrays in seed catalog
    if handle in ("@kouman", "@radio-marz", "kouman_official", "@kouman_official", "radiomarz", "@radiomarz"):
        return False

    if "audited_videos_count" in ch and ch["audited_videos_count"] == 0:
        return True

    if "videos" in ch and isinstance(ch["videos"], list) and len(ch["videos"]) == 0:
        return True

    return False


def load_catalog(
    catalog_path: str | Path = "data/persian_channels_catalog.json",
    dedup: bool = True,
    filter_empty: bool = False,
) -> list[dict[str, Any]]:
    """Load Persian channels catalog with validation, anomaly fixing, deduplication, and filtering.

    Args:
        catalog_path: File path to channel catalog JSON.
        dedup: If True, deduplicate duplicate channel entries (e.g., Fiction Podcast).
        filter_empty: If True, filter out channels with 0 public videos (e.g., @navidkashani).

    Returns:
        List of normalized channel dictionaries.

    Raises:
        FileNotFoundError: If catalog file does not exist.
        ValueError: If JSON is malformed or schema is invalid.
    """
    path = Path(catalog_path)
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"Catalog file not found: {catalog_path}")

    try:
        with open(path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Malformed JSON in catalog file: {exc}") from exc

    if not isinstance(raw_data, dict) or "channels" not in raw_data:
        raise ValueError("Invalid catalog schema: missing 'channels' key in catalog JSON")

    channels_list = raw_data.get("channels")
    if not isinstance(channels_list, list):
        raise ValueError("Invalid catalog schema: 'channels' must be a list")

    normalized_channels: list[dict[str, Any]] = []
    seen_handles: set[str] = set()
    fiction_canonical: dict[str, Any] | None = None

    for idx, item in enumerate(channels_list):
        if not isinstance(item, dict):
            raise ValueError(f"Invalid catalog schema: channel at index {idx} is not a dictionary")

        ch = normalize_channel(item)
        handle = ch["handle"]
        handle_lower = handle.lower()
        channel_id = ch.get("channel_id", "")

        is_fiction = (
            handle_lower == FICTION_PODCAST_HANDLE.lower()
            or channel_id in ("FictionPodcast", FICTION_PODCAST_CANONICAL_ID)
        )

        if dedup and is_fiction:
            if fiction_canonical is None:
                fiction_canonical = ch
                fiction_canonical["handle"] = FICTION_PODCAST_HANDLE
                fiction_canonical["channel_id"] = FICTION_PODCAST_CANONICAL_ID
                fiction_canonical["uploader_id"] = FICTION_PODCAST_HANDLE
                normalized_channels.append(fiction_canonical)
                seen_handles.add(handle_lower)
            else:
                # Merge metadata into canonical entry
                if channel_id == FICTION_PODCAST_CANONICAL_ID:
                    fiction_canonical["channel_id"] = FICTION_PODCAST_CANONICAL_ID
                    if "channel_title" in ch and "پادکست" in ch["channel_title"]:
                        fiction_canonical["channel_title"] = ch["channel_title"]
            continue

        if dedup:
            if handle_lower in seen_handles:
                continue
            seen_handles.add(handle_lower)

        normalized_channels.append(ch)

    if filter_empty:
        normalized_channels = [ch for ch in normalized_channels if not is_empty_channel(ch)]

    return normalized_channels


def get_shard_cli_args(shard: dict[str, Any]) -> str:
    """Format CLI arguments string for a shard harvester."""
    shard_id = shard.get("shard_id", 1)
    channels = shard.get("channels", [])
    handles: list[str] = []

    for ch in channels:
        if isinstance(ch, dict):
            h = ch.get("handle") or ch.get("uploader_id") or ch.get("channel_id") or ""
        elif isinstance(ch, str):
            h = ch
        else:
            h = str(ch)

        h = h.strip()
        if h and not h.startswith("@"):
            h = f"@{h}"
        if h:
            handles.append(h)

    return f"--shard-id {shard_id} --channels {','.join(handles)}"


def _partition_domain(channels: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Partition channels into the 4 predefined domain squads."""
    handle_to_squad: dict[str, int] = {}
    for squad_idx, squad in enumerate(DOMAIN_SQUADS):
        for h in squad["expected_handles"]:
            handle_to_squad[h.lower()] = squad_idx

    # Also map aliases
    handle_to_squad["@kouman_official"] = 1
    handle_to_squad["kouman_official"] = 1
    handle_to_squad["@radiomarz"] = 0
    handle_to_squad["radiomarz"] = 0

    squad_channels: list[list[dict[str, Any]]] = [[] for _ in range(4)]
    unassigned: list[dict[str, Any]] = []

    for ch in channels:
        handle = (ch.get("handle") or ch.get("uploader_id") or "").lower()
        if handle in handle_to_squad:
            squad_idx = handle_to_squad[handle]
            squad_channels[squad_idx].append(ch)
        else:
            unassigned.append(ch)

    # Sort each squad's channels by predefined order
    for squad_idx, squad in enumerate(DOMAIN_SQUADS):
        expected = [h.lower() for h in squad["expected_handles"]]

        def sort_key(item: dict[str, Any]) -> int:
            h = (item.get("handle") or item.get("uploader_id") or "").lower()
            try:
                return expected.index(h)
            except ValueError:
                return 999

        squad_channels[squad_idx].sort(key=sort_key)

    # Allocate any unassigned channels to the squad with fewest channels
    for ch in unassigned:
        min_idx = min(range(4), key=lambda i: len(squad_channels[i]))
        squad_channels[min_idx].append(ch)

    shards: list[dict[str, Any]] = []
    for squad_idx, squad in enumerate(DOMAIN_SQUADS):
        shard_dict: dict[str, Any] = {
            "shard_id": squad["shard_id"],
            "squad_name": squad["squad_name"],
            "channels_count": len(squad_channels[squad_idx]),
            "channels": squad_channels[squad_idx],
        }
        shard_dict["cli_args"] = get_shard_cli_args(shard_dict)
        shards.append(shard_dict)

    return shards


def _partition_count(channels: list[dict[str, Any]], shards_count: int) -> list[dict[str, Any]]:
    """Partition channels evenly across shards_count shards using round-robin distribution."""
    shard_channels: list[list[dict[str, Any]]] = [[] for _ in range(shards_count)]

    for idx, ch in enumerate(channels):
        shard_channels[idx % shards_count].append(ch)

    shards: list[dict[str, Any]] = []
    for i in range(shards_count):
        shard_dict: dict[str, Any] = {
            "shard_id": i + 1,
            "squad_name": f"Shard {i + 1}",
            "channels_count": len(shard_channels[i]),
            "channels": shard_channels[i],
        }
        shard_dict["cli_args"] = get_shard_cli_args(shard_dict)
        shards.append(shard_dict)

    return shards


def partition_channels(
    channels: list[dict[str, Any]],
    shards_count: int = 4,
    balance_mode: str = "domain",
) -> dict[str, Any]:
    """Partition a list of channels into shards.

    Args:
        channels: List of channel dictionaries.
        shards_count: Target number of shards (default: 4).
        balance_mode: Partition mode, either 'domain' or 'count'.

    Returns:
        Dictionary containing metadata and the partitioned shards list.

    Raises:
        ValueError: If shards_count < 1 or balance_mode is invalid.
    """
    if shards_count < 1:
        raise ValueError(f"shards_count must be at least 1, got {shards_count}")

    if balance_mode not in ("domain", "count"):
        raise ValueError(f"Invalid balance_mode '{balance_mode}', must be 'domain' or 'count'")

    if balance_mode == "domain" and shards_count == 4:
        shards = _partition_domain(channels)
    else:
        # Fallback to count mode if shards_count != 4 or explicitly requested
        shards = _partition_count(channels, shards_count)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_channels": len(channels),
        "shards_count": len(shards),
        "balance_mode": balance_mode,
        "shards": shards,
    }


def format_partitions(partitions_data: dict[str, Any], format_type: str = "json") -> str:
    """Format partitioned shards data as JSON, CLI arguments, or summary text.

    Args:
        partitions_data: Dictionary produced by partition_channels.
        format_type: Output format: 'json', 'cli', or 'summary'.

    Returns:
        Formatted string output.

    Raises:
        ValueError: If format_type is unsupported.
    """
    if format_type == "json":
        return json.dumps(partitions_data, indent=2, ensure_ascii=False)

    if format_type == "cli":
        lines = [s["cli_args"] for s in partitions_data.get("shards", [])]
        return "\n".join(lines)

    if format_type == "summary":
        lines = [
            "============================================================",
            "Persian Channels Partitioning Summary",
            f"Total Channels: {partitions_data.get('total_channels', 0)}",
            f"Shards Count:   {partitions_data.get('shards_count', 0)}",
            f"Balance Mode:   {partitions_data.get('balance_mode', 'unknown')}",
            "============================================================",
        ]
        for shard in partitions_data.get("shards", []):
            s_id = shard.get("shard_id")
            s_name = shard.get("squad_name")
            s_count = shard.get("channels_count", len(shard.get("channels", [])))
            lines.append(f"Shard {s_id}: {s_name} ({s_count} channels)")
            lines.append(f"  CLI: {shard.get('cli_args')}")
            lines.append("  Channels:")
            for ch in shard.get("channels", []):
                title = ch.get("channel_title") or ch.get("title") or "Unknown"
                handle = ch.get("handle") or ch.get("uploader_id") or "Unknown"
                lines.append(f"    - {handle:20s} | {title}")
            lines.append("------------------------------------------------------------")
        return "\n".join(lines)

    raise ValueError(f"Unsupported format_type '{format_type}', must be 'json', 'cli', or 'summary'")


def save_partitions(partitions_data: dict[str, Any], output_path: str | Path) -> Path:
    """Save partitioned shards data to a JSON file on disk.

    Args:
        partitions_data: Partitions dictionary.
        output_path: File path to write JSON.

    Returns:
        Path object of saved file.
    """
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(partitions_data, f, indent=2, ensure_ascii=False)
    return out


def build_parser() -> PartitionerArgumentParser:
    """Construct CLI argument parser."""
    parser = PartitionerArgumentParser(
        description="Partition Persian YouTube channels catalog into balanced worker shards.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--catalog",
        default="data/persian_channels_catalog.json",
        help="Path to Persian channels catalog JSON.",
    )
    parser.add_argument(
        "--shards",
        type=int,
        default=4,
        help="Number of worker shards to generate.",
    )
    parser.add_argument(
        "--format",
        choices=["json", "cli", "summary"],
        default="json",
        help="Output format (json, cli, summary).",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional file path to save partitions (e.g. data/staging/partitions.json).",
    )
    parser.add_argument(
        "--dedup",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Deduplicate channels by handle and channel ID.",
    )
    parser.add_argument(
        "--filter-empty",
        action="store_true",
        default=False,
        help="Filter out channels with 0 public videos (e.g. @navidkashani).",
    )
    parser.add_argument(
        "--balance-mode",
        choices=["domain", "count"],
        default="domain",
        help="Partitioning strategy: domain squads or equal count.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for channel partitioner.

    Exit codes:
        0: Success
        1: Catalog file not found
        2: Malformed JSON or invalid schema
        3: Invalid CLI parameters (e.g. shards < 1)
    """
    parser = build_parser()

    try:
        args = parser.parse_args(argv)
        if args.shards < 1:
            raise CLIParamError(3, f"--shards must be at least 1, got {args.shards}")
    except CLIParamError as exc:
        sys.stderr.write(f"Argument Error: {exc.message}\n")
        return exc.code

    try:
        channels = load_catalog(
            catalog_path=args.catalog,
            dedup=args.dedup,
            filter_empty=args.filter_empty,
        )
    except FileNotFoundError as exc:
        sys.stderr.write(f"Catalog Not Found: {exc}\n")
        return 1
    except ValueError as exc:
        sys.stderr.write(f"Catalog Error: {exc}\n")
        return 2

    try:
        partitions_data = partition_channels(
            channels=channels,
            shards_count=args.shards,
            balance_mode=args.balance_mode,
        )
    except ValueError as exc:
        sys.stderr.write(f"Partition Error: {exc}\n")
        return 3

    if args.output:
        try:
            save_partitions(partitions_data, args.output)
        except OSError as exc:
            sys.stderr.write(f"Save Error: {exc}\n")
            return 2

    try:
        formatted = format_partitions(partitions_data, format_type=args.format)
        print(formatted)
    except ValueError as exc:
        sys.stderr.write(f"Format Error: {exc}\n")
        return 3

    return 0


if __name__ == "__main__":
    sys.exit(main())
