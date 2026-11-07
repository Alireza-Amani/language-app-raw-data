#!/usr/bin/env python3
"""French YouTube Channel Partitioner for Scaled Francophone Harvesting.

Partitions audited Francophone YouTube channels into balanced worker shards
across France (FR), Canada/Québec (CA), Belgium (BE), and Switzerland (CH).

Features:
- Canonical regional and domain squads (8 squads: 4 FR, 2 CA, 1 BE, 1 CH).
- Balanced N-shard round-robin partitioning with soft score balancing.
- Anomaly correction for handles.
- Exports shard JSON files, CLI harvester commands, and formatted summaries.
- Strict geographic boundaries (FR, CA, BE, CH only).
"""

from __future__ import annotations

import argparse
import copy
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Set

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.discover_french_channels import (
    ALLOWED_COUNTRY_CODES,
    HANDLE_SUBSTITUTIONS,
)
from scripts.harvest_channel_shard import normalize_uploader_handle

logger = logging.getLogger("partition_french_channels")
if not logger.handlers:
    _handler = logging.StreamHandler(sys.stderr)
    _handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))
    logger.addHandler(_handler)
logger.setLevel(logging.INFO)

# =============================================================================
# Canonical French Regional & Domain Squads Specification (8 Squads)
# =============================================================================

FRENCH_DOMAIN_SQUADS: List[Dict[str, Any]] = [
    {
        "shard_id": 1,
        "squad_name": "FR - Interviews & Deep Dialogue",
        "country_code": "FR",
        "description": "Longform interviews, deep dialogues, intellectual and entrepreneurship talk shows",
        "match_domains": {"interview_talkshow", "interview_entrepreneurship", "interview_geopolitics", "narrative_podcast", "society_podcast", "interview_culture"},
    },
    {
        "shard_id": 2,
        "squad_name": "FR - Banter, Comedy & Street Interviews",
        "country_code": "FR",
        "description": "Spontaneous colloquial banter, standup comedy, street interviews, and comedy sketches",
        "match_domains": {"street_interview", "comedy_talkshow", "standup_comedy", "comedy_sketch", "standup_improvisation", "entertainment_comedy", "entertainment_banter", "entertainment_challenges", "entertainment_vlog", "humor_animation"},
    },
    {
        "shard_id": 3,
        "squad_name": "FR - Knowledge, Philosophy & Pedagogy",
        "country_code": "FR",
        "description": "Popular science, linguistics, philosophy, history vulgarisation, and spoken French pedagogy",
        "match_domains": {"linguistics_french", "popular_science", "history_vulgarisation", "philosophy", "philosophy_culture", "thought_experiments", "cinema_analysis", "cultural_linguistics", "knowledge_curiosities", "science_mathematics", "astronomy_philosophy", "pedagogy_culture", "pedagogy_spoken", "pedagogy_vlog", "street_interview_pedagogy"},
    },
    {
        "shard_id": 4,
        "squad_name": "FR - Media, News & Gaming Banter",
        "country_code": "FR",
        "description": "Journalism, radio broadcasts, investigative media, and gaming/streaming banter",
        "match_domains": {"journalism_news", "radio_broadcast", "investigative_journalism", "politics_public_senat", "youth_radio", "talkshow_culture", "pop_culture", "news_society", "retro_gaming_humor", "gaming_banter", "gaming_streaming", "esports_streaming", "interview_gaming", "tech_talkshow", "interview_fitness", "documentary_culture", "documentary_vlog"},
    },
    {
        "shard_id": 5,
        "squad_name": "CA - Québec Podcasts & Spoken Joual",
        "country_code": "CA",
        "description": "Québécois comedy podcasts, longform discussions, and authentic spoken Joual",
        "match_domains": {"comedy_podcast", "comedy_talkshow", "interview_podcast", "lifestyle_podcast", "youth_talkshow", "sports_talkshow", "daily_podcast", "pop_culture_banter", "truecrime_internet", "comedy_satire", "comedy_sketch"},
    },
    {
        "shard_id": 6,
        "squad_name": "CA - Québec Media, Science & Vlogs",
        "country_code": "CA",
        "description": "Québécois public broadcasting, journalism, popular science, and cultural vlogs",
        "match_domains": {"public_broadcast", "public_journalism", "popular_science", "geopolitics_analysis", "urbanism_history", "lifestyle_vlog", "internet_culture", "food_review_vlog", "culture_talkshow", "society_talkshow", "lifestyle_spirituality"},
    },
    {
        "shard_id": 7,
        "squad_name": "BE - Belgium Creators, Media & Satire",
        "country_code": "BE",
        "description": "Belgian comedy, satire, media, radio broadcasts, and lifestyle podcasts",
        "match_domains": set(),  # All BE channels route here
    },
    {
        "shard_id": 8,
        "squad_name": "CH - Switzerland Romandy & RTS Culture",
        "country_code": "CH",
        "description": "Swiss Romand public broadcasting, satire, standup comedy, and youth culture",
        "match_domains": set(),  # All CH channels route here
    },
]


def load_french_catalog(
    catalog_path: str | Path = "data/french_channels_catalog.json",
    dedup: bool = True,
) -> List[Dict[str, Any]]:
    """Load and validate French channels catalog.
    
    Args:
        catalog_path: File path to catalog JSON.
        dedup: If True, deduplicate channels by canonical handle.
        
    Returns:
        List of audited channel dictionaries.
    """
    p = Path(catalog_path)
    if not p.is_file():
        raise FileNotFoundError(f"Catalog file not found: {catalog_path}")

    with open(p, "r", encoding="utf-8") as f:
        raw = json.load(f)

    if not isinstance(raw, dict) or "channels" not in raw:
        raise ValueError("Invalid catalog format: missing 'channels' list")

    channels = raw.get("channels", [])
    result: List[Dict[str, Any]] = []
    seen_handles: Set[str] = set()

    for ch in channels:
        raw_handle = ch.get("uploader_id") or ch.get("handle") or ""
        norm_h = normalize_uploader_handle(raw_handle)
        if not norm_h:
            continue

        if norm_h in HANDLE_SUBSTITUTIONS:
            norm_h = HANDLE_SUBSTITUTIONS[norm_h]

        ch_copy = copy.deepcopy(ch)
        ch_copy["uploader_id"] = norm_h
        ch_copy["handle"] = norm_h
        if "channel_url" not in ch_copy:
            ch_copy["channel_url"] = f"https://www.youtube.com/{norm_h}"

        cc = str(ch_copy.get("country_code", "FR")).strip().upper()
        if cc not in ALLOWED_COUNTRY_CODES:
            logger.warning("Skipping channel %s with unauthorized country %s", norm_h, cc)
            continue
        ch_copy["country_code"] = cc

        if dedup:
            h_key = norm_h.lower()
            if h_key in seen_handles:
                continue
            seen_handles.add(h_key)

        result.append(ch_copy)

    return result


def partition_by_squads(
    channels: List[Dict[str, Any]],
    squad_specs: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """Partition channels into canonical regional/domain squads."""
    specs = squad_specs or FRENCH_DOMAIN_SQUADS
    shards: List[Dict[str, Any]] = []

    for spec in specs:
        shards.append(
            {
                "shard_id": spec["shard_id"],
                "squad_name": spec["squad_name"],
                "country_code": spec["country_code"],
                "description": spec["description"],
                "channels": [],
            }
        )

    # Helper maps
    squad_map = {s["shard_id"]: s for s in shards}

    for ch in channels:
        cc = ch.get("country_code", "FR")
        dom = ch.get("domain", "")

        assigned_id: Optional[int] = None
        if cc == "BE":
            assigned_id = 7
        elif cc == "CH":
            assigned_id = 8
        elif cc == "CA":
            # Shard 5 (podcasts/banter) vs Shard 6 (media/science/vlogs)
            if dom in FRENCH_DOMAIN_SQUADS[4]["match_domains"]:
                assigned_id = 5
            else:
                assigned_id = 6
        elif cc == "FR":
            # Match FR squads 1, 2, 3, 4
            matched = False
            for sq_idx in [0, 1, 2, 3]:
                if dom in FRENCH_DOMAIN_SQUADS[sq_idx]["match_domains"]:
                    assigned_id = FRENCH_DOMAIN_SQUADS[sq_idx]["shard_id"]
                    matched = True
                    break
            if not matched:
                assigned_id = 4  # Default FR fallback to general media
        else:
            assigned_id = 4

        squad_map[assigned_id]["channels"].append(ch)

    # Compute aggregate stats per shard
    for s in shards:
        chs = s["channels"]
        s["channel_count"] = len(chs)
        s["total_videos"] = sum(c.get("audited_videos_count", 0) for c in chs)
        s["total_manual_cc"] = sum(c.get("manual_fr_cc_count", 0) for c in chs)
        s["total_auto_cc"] = sum(c.get("auto_fr_cc_count", 0) for c in chs)

    return shards


def partition_by_shards(
    channels: List[Dict[str, Any]],
    num_shards: int = 4,
) -> List[Dict[str, Any]]:
    """Partition channels into N balanced shards using soft-score descending round-robin."""
    if num_shards < 1:
        raise ValueError("num_shards must be at least 1")

    # Sort channels by soft_score descending
    sorted_chs = sorted(channels, key=lambda c: c.get("soft_score", 0), reverse=True)

    shards: List[Dict[str, Any]] = []
    for i in range(1, num_shards + 1):
        shards.append(
            {
                "shard_id": i,
                "squad_name": f"French Balanced Shard {i}",
                "country_code": "MULTI",
                "description": f"Balanced shard {i} of {num_shards}",
                "channels": [],
            }
        )

    # Distribute round-robin
    for idx, ch in enumerate(sorted_chs):
        target_shard = shards[idx % num_shards]
        target_shard["channels"].append(ch)

    for s in shards:
        chs = s["channels"]
        s["channel_count"] = len(chs)
        s["total_videos"] = sum(c.get("audited_videos_count", 0) for c in chs)
        s["total_manual_cc"] = sum(c.get("manual_fr_cc_count", 0) for c in chs)
        s["total_auto_cc"] = sum(c.get("auto_fr_cc_count", 0) for c in chs)

    return shards


def export_shards(
    shards: List[Dict[str, Any]],
    output_dir: str | Path = "data/shards/french",
) -> List[Path]:
    """Export partitioned shards to individual JSON catalog files."""
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    exported_paths: List[Path] = []

    for shard in shards:
        shard_id = shard["shard_id"]
        out_file = out_dir / f"shard_{shard_id}.json"
        shard_catalog = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "language_iso1": "fr",
            "shard_id": shard_id,
            "squad_name": shard.get("squad_name", f"Shard {shard_id}"),
            "country_code": shard.get("country_code", "FR"),
            "description": shard.get("description", ""),
            "total_channels_audited": len(shard["channels"]),
            "total_videos_audited": shard.get("total_videos", 0),
            "total_manual_fr_cc_videos": shard.get("total_manual_cc", 0),
            "channels": shard["channels"],
        }
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(shard_catalog, f, indent=2, ensure_ascii=False)
        exported_paths.append(out_file)

    logger.info("Exported %d shards to %s", len(exported_paths), out_dir)
    return exported_paths


def get_shard_cli_command(
    shard_id: int,
    shard_catalog_path: str | Path,
    lang: str = "fr",
    staging_db_dir: str = "data/staging",
) -> str:
    """Format the exact harvest_channel_shard.py CLI command for a shard."""
    cat_str = str(shard_catalog_path)
    return (
        f"uv run python scripts/harvest_channel_shard.py "
        f"--shard-id {shard_id} "
        f"--catalog {cat_str} "
        f"--lang {lang} "
        f"--staging-db {staging_db_dir}/shard_{shard_id}.db"
    )


def format_summary_table(shards: List[Dict[str, Any]]) -> str:
    """Render a clean ASCII summary table of the generated shards."""
    lines = [
        "=" * 85,
        f"{'Shard ID':<10} | {'Squad Name':<38} | {'Channels':<9} | {'Videos':<8} | {'Manual CC':<9}",
        "-" * 85,
    ]
    tot_ch = 0
    tot_vid = 0
    tot_man = 0
    for s in shards:
        s_id = s["shard_id"]
        s_name = s.get("squad_name", f"Shard {s_id}")[:36]
        n_ch = s.get("channel_count", len(s.get("channels", [])))
        n_vid = s.get("total_videos", 0)
        n_man = s.get("total_manual_cc", 0)

        tot_ch += n_ch
        tot_vid += n_vid
        tot_man += n_man

        lines.append(f"{s_id:<10} | {s_name:<38} | {n_ch:<9} | {n_vid:<8} | {n_man:<9}")

    lines.append("-" * 85)
    lines.append(f"{'TOTAL':<10} | {'All Shards Combined':<38} | {tot_ch:<9} | {tot_vid:<8} | {tot_man:<9}")
    lines.append("=" * 85)
    return "\n".join(lines)


# =============================================================================
# CLI Interface
# =============================================================================

def build_parser() -> argparse.ArgumentParser:
    """Build CLI parser for partition_french_channels."""
    parser = argparse.ArgumentParser(
        description="Partition audited French YouTube channels into balanced harvest shards.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--catalog",
        default="data/french_channels_catalog.json",
        help="Path to audited French catalog JSON file.",
    )
    parser.add_argument(
        "--output-dir",
        default="data/shards/french",
        help="Directory to save individual shard JSON files.",
    )
    parser.add_argument(
        "--squads",
        action="store_true",
        help="Partition using the 8 canonical Francophone regional/domain squads.",
    )
    parser.add_argument(
        "--num-shards",
        type=int,
        default=None,
        help="Partition into N balanced shards (overrides --squads if specified).",
    )
    parser.add_argument(
        "--export-cli",
        action="store_true",
        help="Print the exact harvest_channel_shard.py commands for every shard.",
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Print summary table of generated shards.",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        channels = load_french_catalog(args.catalog)
        if not channels:
            logger.error("No valid Francophone channels loaded from %s", args.catalog)
            return 1

        if args.num_shards is not None:
            shards = partition_by_shards(channels, num_shards=args.num_shards)
        else:
            # Default to canonical 8 squads
            shards = partition_by_squads(channels)

        exported = export_shards(shards, output_dir=args.output_dir)

        summary_table = format_summary_table(shards)
        print(summary_table)

        if args.export_cli:
            print("\n[+] Harvest Commands for Shards:")
            for s, path in zip(shards, exported):
                cmd = get_shard_cli_command(s["shard_id"], path, lang="fr")
                print(f"  {cmd}")

        return 0
    except Exception as exc:
        logger.error("Partitioning failed: %s", exc, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
