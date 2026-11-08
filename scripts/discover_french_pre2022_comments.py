#!/usr/bin/env python3
"""French Pre-2022 YouTube Comments Discovery & Catalog Generator.

Discovers authentic pre-2022 YouTube videos and harvested comment threads
across Francophone jurisdictions (FR, CA, BE, CH) from audited channels
in `data/french_channels_catalog.json`.

Strict Pre-2022 Temporal Cutoff:
  timestamp < 1640995200 (2022-01-01T00:00:00Z)
  date < "2022-01-01"
  All post-2021 comments and videos are strictly rejected.

Geographic Invariance:
  Only {"FR", "CA", "BE", "CH"} are permitted.
  African and Maghreb jurisdictions are strictly excluded.

Schema Compliance:
  Strict parity with PROJECT.md § Interface Contracts and
  tests/unit/test_discover_french_tri_source.py.
"""

from __future__ import annotations

import argparse
import concurrent.futures
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import re
import sys
from typing import Any, Dict, List, Optional, Set, Tuple

import yt_dlp

# Project Root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CHANNELS_CATALOG = PROJECT_ROOT / "data" / "french_channels_catalog.json"
DEFAULT_OUTPUT_CATALOG = PROJECT_ROOT / "data" / "french_pre2022_comments_catalog.json"

# Strict Cutoff Constants
PRE_2022_EPOCH_CUTOFF: int = 1640995200  # 2022-01-01T00:00:00Z
PRE_2022_DATE_CUTOFF: str = "2022-01-01"
MIN_VALID_EPOCH: int = 1104537600  # 2005-01-01T00:00:00Z (YouTube launch year)

ALLOWED_COUNTRY_CODES: Set[str] = {"FR", "CA", "BE", "CH"}
FORBIDDEN_AFRICAN_MAGHREB_CODES: Set[str] = {
    "DZ", "MA", "TN", "EG", "LY", "MR", "SN", "CI", "CM", "CD",
    "CG", "ML", "BF", "NE", "GN", "TD", "RW", "BI", "BJ", "TG",
    "GA", "DJ", "CF", "KM", "SC", "GQ", "MU", "MG", "HT", "VU",
}

# Spam & bot detection patterns
SPAM_URL_PATTERN = re.compile(
    r"(https?://|www\.|t\.me/|wa\.me/|bit\.ly/|youtu\.be/|\.com/|\.gg/)",
    re.IGNORECASE,
)
SPAM_KEYWORD_PATTERN = re.compile(
    r"(whatsapp|telegram|invest|crypto|bitcoin|forex|check my bio|abonnez-vous|sub to my channel)",
    re.IGNORECASE,
)

# Common French grammatical tokens for language verification
FRENCH_STOPWORDS: Set[str] = {
    "le", "la", "les", "un", "une", "des", "du", "de", "d'un", "d'une",
    "et", "en", "que", "qui", "dans", "pour", "pas", "est", "sont",
    "je", "tu", "il", "elle", "on", "nous", "vous", "ils", "elles",
    "au", "aux", "avec", "sur", "mais", "ou", "donc", "or", "ni", "car",
    "plus", "tout", "tous", "faire", "fait", "très", "bien", "merci",
    "vidéo", "trop", "c'est", "j'ai", "qu'on", "qu'il", "qu'elle", "ça",
    "comme", "cette", "cet", "ces", "mon", "ma", "mes", "ton", "ta", "tes",
    "son", "sa", "ses", "notre", "nos", "votre", "vos", "leur", "leurs",
    "quoi", "pourquoi", "quand", "comment", "vraiment", "toujours", "aussi",
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
)
logger = logging.getLogger("discover_french_pre2022_comments")


def is_french_comment_thread(comments: List[Dict[str, Any]], min_fr_tokens: int = 3) -> bool:
    """Validate that the comment thread is genuinely in French."""
    all_text = " ".join(c.get("text", "").lower() for c in comments)
    tokens = set(re.findall(r"\b[a-zàâçéèêëîïôûùüÿñæœ']+\b", all_text))
    return len(tokens.intersection(FRENCH_STOPWORDS)) >= min_fr_tokens


def parse_date_to_iso(date_str: Optional[str]) -> Optional[str]:
    """Parse various date formats (YYYYMMDD, YYYY-MM-DD) to ISO YYYY-MM-DD."""
    if not date_str:
        return None
    cleaned = date_str.strip()
    if len(cleaned) == 8 and cleaned.isdigit():
        return f"{cleaned[:4]}-{cleaned[4:6]}-{cleaned[6:8]}"
    if len(cleaned) == 10 and cleaned[4] == "-" and cleaned[7] == "-":
        return cleaned
    try:
        # Try ISO parsing
        dt = datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def format_epoch_to_iso(epoch_ts: int) -> str:
    """Format integer Unix epoch timestamp to YYYY-MM-DD."""
    dt = datetime.fromtimestamp(epoch_ts, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def is_spam_or_bot(text: str) -> bool:
    """Detect promotional bot/spam comments."""
    if len(text.strip()) < 3:
        return True
    if SPAM_URL_PATTERN.search(text):
        return True
    if SPAM_KEYWORD_PATTERN.search(text):
        return True
    # Low-entropy / repetitive check
    stripped = text.strip()
    if len(set(stripped)) <= 2 and len(stripped) > 5:
        return True
    return False


def clean_comment_text(raw_text: str) -> str:
    """Normalize and clean comment text."""
    # Collapse excess whitespace while preserving French typography
    lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
    return " ".join(lines)


def filter_pre2022_comments(raw_comments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Extract and validate comments strictly preceding 2022-01-01."""
    valid_comments: List[Dict[str, Any]] = []

    for idx, c in enumerate(raw_comments):
        ts = c.get("timestamp")
        # Must have numeric timestamp
        if ts is None or not isinstance(ts, (int, float)):
            continue
        ts_int = int(ts)

        # Enforce temporal cutoff
        if ts_int >= PRE_2022_EPOCH_CUTOFF or ts_int < MIN_VALID_EPOCH:
            continue

        c_date = format_epoch_to_iso(ts_int)
        if c_date >= PRE_2022_DATE_CUTOFF:
            continue

        raw_text = c.get("text") or ""
        text = clean_comment_text(raw_text)
        if is_spam_or_bot(text):
            continue

        cid = str(c.get("id") or f"c_{idx}")
        author = str(c.get("author") or c.get("author_id") or "anonymous")

        valid_comments.append({
            "comment_id": cid,
            "id": cid,
            "text": text,
            "author": author,
            "timestamp": ts_int,
            "date": c_date,
        })

    return valid_comments


def discover_channel_candidate_video_ids(
    channel: Dict[str, Any],
    max_candidates: int = 4,
) -> List[str]:
    """Discover candidate video IDs published strictly before 2022 for a channel."""
    title = channel.get("channel_title", "")
    handle = channel.get("uploader_id", "")
    ch_url = channel.get("channel_url", "")

    candidate_ids: List[str] = []
    seen_ids: Set[str] = set()

    ydl_flat_opts = {
        "extract_flat": True,
        "quiet": True,
        "no_warnings": True,
        "ignore_no_formats_error": True,
    }

    with yt_dlp.YoutubeDL(ydl_flat_opts) as ydl:
        # Search strategy 1: Targeted query with date cutoff
        queries = []
        if title:
            queries.append(f'ytsearch{max_candidates * 2}:"{title}" before:2022-01-01')
        if handle and handle.startswith("@"):
            queries.append(f'ytsearch{max_candidates * 2}:{handle} before:2022-01-01')

        for q in queries:
            try:
                res = ydl.extract_info(q, download=False)
                entries = res.get("entries", []) if res else []
                for ent in entries:
                    vid = ent.get("id")
                    if vid and vid not in seen_ids:
                        # Check title or uploader match if possible
                        seen_ids.add(vid)
                        candidate_ids.append(vid)
                    if len(candidate_ids) >= max_candidates * 2:
                        break
            except Exception as e:
                logger.debug("Query '%s' failed: %s", q, e)

            if len(candidate_ids) >= max_candidates:
                break

        # Fallback strategy: Channel videos tab flat playlist (tail entries are oldest)
        if len(candidate_ids) < max_candidates:
            target_url = ch_url or (f"https://www.youtube.com/{handle}" if handle else "")
            if target_url:
                videos_tab = f"{target_url.rstrip('/')}/videos"
                try:
                    res = ydl.extract_info(videos_tab, download=False)
                    entries = res.get("entries", []) if res else []
                    # In YouTube playlists, tail entries are oldest uploads
                    oldest_entries = entries[-max_candidates * 3:]
                    for ent in reversed(oldest_entries):
                        vid = ent.get("id")
                        if vid and vid not in seen_ids:
                            seen_ids.add(vid)
                            candidate_ids.append(vid)
                        if len(candidate_ids) >= max_candidates * 2:
                            break
                except Exception as e:
                    logger.debug("Videos tab fetch for '%s' failed: %s", title, e)

    return candidate_ids[:max_candidates]


def probe_video_comments(
    video_id: str,
    channel: Dict[str, Any],
    max_comments: int = 50,
    min_pre2022_comments: int = 1,
) -> Optional[Dict[str, Any]]:
    """Probe video metadata and comment section using yt-dlp."""
    video_url = f"https://www.youtube.com/watch?v={video_id}"

    ydl_opts = {
        "skip_download": True,
        "getcomments": True,
        "extractor_args": {
            "youtube": {
                "comment_sort": ["top"],
                "max_comments": [str(max_comments), "all", "10"],
            }
        },
        "quiet": True,
        "no_warnings": True,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(video_url, download=False)
            if not info:
                return None

            # Validate video publication date
            upload_date_raw = info.get("upload_date")
            video_date = parse_date_to_iso(upload_date_raw)
            if not video_date or video_date >= PRE_2022_DATE_CUTOFF:
                logger.debug("Video %s upload_date %s violates pre-2022 cutoff.", video_id, video_date)
                return None

            # Harvest comments
            raw_comments = info.get("comments", []) or []
            valid_comments = filter_pre2022_comments(raw_comments)

            if len(valid_comments) < min_pre2022_comments:
                logger.debug(
                    "Video %s yielded %d pre-2022 comments (< min %d).",
                    video_id,
                    len(valid_comments),
                    min_pre2022_comments,
                )
                return None

            # Verify authentic French language thread
            if not is_french_comment_thread(valid_comments, min_fr_tokens=3):
                logger.debug(
                    "Video %s comments failed French language authenticity check.",
                    video_id,
                )
                return None

            # Formulate catalog item matching interface contracts
            country_code = channel["country_code"]
            region = channel.get("region", "Metropolitan France")
            video_title = info.get("title") or channel.get("channel_title", "Vidéo")
            item_title = f"{video_title} [Pre-2022 Comments]"

            item: Dict[str, Any] = {
                "item_id": f"yt_comment_{video_id}",
                "title": item_title,
                "country_code": country_code,
                "region": region,
                "platform": "youtube",
                "payload_type": "comment_thread",
                "origin_type": "human_written",
                "format": "json",
                "source_url": video_url,
                "date": video_date,
                "metadata": {
                    "channel_id": channel.get("channel_id") or info.get("channel_id") or "",
                    "channel_title": channel.get("channel_title") or info.get("uploader") or "",
                    "uploader_id": channel.get("uploader_id") or info.get("uploader_id") or "",
                    "video_id": video_id,
                    "view_count": info.get("view_count") or 0,
                    "comments_count": len(valid_comments),
                    "comments": valid_comments,
                },
            }
            return item

    except Exception as exc:
        logger.debug("Failed probing video %s: %s", video_id, exc)
        return None


def process_single_channel(
    channel: Dict[str, Any],
    max_videos_per_channel: int = 2,
    max_comments_per_video: int = 50,
    min_pre2022_comments: int = 1,
) -> List[Dict[str, Any]]:
    """Process a single channel to harvest pre-2022 comment items."""
    cc = channel.get("country_code", "")
    if cc not in ALLOWED_COUNTRY_CODES or cc in FORBIDDEN_AFRICAN_MAGHREB_CODES:
        return []

    title = channel.get("channel_title", "")
    candidate_ids = discover_channel_candidate_video_ids(
        channel,
        max_candidates=max(max_videos_per_channel * 4, 4),
    )

    harvested_items: List[Dict[str, Any]] = []
    for vid in candidate_ids:
        item = probe_video_comments(
            video_id=vid,
            channel=channel,
            max_comments=max_comments_per_video,
            min_pre2022_comments=min_pre2022_comments,
        )
        if item:
            harvested_items.append(item)
            logger.info(
                "[%s] %s -> Found pre-2022 video %s (%s) with %d comments",
                cc,
                title,
                vid,
                item["date"],
                len(item["metadata"]["comments"]),
            )
        if len(harvested_items) >= max_videos_per_channel:
            break

    return harvested_items


def build_french_pre2022_comments_catalog(
    channels_catalog_path: Path = DEFAULT_CHANNELS_CATALOG,
    output_catalog_path: Path = DEFAULT_OUTPUT_CATALOG,
    max_channels: Optional[int] = None,
    max_videos_per_channel: int = 2,
    max_comments_per_video: int = 50,
    min_pre2022_comments: int = 1,
    workers: int = 8,
    country_filter: Optional[str] = None,
) -> Dict[str, Any]:
    """Build and write the pre-2022 comments catalog."""
    if not channels_catalog_path.is_file():
        raise FileNotFoundError(f"Channels catalog not found: {channels_catalog_path}")

    with open(channels_catalog_path, "r", encoding="utf-8") as f:
        channels_data = json.load(f)

    all_channels: List[Dict[str, Any]] = channels_data.get("channels", [])

    # Filter by country if specified
    if country_filter:
        cf = country_filter.upper()
        all_channels = [ch for ch in all_channels if ch.get("country_code") == cf]

    # Validate channels
    valid_channels = [
        ch for ch in all_channels
        if ch.get("country_code") in ALLOWED_COUNTRY_CODES
        and ch.get("country_code") not in FORBIDDEN_AFRICAN_MAGHREB_CODES
    ]

    # Ensure all jurisdictions are covered by balancing channel selection
    if max_channels and max_channels < len(valid_channels):
        # Stratify by country code
        by_cc: Dict[str, List[Dict[str, Any]]] = {}
        for ch in valid_channels:
            by_cc.setdefault(ch["country_code"], []).append(ch)

        selected: List[Dict[str, Any]] = []
        # Round robin across countries
        while len(selected) < max_channels and any(by_cc.values()):
            for cc in sorted(by_cc.keys()):
                if by_cc[cc]:
                    selected.append(by_cc[cc].pop(0))
                if len(selected) >= max_channels:
                    break
        target_channels = selected
    else:
        target_channels = valid_channels

    logger.info(
        "Starting pre-2022 comment discovery across %d channels using %d workers...",
        len(target_channels),
        workers,
    )

    all_harvested_items: List[Dict[str, Any]] = []
    seen_item_ids: Set[str] = set()

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_ch = {
            executor.submit(
                process_single_channel,
                ch,
                max_videos_per_channel,
                max_comments_per_video,
                min_pre2022_comments,
            ): ch
            for ch in target_channels
        }

        for future in concurrent.futures.as_completed(future_to_ch):
            ch = future_to_ch[future]
            try:
                items = future.result()
                for item in items:
                    iid = item["item_id"]
                    if iid not in seen_item_ids:
                        seen_item_ids.add(iid)
                        all_harvested_items.append(item)
            except Exception as e:
                logger.error("Error processing channel %s: %s", ch.get("channel_title"), e)

    # Sort items by country_code, then date
    all_harvested_items.sort(key=lambda x: (x.get("country_code", ""), x.get("date", "")))

    catalog_data: Dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "language_iso1": "fr",
        "stream_modality": "comment_thread",
        "allowed_country_codes": sorted(list(ALLOWED_COUNTRY_CODES)),
        "total_items": len(all_harvested_items),
        "items": all_harvested_items,
    }

    # Ensure parent output directory exists
    output_catalog_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_catalog_path, "w", encoding="utf-8") as f:
        json.dump(catalog_data, f, ensure_ascii=False, indent=2)

    logger.info(
        "Successfully wrote pre-2022 comments catalog with %d items to %s",
        len(all_harvested_items),
        output_catalog_path,
    )

    return catalog_data


def main() -> None:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description="Discover and catalog French pre-2022 YouTube comment threads."
    )
    parser.add_argument(
        "--channels-catalog",
        type=Path,
        default=DEFAULT_CHANNELS_CATALOG,
        help="Path to source channels catalog JSON.",
    )
    parser.add_argument(
        "--output-catalog",
        type=Path,
        default=DEFAULT_OUTPUT_CATALOG,
        help="Path to output comments catalog JSON.",
    )
    parser.add_argument(
        "--max-channels",
        type=int,
        default=None,
        help="Maximum channels to probe (default: all).",
    )
    parser.add_argument(
        "--max-videos-per-channel",
        type=int,
        default=2,
        help="Maximum pre-2022 videos per channel (default: 2).",
    )
    parser.add_argument(
        "--max-comments-per-video",
        type=int,
        default=50,
        help="Max comments to request from yt-dlp per video (default: 50).",
    )
    parser.add_argument(
        "--min-pre2022-comments",
        type=int,
        default=1,
        help="Minimum pre-2022 comments required to accept a video (default: 1).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Concurrent worker threads (default: 8).",
    )
    parser.add_argument(
        "--country",
        type=str,
        default=None,
        choices=["FR", "CA", "BE", "CH"],
        help="Filter to specific country code.",
    )

    args = parser.parse_args()

    catalog = build_french_pre2022_comments_catalog(
        channels_catalog_path=args.channels_catalog,
        output_catalog_path=args.output_catalog,
        max_channels=args.max_channels,
        max_videos_per_channel=args.max_videos_per_channel,
        max_comments_per_video=args.max_comments_per_video,
        min_pre2022_comments=args.min_pre2022_comments,
        workers=args.workers,
        country_filter=args.country,
    )

    # Print summary
    items = catalog.get("items", [])
    countries_count: Dict[str, int] = {}
    total_comments = 0
    for item in items:
        cc = item.get("country_code", "UNKNOWN")
        countries_count[cc] = countries_count.get(cc, 0) + 1
        comments = item.get("metadata", {}).get("comments", [])
        total_comments += len(comments)

    print("\n" + "=" * 60)
    print("FRENCH PRE-2022 YOUTUBE COMMENTS DISCOVERY SUMMARY")
    print("=" * 60)
    print(f"Total Items Discovered: {len(items)}")
    print(f"Total Comments Harvested: {total_comments}")
    print("Items by Country Code:")
    for cc, count in sorted(countries_count.items()):
        print(f"  [{cc}]: {count} items")
    print("=" * 60)


if __name__ == "__main__":
    main()
