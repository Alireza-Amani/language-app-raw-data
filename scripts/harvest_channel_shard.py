#!/usr/bin/env python3
"""Persian Speech Harvester: Subtitle Resolution & Shard Harvesting Engine.

Milestone 2 Core Component:
- Extracts manual Persian CC (fa, fa-IR, fas) as priority 1 (human_transcribed).
- Falls back to automatic Persian ASR captions (fa, fa-IR, fas, fa-orig) as priority 2 (machine_transcribed).
- Streams WebVTT transcripts purely in memory (0 media bytes written to disk).
- Computes SHA-256 cryptographic provenance digest for deduplication.
- Filters candidate videos by duration (duration >= min_duration).
- Enforces per-channel capping policy (sorted by view_count DESC) with --all-videos bypass.
- Maintains isolated SQLite staging databases (shard_<id>.db) with WAL mode, busy timeout, and FK enforcement.
- Provides resilient execution and idempotent resumption.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import sqlite3
import sys
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import urllib.error
import urllib.request

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.vault.db import VaultManager

# Configure logging
logger = logging.getLogger("harvest_channel_shard")
if not logger.handlers:
    _handler = logging.StreamHandler(sys.stderr)
    _handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))
    logger.addHandler(_handler)
logger.setLevel(logging.INFO)

# Default custom User-Agent for streaming requests
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Canonical handle anomaly remediation mapping
HANDLE_REMEDIATIONS: Dict[str, str] = {
    "kouman": "@Kouman",
    "@kouman": "@Kouman",
    "kouman_official": "@Kouman",
    "@kouman_official": "@Kouman",
    "radiomarz": "@radio-marz",
    "@radiomarz": "@radio-marz",
    "radio-marz": "@radio-marz",
    "@radio-marz": "@radio-marz",
    "tabaghe16": "@Tabaghe16",
    "@tabaghe16": "@Tabaghe16",
    "channelbpodcast": "@ChannelBPodcast",
    "@channelbpodcast": "@ChannelBPodcast",
    "bpluspodcast": "@BplusPodcast",
    "@bpluspodcast": "@BplusPodcast",
    "miaplays": "@MiaPlays",
    "@miaplays": "@MiaPlays",
    "chefjavadjavadi": "@ChefJavadJavadi",
    "@chefjavadjavadi": "@ChefJavadJavadi",
    "mehranrowshan": "@MehranRowshan",
    "@mehranrowshan": "@MehranRowshan",
    "navidkashani": "@navidkashani",
    "@navidkashani": "@navidkashani",
    "fictionpodcast": "@FictionPodcast",
    "@fictionpodcast": "@FictionPodcast",
}


class CLIParamError(Exception):
    """Raised when command-line parameters or arguments are invalid."""

    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class ShardHarvestArgumentParser(argparse.ArgumentParser):
    """Custom ArgumentParser converting validation errors into exit code 3."""

    def exit(self, status: int = 0, message: Optional[str] = None) -> None:
        if message:
            self._print_message(message, sys.stderr)
        code = 3 if status != 0 else 0
        raise CLIParamError(code, message or "")

    def error(self, message: str) -> None:
        raise CLIParamError(3, message)


def normalize_uploader_handle(raw_handle: str) -> str:
    """Normalize raw channel handle applying canonical casing and anomaly substitutions."""
    raw = (raw_handle or "").strip()
    if not raw:
        return ""
    raw_lower = raw.lower()
    if raw_lower in HANDLE_REMEDIATIONS:
        return HANDLE_REMEDIATIONS[raw_lower]
    if not raw.startswith("@") and not raw.startswith("UC"):
        fixed = f"@{raw}"
        return HANDLE_REMEDIATIONS.get(fixed.lower(), fixed)
    return raw


# =============================================================================
# 1. Subtitle Resolution Contract
# =============================================================================

LANGUAGE_SUBTITLE_KEYS: Dict[str, Tuple[str, ...]] = {
    "fa": ("fa", "fa-IR", "fas"),
    "fr": (
        "fr",
        "fr-FR",
        "fr-CA",
        "fr-BE",
        "fr-CH",
        "fr-SN",
        "fr-CI",
        "fr-CM",
        "fr-CD",
        "fr-MA",
        "fr-DZ",
        "fr-TN",
        "fr-HT",
        "fra",
        "fre",
    ),
    "es": ("es", "es-ES", "es-MX", "es-419", "es-AR", "es-CO", "spa"),
    "de": ("de", "de-DE", "de-AT", "de-CH", "deu", "ger"),
    "ar": ("ar", "ar-EG", "ar-SA", "ar-AE", "ar-LB", "ar-MA", "ara"),
    "en": ("en", "en-US", "en-GB", "en-CA", "en-AU", "eng"),
}


def resolve_subtitles(
    video_metadata: Optional[Dict[str, Any]],
    language_iso1: str = "fa",
    country_code: Optional[str] = None,
) -> Tuple[Optional[str], str, str, Optional[str], Dict[str, Any]]:
    """Inspect video metadata for target language subtitles (manual CC priority over auto CC).

    Returns:
        (vtt_url, payload_type, origin_type, locale, track_metadata)
    """
    if not isinstance(video_metadata, dict):
        return None, "none", "none", None, {}

    lang_base = (language_iso1 or "fa").strip().lower()
    norm_country = country_code.strip().upper() if isinstance(country_code, str) and country_code.strip() else None

    manual_subs = video_metadata.get("subtitles")
    auto_subs = video_metadata.get("automatic_captions")

    if not isinstance(manual_subs, dict):
        manual_subs = {}
    if not isinstance(auto_subs, dict):
        auto_subs = {}

    def _extract_vtt_url_and_meta(tracks: Any, track_key: str) -> Tuple[Optional[str], Dict[str, Any]]:
        if not isinstance(tracks, list) or not tracks:
            return None, {}

        # Pass 1: Prefer track with ext == "vtt" and valid URL (excluding machine translations with tlang=)
        for track in tracks:
            if isinstance(track, dict) and track.get("ext") == "vtt" and track.get("url"):
                url = str(track["url"])
                if "tlang=" in url:
                    continue
                if "fmt=" in url:
                    url = re.sub(r"fmt=[^&]+", "fmt=vtt", url)
                meta: Dict[str, Any] = {"track_key": track_key}
                if track.get("name"):
                    meta["track_name"] = str(track["name"])
                return url, meta

        # Pass 2: Fall back to first valid track URL and append or replace format query parameter (excluding tlang=)
        for track in tracks:
            if isinstance(track, dict) and track.get("url"):
                url = str(track["url"])
                if "tlang=" in url:
                    continue
                if "fmt=" in url:
                    url = re.sub(r"fmt=[^&]+", "fmt=vtt", url)
                else:
                    delimiter = "&" if "?" in url else "?"
                    url = f"{url}{delimiter}fmt=vtt"
                meta = {"track_key": track_key}
                if track.get("name"):
                    meta["track_name"] = str(track["name"])
                return url, meta

        return None, {}

    def _resolve_locale(matched_key: str) -> str:
        if "-" in matched_key and not matched_key.lower().endswith("-orig"):
            parts = matched_key.split("-", 1)
            return f"{parts[0].lower()}-{parts[1].upper()}" if len(parts[1]) == 2 else matched_key
        if norm_country:
            return f"{lang_base}-{norm_country}"
        return lang_base

    base_candidates = list(LANGUAGE_SUBTITLE_KEYS.get(lang_base, (lang_base,)))
    if norm_country:
        regional_tag = f"{lang_base}-{norm_country}"
        if regional_tag in base_candidates:
            base_candidates.remove(regional_tag)
        base_candidates.insert(0, regional_tag)

    # Include any additional regional keys present in manual_subs (e.g. fr-LU)
    manual_candidates = list(base_candidates)
    for k in manual_subs.keys():
        if isinstance(k, str) and k.lower().startswith(f"{lang_base}-") and not k.lower().endswith("-orig"):
            if k not in manual_candidates:
                manual_candidates.append(k)

    # Priority 1: Manual CC
    for lang_key in manual_candidates:
        if lang_key in manual_subs and manual_subs[lang_key]:
            vtt_url, track_meta = _extract_vtt_url_and_meta(manual_subs[lang_key], lang_key)
            if vtt_url:
                return vtt_url, "manual_cc", "human_transcribed", _resolve_locale(lang_key), track_meta

    # Priority 2: Auto CC
    auto_candidates = list(base_candidates) + [f"{lang_base}-orig"]
    for k in auto_subs.keys():
        if isinstance(k, str) and k.lower().startswith(f"{lang_base}-"):
            if k not in auto_candidates:
                auto_candidates.append(k)

    for lang_key in auto_candidates:
        if lang_key in auto_subs and auto_subs[lang_key]:
            vtt_url, track_meta = _extract_vtt_url_and_meta(auto_subs[lang_key], lang_key)
            if vtt_url:
                return vtt_url, "auto_cc", "machine_transcribed", _resolve_locale(lang_key), track_meta

    # Priority 3: No matching subtitles found
    return None, "none", "none", None, {}


def resolve_persian_subtitles(
    video_metadata: Optional[Dict[str, Any]],
) -> Tuple[Optional[str], str, str]:
    """Inspect video metadata for Persian subtitles (manual CC priority over auto CC).

    Priority:
    - Priority 1: Manual Persian CC: keys ('fa', 'fa-IR', 'fas') in video_metadata.get("subtitles")
      -> (vtt_url, "manual_cc", "human_transcribed").
    - Priority 2: Auto Persian CC fallback: keys ('fa', 'fa-IR', 'fas', 'fa-orig') in
      video_metadata.get("automatic_captions") -> (vtt_url, "auto_cc", "machine_transcribed").
    - Priority 3: Neither exists -> (None, "none", "none").
    """
    vtt_url, p_type, o_type, _, _ = resolve_subtitles(video_metadata, language_iso1="fa")
    return vtt_url, p_type, o_type


# =============================================================================
# 2. Persian Word Counting Helper
# =============================================================================

def count_persian_words_in_vtt(vtt_text: Optional[str]) -> int:
    """Accurately count Persian words in a WebVTT transcript payload using a state machine.

    Robust WebVTT parser state machine:
    - Resets in_cue = False and in_metadata_block = False on empty lines.
    - Skips WEBVTT, Kind:, Language: header lines.
    - Skips NOTE, STYLE, REGION multi-line blocks until the next empty line.
    - Only sets in_cue = True upon encountering '-->'.
    - When in_cue is True: strips inline tags (<...>) and counts words.
    - Ignores cue identifier lines that precede the timing line.
    - Safely returns 0 for None or empty inputs.
    """
    if not vtt_text or not isinstance(vtt_text, str):
        return 0

    words = 0
    in_cue = False
    in_metadata_block = False

    for raw_line in vtt_text.splitlines():
        line = raw_line.strip()
        if not line:
            in_cue = False
            in_metadata_block = False
            continue

        if in_metadata_block:
            continue

        if line.startswith(("WEBVTT", "Kind:", "Language:")):
            continue

        if line.startswith(("NOTE", "STYLE", "REGION")):
            in_metadata_block = True
            continue

        if "-->" in line:
            in_cue = True
            continue

        if in_cue:
            clean_line = re.sub(r"<[^>]+>", "", line)
            tokens = [t for t in clean_line.split() if t]
            words += len(tokens)

    return words


# =============================================================================
# 3. In-Memory Subtitle Streaming
# =============================================================================

def fetch_vtt_in_memory(
    vtt_url: str,
    max_retries: int = 3,
    timeout: int = 25,
) -> Optional[str]:
    """In-memory streaming via HTTP GET using urllib.request.

    - Custom User-Agent header.
    - 0 bytes written to disk (strictly in RAM).
    - Retries with exponential backoff on transient network errors.
    - Returns decoded UTF-8 string or None on persistent failure.
    """
    if not vtt_url or not isinstance(vtt_url, str):
        return None

    req = urllib.request.Request(
        vtt_url,
        headers={"User-Agent": USER_AGENT},
    )

    backoff = 0.5
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                content_bytes = resp.read()
                return content_bytes.decode("utf-8", errors="replace")
        except Exception as exc:
            if attempt == max_retries - 1:
                logger.warning(
                    "fetch_vtt_in_memory failed for %s after %d retries: %s",
                    vtt_url,
                    max_retries,
                    exc,
                )
                return None
            time.sleep(backoff)
            backoff *= 2.0

    return None


# =============================================================================
# 4. Shard Harvesting Engine
# =============================================================================

def harvest_channel_shard(
    shard_id: Union[int, str],
    channels: Union[str, List[Union[str, Dict[str, Any]]], Path],
    staging_dir: Union[str, Path] = "data/staging",
    cap_per_channel: int = 25,
    all_videos: bool = False,
    min_duration: int = 600,
    catalog_path: Optional[Union[str, Path]] = "data/persian_channels_catalog.json",
    dry_run: bool = False,
    vtt_provider: Optional[Callable[[str, str], Optional[str]]] = None,
    channel_video_provider: Optional[Callable[[str], List[Dict[str, Any]]]] = None,
    workers: int = 1,
    live_extraction: bool = False,
    vault_path: Optional[Union[str, Path]] = None,
    language_iso1: str = "fa",
    country_code: Optional[str] = None,
) -> Dict[str, Any]:
    """Execute harvesting for an isolated shard with genuine SQLite writes."""
    numeric_shard_id = int(shard_id) if str(shard_id).isdigit() else shard_id
    staging_p = Path(staging_dir)
    staging_p.mkdir(parents=True, exist_ok=True)
    db_file = staging_p / f"shard_{shard_id}.db"

    # 1. Normalize channels argument into list of handles and capture channel-level metadata
    normalized_channels: List[str] = []
    channel_meta_by_handle: Dict[str, Dict[str, Any]] = {}
    is_json_file = False

    def _record_channel_entry(ch_entry: Any) -> None:
        if isinstance(ch_entry, dict):
            raw_h = ch_entry.get("handle") or ch_entry.get("uploader_id") or ch_entry.get("channel_id") or ""
            h = normalize_uploader_handle(raw_h)
            if h:
                normalized_channels.append(h)
                channel_meta_by_handle.setdefault(h.lower(), {}).update(ch_entry)
        else:
            h = normalize_uploader_handle(str(ch_entry))
            if h:
                normalized_channels.append(h)

    if isinstance(channels, (str, Path)):
        p = Path(channels)
        if p.is_file():
            is_json_file = True
            with open(p, "r", encoding="utf-8") as f:
                content = json.load(f)

            if isinstance(content, dict) and "shards" in content:
                target_shard = next(
                    (s for s in content["shards"] if str(s.get("shard_id")) == str(shard_id)),
                    None,
                )
                if target_shard and "channels" in target_shard:
                    for ch in target_shard["channels"]:
                        _record_channel_entry(ch)
            elif isinstance(content, dict) and "channels" in content:
                for ch in content["channels"]:
                    _record_channel_entry(ch)
            elif isinstance(content, list):
                for ch in content:
                    _record_channel_entry(ch)

    if not is_json_file:
        if isinstance(channels, str):
            raw_list = [c.strip() for c in channels.split(",") if c.strip()]
            for item in raw_list:
                _record_channel_entry(item)
        elif isinstance(channels, list):
            for item in channels:
                _record_channel_entry(item)

    # Deduplicate normalized handles while preserving order
    unique_channels: List[str] = []
    seen_channels = set()
    for h in normalized_channels:
        if h and h.lower() not in seen_channels:
            seen_channels.add(h.lower())
            unique_channels.append(h)

    # 2. Pre-load catalog video entries and channel metadata if catalog_path is present
    catalog_videos_by_channel: Dict[str, List[Dict[str, Any]]] = {}
    if catalog_path and Path(catalog_path).is_file():
        try:
            with open(catalog_path, "r", encoding="utf-8") as f:
                cat_data = json.load(f)
            for ch in cat_data.get("channels", []):
                h = normalize_uploader_handle(
                    ch.get("uploader_id") or ch.get("channel_id") or ch.get("handle") or ""
                )
                if h:
                    catalog_videos_by_channel[h.lower()] = ch.get("videos", [])
                    meta_copy = {k: v for k, v in ch.items() if k != "videos"}
                    existing_meta = channel_meta_by_handle.setdefault(h.lower(), {})
                    for mk, mv in meta_copy.items():
                        existing_meta.setdefault(mk, mv)
        except Exception as exc:
            logger.warning("Failed to load catalog from %s: %s", catalog_path, exc)

    # 3. Initialize SQLite database & VaultManager
    vm = VaultManager(db_path=db_file)
    if not dry_run:
        vm.init_db()
        conn = vm.conn
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA busy_timeout = 5000;")
        shard_item_ids = {row[0] for row in conn.execute("SELECT id FROM items;").fetchall()}
        shard_payload_items = {
            row[0] for row in conn.execute("SELECT item_id FROM payloads;").fetchall()
        }
        vault_completed_items = set()
        if vault_path and Path(vault_path).is_file():
            try:
                vconn = sqlite3.connect(str(vault_path))
                for row in vconn.execute("SELECT item_id FROM payloads;").fetchall():
                    vault_completed_items.add(row[0])
                vconn.close()
            except Exception:
                pass
    else:
        shard_item_ids = set()
        shard_payload_items = set()
        vault_completed_items = set()

    metrics: Dict[str, Any] = {
        "shard_id": numeric_shard_id,
        "db_path": str(db_file),
        "sources_processed": 0,
        "eligible_videos_scanned": 0,
        "items_cataloged": 0,
        "payloads_inserted": 0,
        "manual_cc_count": 0,
        "auto_cc_count": 0,
        "total_persian_words": 0,
        "resumed_skipped_count": 0,
    }

    db_lock = threading.RLock()

    # 4. Process each channel
    for ch_handle in unique_channels:
        norm_handle = normalize_uploader_handle(ch_handle)
        ch_meta = channel_meta_by_handle.get(norm_handle.lower(), {})
        ch_lang = str(ch_meta.get("language_iso1") or language_iso1 or "fa").strip().lower()
        ch_country_raw = ch_meta.get("country_code") or ch_meta.get("country") or country_code
        ch_country = str(ch_country_raw).strip().upper() if ch_country_raw else None

        source_id = f"yt:{norm_handle}"
        source_name = ch_meta.get("channel_title") or ch_meta.get("name") or norm_handle.lstrip("@")
        source_url = ch_meta.get("channel_url") or f"https://www.youtube.com/{norm_handle}"

        source_metadata: Dict[str, Any] = {"handle": norm_handle, "shard_id": numeric_shard_id}
        for optional_key in ("country_code", "region", "register", "interaction_type", "domain", "dialect"):
            if optional_key in ch_meta and ch_meta[optional_key] is not None:
                source_metadata[optional_key] = ch_meta[optional_key]
        if ch_country and "country_code" not in source_metadata:
            source_metadata["country_code"] = ch_country

        if not dry_run:
            with db_lock:
                vm.upsert_source(
                    id=source_id,
                    platform="youtube",
                    language_iso1=ch_lang,
                    country_code=ch_country,
                    name=source_name,
                    url=source_url,
                    metadata=source_metadata,
                )
        metrics["sources_processed"] += 1

        # Retrieve video entries
        videos: List[Dict[str, Any]] = []
        if channel_video_provider is not None:
            videos = channel_video_provider(norm_handle) or []
            if not videos and not norm_handle.startswith("@"):
                videos = channel_video_provider(f"@{norm_handle}") or []
            elif not videos and norm_handle.startswith("@"):
                videos = channel_video_provider(norm_handle.lstrip("@")) or []
        elif (
            not live_extraction
            and norm_handle.lower() in catalog_videos_by_channel
            and catalog_videos_by_channel[norm_handle.lower()]
        ):
            videos = catalog_videos_by_channel[norm_handle.lower()]
        elif not dry_run:
            # Live flat playlist extraction with yt_dlp
            try:
                import yt_dlp

                ydl_opts = {
                    "extract_flat": True,
                    "quiet": True,
                    "no_warnings": True,
                    "extractor_args": {"youtube": {"player_client": ["mweb", "web", "android"]}},
                }
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    channel_url = f"https://www.youtube.com/{norm_handle}/videos"
                    try:
                        info = ydl.extract_info(channel_url, download=False)
                    except Exception:
                        channel_url = f"https://www.youtube.com/{norm_handle}"
                        info = ydl.extract_info(channel_url, download=False)
                    videos = list(info.get("entries", [])) if info else []
            except Exception as exc:
                logger.warning("yt_dlp extraction failed for %s: %s", norm_handle, exc)
                videos = []

        # Duration filter: duration >= min_duration (skips Shorts <60s, promos, None)
        candidate_videos: List[Dict[str, Any]] = []
        for vid in videos:
            dur = (
                vid.get("duration_seconds")
                if vid.get("duration_seconds") is not None
                else vid.get("duration")
            )
            if dur is None or dur < min_duration:
                continue
            candidate_videos.append(vid)

        # Sort candidate videos: view_count DESC (None sorts last)
        candidate_videos.sort(
            key=lambda v: (v.get("view_count") if v.get("view_count") is not None else -1),
            reverse=True,
        )

        # Capping policy: slice top cap_per_channel unless all_videos is True
        if not all_videos and cap_per_channel is not None and cap_per_channel > 0:
            target_videos = candidate_videos[:cap_per_channel]
        else:
            target_videos = candidate_videos

        metrics["eligible_videos_scanned"] += len(target_videos)

        def _process_video(vid: Dict[str, Any]) -> None:
            v_id = vid.get("video_id") or vid.get("id")
            if not v_id:
                return

            v_id_str = str(v_id)
            item_id = f"yt:{v_id_str}" if not v_id_str.startswith("yt:") else v_id_str
            title = vid.get("title") or f"Video {v_id_str}"
            v_url = (
                vid.get("video_url")
                or vid.get("webpage_url")
                or f"https://www.youtube.com/watch?v={v_id_str}"
            )
            published_at = vid.get("upload_date") or vid.get("published_at")
            dur = (
                vid.get("duration_seconds")
                if vid.get("duration_seconds") is not None
                else vid.get("duration")
            )
            view_count = vid.get("view_count")

            # Check idempotent resumption: already has payload in vault or in current shard
            with db_lock:
                if item_id in vault_completed_items or item_id in shard_payload_items:
                    metrics["resumed_skipped_count"] += 1
                    return

                if not dry_run:
                    if item_id not in shard_item_ids:
                        vm.upsert_item(
                            id=item_id,
                            source_id=source_id,
                            title=title,
                            url=v_url,
                            published_at=str(published_at) if published_at else None,
                            duration_seconds=dur,
                            view_count=view_count,
                            metadata={"video_id": v_id_str, "shard_id": numeric_shard_id},
                        )
                        shard_item_ids.add(item_id)
                        metrics["items_cataloged"] += 1
                else:
                    metrics["items_cataloged"] += 1

            # Subtitle resolution
            vtt_url, p_type, o_type, matched_locale, track_meta = resolve_subtitles(
                vid, language_iso1=ch_lang, country_code=ch_country
            )
            if not vtt_url:
                if "vtt_content" in vid:
                    vtt_url = f"https://mock.local/{v_id_str}.vtt"
                    p_type = "manual_cc"
                    o_type = "human_transcribed"
                    matched_locale = vid.get("locale") or (f"{ch_lang}-{ch_country}" if ch_country else ch_lang)
                    track_meta = {}
                elif channel_video_provider is None and not dry_run:
                    try:
                        import yt_dlp

                        ydl_sub_opts = {
                            "skip_download": True,
                            "quiet": True,
                            "no_warnings": True,
                            "ignore_no_formats_error": True,
                            "extractor_args": {"youtube": {"player_client": ["mweb", "web"]}},
                        }
                        for attempt in range(3):
                            try:
                                with yt_dlp.YoutubeDL(ydl_sub_opts) as ydl:
                                    full_info = ydl.extract_info(v_url, download=False)
                                    if full_info:
                                        vtt_url, p_type, o_type, matched_locale, track_meta = resolve_subtitles(
                                            full_info, language_iso1=ch_lang, country_code=ch_country
                                        )
                                break
                            except Exception:
                                if attempt < 2:
                                    time.sleep(1.0 * (attempt + 1))
                    except Exception:
                        pass

            if not vtt_url:
                return

            # Fetch VTT text in memory
            vtt_text: Optional[str] = None
            if vtt_provider is not None:
                try:
                    vtt_text = vtt_provider(v_id_str, vtt_url or "")
                except Exception as exc:
                    logger.warning("vtt_provider failed for %s: %s", v_id_str, exc)
                    vtt_text = None
            elif "vtt_content" in vid:
                vtt_text = vid["vtt_content"]
            elif vtt_url:
                vtt_text = fetch_vtt_in_memory(vtt_url)

            if vtt_text and not dry_run:
                try:
                    with db_lock:
                        vm.insert_payload(
                            item_id=item_id,
                            payload_type=p_type,
                            language_iso1=ch_lang,
                            locale=matched_locale,
                            origin_type=o_type,
                            format="vtt",
                            raw_content=vtt_text,
                            metadata=track_meta,
                        )
                        metrics["payloads_inserted"] += 1
                        if p_type == "manual_cc":
                            metrics["manual_cc_count"] += 1
                        else:
                            metrics["auto_cc_count"] += 1
                        wc = count_persian_words_in_vtt(vtt_text)
                        metrics["total_persian_words"] += wc
                        shard_payload_items.add(item_id)
                except Exception as exc:
                    logger.warning("Failed to insert payload for %s: %s", item_id, exc)

        if workers > 1 and len(target_videos) > 1:
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                list(executor.map(_process_video, target_videos))
        else:
            for vid in target_videos:
                _process_video(vid)

    vm.close()
    return metrics


# =============================================================================
# 5. CLI Parser & Entrypoint
# =============================================================================

def build_parser() -> ShardHarvestArgumentParser:
    """Construct CLI argument parser for shard harvester."""
    parser = ShardHarvestArgumentParser(
        description="Harvest and catalog YouTube channel shards into isolated SQLite staging DB.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--shard-id",
        type=int,
        default=1,
        help="Shard numeric identifier (1-indexed).",
    )
    parser.add_argument(
        "--channels",
        default="",
        help="Comma-separated handles, or JSON partition file path.",
    )
    parser.add_argument(
        "--catalog",
        default="data/persian_channels_catalog.json",
        help="Path to channels catalog JSON.",
    )
    parser.add_argument(
        "--lang",
        default="fa",
        help="Target ISO-639-1 language code (e.g. fa, fr).",
    )
    parser.add_argument(
        "--country",
        default=None,
        help="Default ISO 3166-1 alpha-2 country code (e.g. FR, CA, BE, CH, SN, IR).",
    )
    parser.add_argument(
        "--cap-per-channel",
        type=int,
        default=25,
        help="Maximum longform videos to harvest per channel.",
    )
    parser.add_argument(
        "--all-videos",
        action="store_true",
        default=False,
        help="Bypass per-channel cap and harvest all eligible videos.",
    )
    parser.add_argument(
        "--min-duration",
        type=int,
        default=600,
        help="Minimum duration in seconds (default: 600s).",
    )
    parser.add_argument(
        "--staging-dir",
        default=None,
        help="Directory for staging SQLite shard database.",
    )
    parser.add_argument(
        "--output-dir",
        default="data/staging",
        help="Alternative/alias output directory for staging DB.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Worker concurrency threads.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Simulate harvesting without writing to disk.",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        default=False,
        help="Perform live flat playlist extraction from YouTube instead of catalog samples.",
    )
    parser.add_argument(
        "--vault-path",
        default="data/vault/language-app-raw-data/raw_vault.db",
        help="Path to canonical vault to pre-populate existing items for skipping.",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    """CLI entrypoint for shard harvester.

    Exit codes:
    - 0: Success
    - 1: Catalog or input file not found
    - 2: Database / execution / runtime error
    - 3: Invalid CLI parameters (e.g. shard_id < 1, cap < 0)
    """
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
        if args.shard_id < 1:
            raise CLIParamError(3, f"--shard-id must be >= 1, got {args.shard_id}")
        if args.cap_per_channel < 0:
            raise CLIParamError(3, f"--cap-per-channel must be >= 0, got {args.cap_per_channel}")
        if args.min_duration < 0:
            raise CLIParamError(3, f"--min-duration must be >= 0, got {args.min_duration}")
        if args.workers < 1:
            raise CLIParamError(3, f"--workers must be >= 1, got {args.workers}")
    except CLIParamError as exc:
        sys.stderr.write(f"Argument Error: {exc.message}\n")
        return exc.code

    staging_dir = args.staging_dir or args.output_dir or "data/staging"

    channels = args.channels
    if not channels:
        cat_p = Path(args.catalog)
        if not cat_p.is_file():
            sys.stderr.write(f"Catalog file not found: {args.catalog}\n")
            return 1
        channels = str(cat_p)
    else:
        if channels.endswith(".json") and not Path(channels).is_file():
            sys.stderr.write(f"Channels file not found: {channels}\n")
            return 1

    try:
        metrics = harvest_channel_shard(
            shard_id=args.shard_id,
            channels=channels,
            staging_dir=staging_dir,
            cap_per_channel=args.cap_per_channel,
            all_videos=args.all_videos,
            min_duration=args.min_duration,
            catalog_path=args.catalog,
            dry_run=args.dry_run,
            workers=args.workers,
            live_extraction=args.live,
            vault_path=args.vault_path,
            language_iso1=args.lang,
            country_code=args.country,
        )
        print(json.dumps(metrics, indent=2))
        return 0
    except FileNotFoundError as exc:
        sys.stderr.write(f"File Not Found: {exc}\n")
        return 1
    except Exception as exc:
        sys.stderr.write(f"Harvest Error: {exc}\n")
        return 2


if __name__ == "__main__":
    sys.exit(main())
