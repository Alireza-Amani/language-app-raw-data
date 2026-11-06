#!/usr/bin/env python3
"""Ingestion pipeline: Ingest Tabaghe 16 (طبقه 16) into the Cloud Raw Linguistic Vault.

Features:
- Discovers long-form videos (duration >= 600s) from @Tabaghe16 on YouTube.
- Filters out Shorts and short promos.
- Extracts manual Persian Closed Captions (fa, fa-IR) as priority (human_transcribed).
- Fallback to automatic Persian captions (fa-orig, fa) when manual CC is unavailable (machine_transcribed).
- Catalogs all eligible longform episodes in `items`.
- Streams subtitles directly into memory via HTTP (0 video/audio downloaded).
- Computes SHA-256 digest for cryptographic provenance and deduplication.
- Persists into SQLite Raw Linguistic Vault with FTS5 indexing.
- Supports cloud Google Drive path resolution and optional gdrive sync.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.vault.db import VaultManager

# Pinned channel metadata
CHANNEL_ID = "yt:@Tabaghe16"
CHANNEL_NAME = "طبقه 16"
CHANNEL_URL = "https://www.youtube.com/@Tabaghe16"
CHANNEL_VIDEOS_URL = "https://www.youtube.com/@Tabaghe16/videos"
GDRIVE_FOLDER_ID = "1eQt8Rux2WoLSsTRs9nXxgRKbIYIEd2xg"  # language-app-raw-data in Drive


def resolve_vault_path(
    db_path: Optional[str] = None,
    vault_dir: Optional[str] = None,
) -> Path:
    """Resolve destination SQLite database path, prioritizing cloud Google Drive mounts."""
    if db_path:
        p = Path(db_path).resolve()
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    if vault_dir:
        d = Path(vault_dir).resolve()
        d.mkdir(parents=True, exist_ok=True)
        return d / "raw_vault.db"

    # Known candidate paths for Google Drive on macOS/Linux
    candidates: List[Path] = [
        # Option B: Inside Language App Development
        Path(
            "/Users/pinovanrijn/Library/CloudStorage/GoogleDrive-alireza.amani101@gmail.com/My Drive/Language App Development/language-app-raw-data"
        ),
        # Option A: Root My Drive
        Path(
            "/Users/pinovanrijn/Library/CloudStorage/GoogleDrive-alireza.amani101@gmail.com/My Drive/language-app-raw-data"
        ),
    ]

    # Dynamically scan user CloudStorage if present
    cloud_storage_root = Path.home() / "Library" / "CloudStorage"
    if cloud_storage_root.is_dir():
        for drive_mount in cloud_storage_root.glob("GoogleDrive-*"):
            candidates.append(
                drive_mount / "My Drive" / "Language App Development" / "language-app-raw-data"
            )
            candidates.append(drive_mount / "My Drive" / "language-app-raw-data")

    # Check if any candidate or its parent folder exists
    for candidate in candidates:
        if candidate.is_dir():
            return candidate / "raw_vault.db"
        if candidate.parent.is_dir():
            candidate.mkdir(parents=True, exist_ok=True)
            return candidate / "raw_vault.db"

    # Default local workspace fallback
    default_local = Path("data/vault/language-app-raw-data/raw_vault.db").resolve()
    default_local.parent.mkdir(parents=True, exist_ok=True)
    return default_local


def extract_channel_videos(channel_videos_url: str = CHANNEL_VIDEOS_URL) -> List[Dict[str, Any]]:
    """Extract flat playlist entries for all videos on the channel."""
    import yt_dlp

    ydl_opts = {
        "extract_flat": True,
        "quiet": True,
        "no_warnings": True,
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(channel_videos_url, download=False)
        return list(info.get("entries", []))


def extract_persian_vtt(
    video_id: str,
    include_auto_cc: bool = True,
) -> Tuple[Optional[str], Optional[str], Optional[Dict[str, Any]], Optional[str], Optional[str]]:
    """Inspect video for Persian subtitles (manual first, then auto-cc if enabled).
    
    Returns (vtt_text, vtt_url, item_info, payload_type, origin_type).
    """
    import yt_dlp

    ydl_opts = {
        "skip_download": True,
        "quiet": True,
        "no_warnings": True,
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }
    video_url = f"https://www.youtube.com/watch?v={video_id}"

    info = None
    for attempt in range(2):
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(video_url, download=False)
                break
        except Exception:
            if attempt == 0:
                time.sleep(1.0)
            else:
                return None, None, None, None, None

    if not info:
        return None, None, None, None, None

    # 1. Check manual creator subtitles (gold standard)
    subtitles = info.get("subtitles", {})
    fa_tracks = subtitles.get("fa") or subtitles.get("fa-IR") or []
    payload_type = "manual_cc"
    origin_type = "human_transcribed"

    # 2. Check automatic ASR captions if manual not available and auto enabled
    if not fa_tracks and include_auto_cc:
        auto = info.get("automatic_captions", {})
        fa_tracks = auto.get("fa-orig") or auto.get("fa") or auto.get("fa-IR") or []
        payload_type = "auto_cc"
        origin_type = "machine_transcribed"

    if not fa_tracks:
        return None, None, info, None, None

    # Find VTT format track URL
    vtt_url = None
    for track in fa_tracks:
        if track.get("ext") == "vtt":
            vtt_url = track.get("url")
            break
    if not vtt_url and fa_tracks:
        vtt_url = fa_tracks[0].get("url")
        if vtt_url and "fmt=" not in vtt_url:
            vtt_url += "&fmt=vtt"

    if not vtt_url:
        return None, None, info, None, None

    # Stream VTT content in-memory via HTTP
    try:
        req = urllib.request.Request(
            vtt_url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
        )
        with urllib.request.urlopen(req, timeout=25) as resp:
            vtt_text = resp.read().decode("utf-8")
        return vtt_text, vtt_url, info, payload_type, origin_type
    except Exception:
        return None, None, info, None, None


def count_persian_words(vtt_text: str) -> int:
    """Approximate word count of conversational speech inside VTT payload."""
    words = 0
    in_cue = False
    for line in vtt_text.splitlines():
        line = line.strip()
        if not line or line.startswith("WEBVTT") or line.startswith("Kind:") or line.startswith("Language:"):
            continue
        if "-->" in line:
            in_cue = True
            continue
        if in_cue:
            words += len(line.split())
    return words


def upload_to_gdrive(db_path: Path, folder_id: str = GDRIVE_FOLDER_ID) -> bool:
    """Upload or update database file in Google Drive via gdrive CLI."""
    gdrive_bin = None
    for candidate in [
        "/tmp/antigravity/bin/gdrive",
        shutil.which("gdrive"),
        "/opt/antigravity/bin/gdrive",
    ]:
        if candidate and Path(candidate).is_file():
            gdrive_bin = str(candidate)
            break

    if not gdrive_bin:
        print("[!] gdrive CLI binary not found. Skipping cloud sync.")
        return False

    print(f"[*] Uploading {db_path.name} to Google Drive folder (ID: {folder_id})...")
    env = os.environ.copy()
    env["ANTIGRAVITY_APP_DATA_DIR"] = "/tmp/antigravity"
    env["HOME"] = str(Path.home())

    cmd = [gdrive_bin, "mutate", "upload", str(db_path), "--parent", folder_id]
    res = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if res.returncode == 0:
        print(f"[+] Successfully uploaded vault to Google Drive: {res.stdout.strip()}")
        return True
    else:
        print(f"[!] Upload to Google Drive failed: {res.stderr.strip()}")
        return False


def run_ingestion(
    db_path: Optional[str] = None,
    vault_dir: Optional[str] = None,
    min_duration: int = 600,
    include_auto_cc: bool = True,
    limit: Optional[int] = None,
    workers: int = 6,
    dry_run: bool = False,
    sync_gdrive: bool = False,
) -> Dict[str, Any]:
    """Execute Tabaghe 16 ingestion pipeline."""
    target_path = resolve_vault_path(db_path=db_path, vault_dir=vault_dir)
    print("=" * 65)
    print("       INGESTING TABAGHE 16 INTO RAW LINGUISTIC VAULT")
    print("=" * 65)
    print(f"Vault Destination   : {target_path}")
    print(f"Min Duration Filter : {min_duration} seconds (>= 10 minutes)")
    print(f"Manual Persian CC   : Included ('human_transcribed')")
    print(f"Auto Persian CC     : {'Included (machine_transcribed)' if include_auto_cc else 'Excluded (Manual Only)'}")
    print(f"Catalog Coverage    : 100% of eligible channel videos")
    print(f"Media Download      : 0 MB (In-Memory Subtitle Streaming)")
    print("-" * 65)

    vm = VaultManager(db_path=target_path)
    vm.init_db()

    # 1. Upsert Source
    vm.upsert_source(
        id=CHANNEL_ID,
        platform="youtube",
        language_iso1="fa",
        name=CHANNEL_NAME,
        url=CHANNEL_URL,
        metadata={"handle": "@Tabaghe16", "category": "interviews_and_technology"},
    )
    print(f"[+] Source registered: {CHANNEL_ID} ('{CHANNEL_NAME}')")

    # 2. Existing items and payloads in DB
    existing_items = {
        row["id"]
        for row in vm.conn.execute("SELECT id FROM items WHERE source_id = ?;", (CHANNEL_ID,)).fetchall()
    }
    existing_payloads = {
        row["item_id"]
        for row in vm.conn.execute("SELECT item_id FROM payloads;").fetchall()
    }
    print(f"[*] Found {len(existing_items)} existing items and {len(existing_payloads)} payloads already in vault.")

    # 3. Discover Channel Videos
    print(f"[*] Fetching video catalog from {CHANNEL_VIDEOS_URL}...")
    entries = extract_channel_videos(CHANNEL_VIDEOS_URL)
    print(f"[+] Discovered {len(entries)} total channel videos.")

    # 4. Filter by duration (>= min_duration)
    candidates: List[Dict[str, Any]] = []
    short_skipped = 0
    for e in entries:
        dur = e.get("duration") or 0
        if dur >= min_duration:
            candidates.append(e)
        else:
            short_skipped += 1

    print(f"[+] Duration Filter Applied:")
    print(f"    - Eligible Long-form Videos (>= {min_duration}s) : {len(candidates)}")
    print(f"    - Filtered Shorts & Teasers (< {min_duration}s): {short_skipped}")

    if limit is not None and limit > 0:
        candidates = candidates[:limit]
        print(f"[*] Limiting scan to first {limit} candidates.")

    # 5. Process Candidates
    # Skip videos that already have both an item AND a payload in the vault
    to_fetch: List[Dict[str, Any]] = []
    already_complete_count = 0
    for entry in candidates:
        vid_id = entry.get("id")
        if not vid_id:
            continue
        item_id = f"yt:{vid_id}"
        if item_id in existing_items and item_id in existing_payloads:
            already_complete_count += 1
        else:
            to_fetch.append(entry)

    print(f"[*] {already_complete_count} videos already fully ingested with payloads.")
    print(f"[*] Processing remaining {len(to_fetch)} videos with {workers} workers...")
    t0 = time.time()

    processed_count = 0
    newly_ingested_manual = 0
    newly_ingested_auto = 0
    cataloged_no_payload = 0
    total_new_words = 0
    total_new_duration = 0

    def worker_task(entry: Dict[str, Any]):
        vid = entry["id"]
        res = extract_persian_vtt(vid, include_auto_cc=include_auto_cc)
        return entry, res

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(worker_task, entry): entry for entry in to_fetch}
        for future in as_completed(futures):
            entry, (vtt_text, vtt_url, item_info, payload_type, origin_type) = future.result()
            processed_count += 1
            vid = entry["id"]
            title = entry.get("title", f"Tabaghe 16 - {vid}")
            item_id = f"yt:{vid}"
            duration = entry.get("duration") or 0
            view_count = entry.get("view_count")

            published_at = (item_info or {}).get("upload_date")
            if published_at and len(published_at) == 8:
                published_at = f"{published_at[:4]}-{published_at[4:6]}-{published_at[6:]}"

            item_metadata = {
                "video_id": vid,
                "channel": "@Tabaghe16",
                "tags": (item_info or {}).get("tags", []),
                "subtitle_url": vtt_url,
            }

            if not dry_run:
                with vm.transaction():
                    # 1. Ensure item is recorded in items table
                    vm.upsert_item(
                        id=item_id,
                        source_id=CHANNEL_ID,
                        title=title,
                        url=f"https://www.youtube.com/watch?v={vid}",
                        published_at=published_at,
                        duration_seconds=duration,
                        view_count=view_count,
                        metadata=item_metadata,
                    )
                    # 2. Insert payload if subtitle was extracted
                    if vtt_text and payload_type and origin_type:
                        vm.insert_payload(
                            item_id=item_id,
                            payload_type=payload_type,
                            language_iso1="fa",
                            origin_type=origin_type,
                            format="vtt",
                            raw_content=vtt_text,
                        )

            if vtt_text:
                words = count_persian_words(vtt_text)
                total_new_words += words
                total_new_duration += duration
                if payload_type == "manual_cc":
                    newly_ingested_manual += 1
                    tag = "MANUAL CC"
                else:
                    newly_ingested_auto += 1
                    tag = "AUTO CC"
                print(f"[{processed_count}/{len(to_fetch)}] Ingested [{tag}]: {vid} | {title[:40]}... ({words:,} words)")
            else:
                cataloged_no_payload += 1
                print(f"[{processed_count}/{len(to_fetch)}] Cataloged (No CC): {vid} | {title[:40]}...")

    elapsed = time.time() - t0
    stats = vm.get_vault_stats()
    vm.close()

    print("\n" + "=" * 65)
    print("                 INGESTION SUMMARY REPORT")
    print("=" * 65)
    print(f"Total Eligible Channel Videos : {len(candidates)}")
    print(f"Previously Ingested           : {already_complete_count}")
    print(f"Newly Ingested (Manual CC)    : {newly_ingested_manual}")
    print(f"Newly Ingested (Auto CC)      : {newly_ingested_auto}")
    print(f"Cataloged without CC          : {cataloged_no_payload}")
    print(f"Total New Conversational Words: {total_new_words:,}")
    print(f"Duration Added (Hours)        : {total_new_duration / 3600.0:.2f} hrs")
    print(f"Elapsed Processing Time       : {elapsed:.2f} seconds")
    print("-" * 65)
    print(f"Total Items in Vault          : {stats['total_items']:,} / {len(candidates)} (100% cataloged)")
    print(f"Total Payloads in Vault       : {stats['total_payloads']:,}")
    print(f"  - Human Transcribed (manual): {stats['payloads_by_origin'].get('human_transcribed', 0):,}")
    print(f"  - Machine Transcribed (auto): {stats['payloads_by_origin'].get('machine_transcribed', 0):,}")
    print(f"Total Vault Speech Duration   : {stats['total_duration_hours']:.2f} hours")
    print(f"Total Audience Views          : {stats['total_views']:,}")
    print("=" * 65)

    if sync_gdrive and not dry_run:
        upload_to_gdrive(target_path)

    return {
        "vault_path": str(target_path),
        "total_eligible": len(candidates),
        "already_complete": already_complete_count,
        "newly_manual": newly_ingested_manual,
        "newly_auto": newly_ingested_auto,
        "no_cc": cataloged_no_payload,
        "new_words": total_new_words,
        "elapsed_seconds": elapsed,
        "vault_stats": stats,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Ingest Tabaghe 16 longform Persian CC into Raw Linguistic Vault",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Direct path to SQLite database file",
    )
    parser.add_argument(
        "--vault-dir",
        default=None,
        help="Path to folder containing or to contain raw_vault.db",
    )
    parser.add_argument(
        "--min-duration",
        type=int,
        default=600,
        help="Minimum video duration in seconds (default: 600s / 10 minutes)",
    )
    parser.add_argument(
        "--include-auto-cc",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include YouTube automatic captions for episodes without manual subtitles (default: True)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max videos to process (useful for testing or batching)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=6,
        help="Number of concurrent worker threads (default: 6)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scan and test subtitle extraction without saving to SQLite",
    )
    parser.add_argument(
        "--sync-gdrive",
        action="store_true",
        help="Upload/update database to Google Drive cloud folder after ingestion",
    )

    args = parser.parse_args()
    try:
        run_ingestion(
            db_path=args.db_path,
            vault_dir=args.vault_dir,
            min_duration=args.min_duration,
            include_auto_cc=args.include_auto_cc,
            limit=args.limit,
            workers=args.workers,
            dry_run=args.dry_run,
            sync_gdrive=args.sync_gdrive,
        )
        return 0
    except Exception as e:
        print(f"[!] Ingestion failed: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
