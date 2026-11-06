#!/usr/bin/env python3
"""Ingestion pipeline: Ingest Tabaghe 16 (طبقه 16) into the Cloud Raw Linguistic Vault.

Features:
- Discovers long-form videos (duration >= 600s) from @Tabaghe16 on YouTube.
- Filters out Shorts and short promos.
- Extracts strictly manual Persian Closed Captions (fa, fa-IR) in .vtt format.
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


def extract_persian_vtt(video_id: str) -> Optional[Tuple[str, str, Dict[str, Any]]]:
    """Inspect video for manual Persian subtitles and stream .vtt in-memory.
    
    Returns (vtt_text, vtt_url, item_info) or None if no manual Persian CC.
    """
    import yt_dlp

    ydl_opts = {
        "skip_download": True,
        "quiet": True,
        "no_warnings": True,
    }
    video_url = f"https://www.youtube.com/watch?v={video_id}"
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(video_url, download=False)
    except Exception as e:
        return None

    # Inspect manual subtitles strictly (ignore automatic_captions)
    subtitles = info.get("subtitles", {})
    fa_tracks = subtitles.get("fa") or subtitles.get("fa-IR") or []
    if not fa_tracks:
        return None

    # Find VTT format track URL
    vtt_url = None
    for track in fa_tracks:
        if track.get("ext") == "vtt":
            vtt_url = track.get("url")
            break
    if not vtt_url and fa_tracks:
        # Fallback to requesting VTT format directly from timedtext API
        vtt_url = fa_tracks[0].get("url")
        if vtt_url and "fmt=" not in vtt_url:
            vtt_url += "&fmt=vtt"

    if not vtt_url:
        return None

    # Stream VTT content in-memory via HTTP
    try:
        req = urllib.request.Request(
            vtt_url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            vtt_text = resp.read().decode("utf-8")
        return vtt_text, vtt_url, info
    except Exception:
        return None


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
    # Find gdrive binary
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
    limit: Optional[int] = None,
    workers: int = 8,
    dry_run: bool = False,
    sync_gdrive: bool = False,
) -> Dict[str, Any]:
    """Execute Tabaghe 16 ingestion pipeline."""
    target_path = resolve_vault_path(db_path=db_path, vault_dir=vault_dir)
    print("=" * 65)
    print("       INGESTING TABAGHE 16 INTO RAW LINGUISTIC VAULT")
    print("=" * 65)
    print(f"Vault Destination : {target_path}")
    print(f"Min Duration Filter: {min_duration} seconds (>= 10 minutes)")
    print(f"Language Track     : Strictly Persian ('fa', 'fa-IR')")
    print(f"Media Download     : 0 MB (In-Memory Subtitle Streaming)")
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

    # 2. Existing items in DB to avoid duplicate network fetches
    existing_items = {
        row["id"]
        for row in vm.conn.execute("SELECT id FROM items WHERE source_id = ?;", (CHANNEL_ID,)).fetchall()
    }
    print(f"[*] Found {len(existing_items)} existing items already in vault.")

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
    print(f"[*] Scanning subtitle tracks across {len(candidates)} videos with {workers} workers...")
    t0 = time.time()

    processed_count = 0
    already_ingested_count = 0
    no_persian_cc_count = 0
    newly_ingested_count = 0
    total_new_words = 0
    total_new_duration = 0

    to_fetch: List[Dict[str, Any]] = []
    for entry in candidates:
        vid_id = entry.get("id")
        if not vid_id:
            continue
        item_id = f"yt:{vid_id}"
        if item_id in existing_items:
            already_ingested_count += 1
            processed_count += 1
        else:
            to_fetch.append(entry)

    def worker_task(entry: Dict[str, Any]):
        vid = entry["id"]
        res = extract_persian_vtt(vid)
        return entry, res

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(worker_task, entry): entry for entry in to_fetch}
        for future in as_completed(futures):
            entry, result = future.result()
            processed_count += 1
            vid = entry["id"]
            title = entry.get("title", f"Tabaghe 16 - {vid}")
            item_id = f"yt:{vid}"
            duration = entry.get("duration") or 0
            view_count = entry.get("view_count")

            if result is None:
                no_persian_cc_count += 1
                continue

            vtt_text, vtt_url, item_info = result
            words = count_persian_words(vtt_text)
            published_at = item_info.get("upload_date")
            if published_at and len(published_at) == 8:
                published_at = f"{published_at[:4]}-{published_at[4:6]}-{published_at[6:]}"

            item_metadata = {
                "video_id": vid,
                "channel": "@Tabaghe16",
                "tags": item_info.get("tags", []),
                "subtitle_url": vtt_url,
            }

            if not dry_run:
                with vm.transaction():
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
                    vm.insert_payload(
                        item_id=item_id,
                        payload_type="manual_cc",
                        language_iso1="fa",
                        origin_type="human_transcribed",
                        format="vtt",
                        raw_content=vtt_text,
                    )

            newly_ingested_count += 1
            total_new_words += words
            total_new_duration += duration
            print(f"[{processed_count}/{len(candidates)}] Ingested: {vid} | {title[:45]}... ({words:,} words)")

    elapsed = time.time() - t0
    stats = vm.get_vault_stats()
    vm.close()

    print("\n" + "=" * 65)
    print("                 INGESTION SUMMARY REPORT")
    print("=" * 65)
    print(f"Total Videos Scanned          : {len(candidates)}")
    print(f"Already Ingested (Skipped)    : {already_ingested_count}")
    print(f"No Manual Persian CC (Skipped): {no_persian_cc_count}")
    print(f"Newly Ingested Episodes       : {newly_ingested_count}")
    print(f"Total Conversational Words    : {total_new_words:,}")
    print(f"Duration Added (Hours)        : {total_new_duration / 3600.0:.2f} hrs")
    print(f"Elapsed Processing Time       : {elapsed:.2f} seconds")
    print("-" * 65)
    print(f"Total Items in Vault          : {stats['total_items']:,}")
    print(f"Total Payloads in Vault       : {stats['total_payloads']:,}")
    print(f"Total Vault Speech Duration   : {stats['total_duration_hours']:.2f} hours")
    print("=" * 65)

    if sync_gdrive and not dry_run:
        upload_to_gdrive(target_path)

    return {
        "vault_path": str(target_path),
        "total_scanned": len(candidates),
        "already_ingested": already_ingested_count,
        "newly_ingested": newly_ingested_count,
        "no_persian_cc": no_persian_cc_count,
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
        "--limit",
        type=int,
        default=None,
        help="Max videos to process (useful for testing or batching)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Number of concurrent worker threads (default: 8)",
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
