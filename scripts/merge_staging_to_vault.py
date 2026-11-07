#!/usr/bin/env python3
"""Milestone 3: Staging-to-Vault Merger Engine.

Safely, idempotently, and atomically consolidates isolated staging shard databases
(data/staging/shard_*.db) into the canonical Raw Linguistic Vault SQLite database
(raw_vault.db), rebuilding/verifying FTS5 indices, and producing audit ingestion manifests.
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.vault.db import VaultManager

# Configure logger
logger = logging.getLogger("merge_staging_to_vault")
if not logger.handlers:
    _handler = logging.StreamHandler(sys.stderr)
    _handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))
    logger.addHandler(_handler)
logger.setLevel(logging.INFO)


class MergerArgumentParser(argparse.ArgumentParser):
    """Custom ArgumentParser converting parameter validation errors into exit code 3."""

    def error(self, message: str) -> None:
        sys.stderr.write(f"error: {message}\n")
        sys.exit(3)


def count_persian_words_in_vtt(vtt_text: Optional[str]) -> int:
    """Accurately count Persian words in a WebVTT transcript payload using a state machine."""
    try:
        from scripts.harvest_channel_shard import count_persian_words_in_vtt as _harvest_count

        return _harvest_count(vtt_text)
    except (ImportError, ModuleNotFoundError):
        pass

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


def check_shard_health(shard_path: Union[str, Path]) -> Tuple[bool, bool]:
    """Verify shard database integrity and schema health.

    Returns:
        (is_valid: bool, is_corrupt: bool)
        - Healthy: (True, False)
        - Locked / Busy (transient lock): (False, False) -> Do NOT quarantine!
        - Genuinely Corrupt / Invalid: (False, True) -> Quarantine!
    """
    p = Path(shard_path)
    try:
        if not p.is_file():
            return False, True
        st = p.stat()
        if st.st_size < 100:
            return False, True
        with open(p, "rb") as f:
            header = f.read(16)
            if header != b"SQLite format 3\x00":
                return False, True
    except Exception:
        return False, True

    test_conn = None
    try:
        uri_path = p.resolve().as_posix()
        test_conn = sqlite3.connect(f"file:{uri_path}?mode=ro", uri=True, timeout=1.0)
        res = test_conn.execute("PRAGMA quick_check;").fetchone()
        if not res or res[0] != "ok":
            return False, True

        cur = test_conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = {r[0] for r in cur.fetchall()}
        if not {"sources", "items", "payloads"}.issubset(tables):
            return False, True

        return True, False
    except sqlite3.OperationalError as e:
        err_msg = str(e).lower()
        if "locked" in err_msg or "busy" in err_msg:
            # Active lock / busy contention: NOT corruption!
            logger.warning(f"Shard {p} is locked/busy; skipping quarantine.")
            return False, False
        return False, True
    except Exception:
        return False, True
    finally:
        if test_conn is not None:
            try:
                test_conn.close()
            except Exception:
                pass


def quarantine_corrupt_shard(shard_path: Union[str, Path]) -> str:
    """Safely quarantine a genuinely corrupt shard and unlink its WAL/SHM sidecars."""
    p = Path(shard_path)
    quarantine_target = p.with_suffix(".corrupt")
    try:
        p.replace(quarantine_target)
        quarantine_str = str(quarantine_target)
        logger.warning(f"Quarantined corrupt shard: {p} -> {quarantine_target}")
    except Exception as e:
        logger.warning(f"Failed to rename corrupt shard {p}: {e}")
        quarantine_str = str(p)

    # Clean up WAL and SHM sidecars
    sidecars_to_check = [
        p.name + "-wal",
        p.name + "-shm",
        quarantine_target.name + "-wal",
        quarantine_target.name + "-shm",
        p.stem + ".corrupt-wal",
        p.stem + ".corrupt-shm",
    ]
    for sc in sidecars_to_check:
        sidecar_path = p.parent / sc
        if sidecar_path.exists():
            try:
                sidecar_path.unlink()
            except OSError:
                pass

    return quarantine_str


def merge_staging_to_vault(
    staging_dir: Union[str, Path] = "data/staging",
    vault_path: Union[str, Path] = "data/vault/language-app-raw-data/raw_vault.db",
    rebuild_fts: bool = True,
    manifest_out: Optional[Union[str, Path]] = None,
    target_gdrive: bool = False,
    clean_shards: bool = False,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Atomically merge all staging shard databases into canonical raw_vault.db.

    1. Scans staging directory for all shard_*.db files (sorted deterministically).
    2. Runs PRAGMA quick_check; quarantines corrupt shards safely while valid shards proceed.
    3. Merges sources, items, payloads via ATTACH and INSERT OR IGNORE inside transactions.
    4. Rebuilds and synchronizes payloads_fts.
    5. Computes total aggregate Persian word count across payloads.
    6. Produces an ingestion manifest recording provenance and statistics.
    7. Optionally cleans up successfully merged shard database files and sidecars.
    """
    clean_shards = kwargs.get("clean_shards", clean_shards)
    target_gdrive = kwargs.get("target_gdrive", target_gdrive)

    staging_p = Path(staging_dir)
    vault_p = Path(vault_path)

    # Shard Discovery:
    # Scan staging_dir for shard_*.db databases (ignore temporary/sidecar files like -wal, -shm, .corrupt)
    shard_files: List[Path] = []
    if staging_p.is_dir():
        for f in staging_p.glob("shard_*.db"):
            if not f.is_file():
                continue
            name = f.name
            if name.endswith("-wal") or name.endswith("-shm") or ".corrupt" in name:
                continue
            shard_files.append(f)
        shard_files.sort(key=lambda p: str(p))

    # Empty Staging Directory:
    if not shard_files:
        logger.info(f"No staging shards discovered in {staging_p}; exiting gracefully.")
        # If vault exists, retrieve baseline counts; otherwise 0
        total_sources = total_items = total_payloads = total_words = 0
        if vault_p.is_file():
            try:
                vm_check = VaultManager(db_path=vault_p)
                vm_check.init_db()
                conn_check = vm_check.conn
                total_sources = conn_check.execute("SELECT COUNT(*) FROM sources;").fetchone()[0]
                total_items = conn_check.execute("SELECT COUNT(*) FROM items;").fetchone()[0]
                total_payloads = conn_check.execute("SELECT COUNT(*) FROM payloads;").fetchone()[0]
                cur = conn_check.execute("SELECT raw_content FROM payloads;")
                for row in cur.fetchall():
                    total_words += count_persian_words_in_vtt(row[0])
                vm_check.close()
            except Exception:
                pass

        manifest = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": "NO_SHARDS",
            "vault_path": str(vault_p),
            "shards_processed": [],
            "quarantined_shards": [],
            "sources_added": 0,
            "items_added": 0,
            "payloads_added": 0,
            "total_sources": total_sources,
            "total_items": total_items,
            "total_payloads": total_payloads,
            "total_persian_words": total_words,
            "target_gdrive": target_gdrive,
        }

        if manifest_out:
            out_file = Path(manifest_out)
        else:
            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            staging_p.mkdir(parents=True, exist_ok=True)
            out_file = staging_p / f"ingestion_manifest_{ts}.json"

        out_file.parent.mkdir(parents=True, exist_ok=True)
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)
        manifest["manifest_path"] = str(out_file)
        return manifest

    # Shard Health Validation:
    valid_shards: List[Path] = []
    quarantined_shards: List[str] = []

    for shard in shard_files:
        is_valid, is_corrupt = check_shard_health(shard)
        if is_valid:
            valid_shards.append(shard)
        elif is_corrupt:
            q_target = quarantine_corrupt_shard(shard)
            quarantined_shards.append(q_target)
        else:
            # Active database lock / busy: do NOT quarantine
            logger.warning(f"Shard {shard} is locked or busy; skipping without quarantine.")

    vault_p.parent.mkdir(parents=True, exist_ok=True)
    vm = VaultManager(db_path=vault_p)
    vm.init_db()

    conn = vm.conn
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA busy_timeout = 5000;")

    initial_sources = conn.execute("SELECT COUNT(*) FROM sources;").fetchone()[0]
    initial_items = conn.execute("SELECT COUNT(*) FROM items;").fetchone()[0]
    initial_payloads = conn.execute("SELECT COUNT(*) FROM payloads;").fetchone()[0]

    successfully_merged_shards: List[Path] = []

    for shard_file in valid_shards:
        shard_path_str = str(shard_file.resolve())
        attached = False
        try:
            conn.execute("ATTACH DATABASE ? AS shard;", (shard_path_str,))
            attached = True
            with vm.transaction():
                # Step 1: Sources first (FK root)
                conn.execute("INSERT OR IGNORE INTO sources SELECT * FROM shard.sources;")
                # Step 2: Items second (FK to sources)
                conn.execute("INSERT OR IGNORE INTO items SELECT * FROM shard.items;")
                # Step 3: Payloads third (FK to items)
                conn.execute("INSERT OR IGNORE INTO payloads SELECT * FROM shard.payloads;")
            successfully_merged_shards.append(shard_file)
        except sqlite3.OperationalError as e:
            err_msg = str(e).lower()
            if "locked" in err_msg or "busy" in err_msg:
                logger.warning(f"Lock contention on shard {shard_file}: {e}")
                continue
            logger.warning(f"Operational error merging shard {shard_file}: {e}")
            q_target = quarantine_corrupt_shard(shard_file)
            quarantined_shards.append(q_target)
        except Exception as e:
            logger.warning(f"Unexpected error merging shard {shard_file}: {e}")
            q_target = quarantine_corrupt_shard(shard_file)
            quarantined_shards.append(q_target)
        finally:
            if attached:
                try:
                    conn.execute("DETACH DATABASE shard;")
                except Exception:
                    pass

    # FTS5 Index Maintenance:
    if rebuild_fts:
        with vm.transaction():
            conn.execute("INSERT INTO payloads_fts(payloads_fts) VALUES ('rebuild');")

    final_sources = conn.execute("SELECT COUNT(*) FROM sources;").fetchone()[0]
    final_items = conn.execute("SELECT COUNT(*) FROM items;").fetchone()[0]
    final_payloads = conn.execute("SELECT COUNT(*) FROM payloads;").fetchone()[0]

    # Aggregate Persian word count across all payloads in vault
    total_words = 0
    cur = conn.execute("SELECT raw_content FROM payloads;")
    for row in cur.fetchall():
        total_words += count_persian_words_in_vtt(row[0])

    manifest = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "SUCCESS",
        "vault_path": str(vault_p),
        "shards_processed": [str(s) for s in successfully_merged_shards],
        "quarantined_shards": quarantined_shards,
        "sources_added": final_sources - initial_sources,
        "items_added": final_items - initial_items,
        "payloads_added": final_payloads - initial_payloads,
        "total_sources": final_sources,
        "total_items": final_items,
        "total_payloads": final_payloads,
        "total_persian_words": total_words,
        "target_gdrive": target_gdrive,
    }

    if manifest_out:
        out_file = Path(manifest_out)
    else:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        staging_p.mkdir(parents=True, exist_ok=True)
        out_file = staging_p / f"ingestion_manifest_{ts}.json"

    out_file.parent.mkdir(parents=True, exist_ok=True)
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    manifest["manifest_path"] = str(out_file)

    # Shard Cleanup:
    if clean_shards:
        for shard_file in successfully_merged_shards:
            for ext in ("", "-wal", "-shm"):
                sidecar = shard_file.parent / (shard_file.name + ext)
                if sidecar.exists():
                    try:
                        sidecar.unlink()
                    except OSError as e:
                        logger.warning(f"Failed to delete {sidecar}: {e}")

    vm.close()
    return manifest


def main(argv: Optional[List[str]] = None) -> int:
    """CLI Entry point with custom error codes."""
    parser = MergerArgumentParser(
        description="Consolidate staging shard SQLite databases into canonical raw_vault.db."
    )
    parser.add_argument(
        "--staging-dir",
        type=str,
        default="data/staging",
        help="Path to staging directory containing shard_*.db databases (default: data/staging)",
    )
    parser.add_argument(
        "--vault-path",
        type=str,
        default="data/vault/language-app-raw-data/raw_vault.db",
        help="Path to destination canonical raw_vault.db (default: data/vault/language-app-raw-data/raw_vault.db)",
    )
    parser.add_argument(
        "--rebuild-fts",
        dest="rebuild_fts",
        action="store_true",
        default=True,
        help="Rebuild FTS5 index after merging (default: True)",
    )
    parser.add_argument(
        "--no-rebuild-fts",
        dest="rebuild_fts",
        action="store_false",
        help="Skip rebuilding FTS5 index",
    )
    parser.add_argument(
        "--manifest-out",
        type=str,
        default=None,
        help="Custom output file path for the ingestion manifest JSON",
    )
    parser.add_argument(
        "--clean-shards",
        action="store_true",
        default=False,
        help="Delete successfully merged shard database files and sidecars",
    )
    parser.add_argument(
        "--target-gdrive",
        action="store_true",
        default=False,
        help="Record target cloud storage destination in manifest",
    )

    try:
        args = parser.parse_args(argv)
        manifest = merge_staging_to_vault(
            staging_dir=args.staging_dir,
            vault_path=args.vault_path,
            rebuild_fts=args.rebuild_fts,
            manifest_out=args.manifest_out,
            target_gdrive=args.target_gdrive,
            clean_shards=args.clean_shards,
        )
        print(json.dumps(manifest, indent=2, ensure_ascii=False))
        return 0
    except SystemExit:
        raise
    except Exception as e:
        logger.error(f"Fatal error during merge execution: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
