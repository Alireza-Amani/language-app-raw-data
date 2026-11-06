#!/usr/bin/env python3
"""CLI utility for managing and querying the Raw Linguistic Vault."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.vault.db import VaultManager


def cmd_init(args: argparse.Namespace) -> int:
    """Initialize the vault database and create tables."""
    db_path = Path(args.db_path)
    print(f"[*] Initializing vault database at: {db_path}")
    try:
        vm = VaultManager(db_path=db_path)
        schema_file = Path(args.schema_file) if getattr(args, "schema_file", None) else None
        vm.init_db(schema_file=schema_file)
        
        # Verify WAL mode and foreign keys
        cur = vm.conn.cursor()
        wal_mode = cur.execute("PRAGMA journal_mode;").fetchone()[0]
        fk_enabled = cur.execute("PRAGMA foreign_keys;").fetchone()[0]
        vm.close()

        print(f"[+] Vault initialized successfully.")
        print(f"    - Journal Mode: {wal_mode.upper()} (expected: WAL)")
        print(f"    - Foreign Keys: {'ON' if fk_enabled else 'OFF'} (expected: ON)")
        return 0
    except Exception as e:
        print(f"[!] Initialization error: {e}", file=sys.stderr)
        return 1


def cmd_stats(args: argparse.Namespace) -> int:
    """Display comprehensive statistics for the vault."""
    db_path = Path(args.db_path)
    if not db_path.exists():
        print(f"[!] Database file not found at {db_path}. Run 'init' first.", file=sys.stderr)
        return 1

    try:
        with VaultManager(db_path=db_path) as vm:
            stats = vm.get_vault_stats()

        if getattr(args, "json", False):
            print(json.dumps(stats, indent=2))
            return 0

        print("=" * 60)
        print("          RAW LINGUISTIC VAULT STATISTICS")
        print("=" * 60)
        print(f"Database Path       : {db_path}")
        print(f"Total Sources       : {stats['total_sources']:,}")
        print(f"Total Items         : {stats['total_items']:,}")
        print(f"Total Payloads      : {stats['total_payloads']:,}")
        print(f"Total Duration      : {stats['total_duration_hours']:.2f} hours")
        print(f"Total Views         : {stats['total_views']:,}")
        print("-" * 60)

        print("\n--- Sources by Platform ---")
        if stats["sources_by_platform"]:
            for platform, count in stats["sources_by_platform"].items():
                print(f"  {platform:<20}: {count:>8,}")
        else:
            print("  (none)")

        print("\n--- Sources by Language ---")
        if stats["sources_by_language"]:
            for lang, count in stats["sources_by_language"].items():
                print(f"  {lang:<20}: {count:>8,}")
        else:
            print("  (none)")

        print("\n--- Payloads by Type ---")
        if stats["payloads_by_type"]:
            for p_type, count in stats["payloads_by_type"].items():
                print(f"  {p_type:<20}: {count:>8,}")
        else:
            print("  (none)")

        print("\n--- Payloads by Origin ---")
        if stats["payloads_by_origin"]:
            for origin, count in stats["payloads_by_origin"].items():
                print(f"  {origin:<20}: {count:>8,}")
        else:
            print("  (none)")

        print("\n--- Payloads by Language ---")
        if stats["payloads_by_language"]:
            for lang, count in stats["payloads_by_language"].items():
                print(f"  {lang:<20}: {count:>8,}")
        else:
            print("  (none)")
        print("=" * 60)
        return 0
    except Exception as e:
        print(f"[!] Error retrieving stats: {e}", file=sys.stderr)
        return 1


def cmd_search(args: argparse.Namespace) -> int:
    """Execute FTS5 search query across stored payloads."""
    db_path = Path(args.db_path)
    if not db_path.exists():
        print(f"[!] Database file not found at {db_path}. Run 'init' first.", file=sys.stderr)
        return 1

    try:
        with VaultManager(db_path=db_path) as vm:
            results = vm.search_text(
                query=args.query,
                language_iso1=getattr(args, "lang", None),
                limit=getattr(args, "limit", 20),
            )

        if getattr(args, "json", False):
            print(json.dumps(results, indent=2))
            return 0

        print(f"\n[*] Search results for '{args.query}' (Found: {len(results)} matches):\n")
        if not results:
            print("  No matching transcripts found.")
            return 0

        for idx, match in enumerate(results, start=1):
            print(f"[{idx}] Item ID : {match['item_id']}")
            print(f"    Title   : {match.get('item_title') or 'N/A'}")
            print(f"    URL     : {match.get('item_url') or 'N/A'}")
            print(f"    Payload : {match['payload_id'][:12]}... (format: {match['format']}, lang: {match['language_iso1']}, origin: {match['origin_type']})")
            snippet = match.get("snippet", "").replace("\n", " ")
            print(f"    Snippet : {snippet}")
            print("-" * 60)
        return 0
    except Exception as e:
        print(f"[!] Search failed: {e}", file=sys.stderr)
        return 1


def cmd_inspect(args: argparse.Namespace) -> int:
    """Inspect a specific item, its URL, and its payloads."""
    db_path = Path(args.db_path)
    if not db_path.exists():
        print(f"[!] Database file not found at {db_path}.", file=sys.stderr)
        return 1

    with VaultManager(db_path=db_path) as vm:
        item = vm.get_item(args.item_id)
        if not item:
            # Try prepending 'yt:' if user typed bare id
            item = vm.get_item(f"yt:{args.item_id}")
        if not item:
            print(f"[!] Item '{args.item_id}' not found in vault.", file=sys.stderr)
            return 1

        payloads = vm.get_payloads_by_item(item["id"])
        source = vm.get_source(item["source_id"])

    print("=" * 60)
    print(f"ITEM DETAILS: {item['id']}")
    print("=" * 60)
    print(f"Title         : {item['title']}")
    print(f"Direct URL    : {item['url']}")
    print(f"Source        : {item['source_id']} ({source['name'] if source else 'N/A'})")
    print(f"Published At  : {item['published_at']}")
    print(f"Duration      : {item['duration_seconds']} seconds ({item['duration_seconds'] / 60.0:.1f} mins)")
    print(f"View Count    : {item['view_count']:,}" if item['view_count'] else "View Count    : N/A")
    print(f"Payloads Found: {len(payloads)}")
    print("-" * 60)
    for p in payloads:
        print(f"  * Payload ID   : {p['id'][:16]}...")
        print(f"    Type/Origin  : {p['payload_type']} | {p['origin_type']} | format: {p['format']}")
        print(f"    Sample Cues  :\n{p['raw_content'][:200]}...")
    print("=" * 60)
    return 0


def main() -> int:
    # Common parent parser so --db-path can be passed before or after subcommand
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument(
        "--db-path",
        default=argparse.SUPPRESS,
        help="Path to SQLite database file",
    )

    parser = argparse.ArgumentParser(
        description="Raw Linguistic Vault CLI - inspect, initialize, and query data.",
    )
    parser.add_argument(
        "--db-path",
        default="data/vault/raw_vault.db",
        help="Path to SQLite database file (default: data/vault/raw_vault.db)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # init
    p_init = subparsers.add_parser("init", parents=[parent_parser], help="Initialize schema and enable WAL mode")
    p_init.add_argument(
        "--schema-file",
        default=None,
        help="Path to custom schema.sql file",
    )

    # stats
    p_stats = subparsers.add_parser("stats", parents=[parent_parser], help="Show summary statistics of vault")
    p_stats.add_argument("--json", action="store_true", help="Output raw JSON")

    # search
    p_search = subparsers.add_parser("search", parents=[parent_parser], help="Full-text search (FTS5) across payloads")
    p_search.add_argument("query", help="Text, phrase, or term to search")
    p_search.add_argument("--lang", default=None, help="Filter by ISO-639-1 language code (e.g. fa, es)")
    p_search.add_argument("--limit", type=int, default=20, help="Maximum number of results to display")
    p_search.add_argument("--json", action="store_true", help="Output raw JSON")

    # inspect
    p_inspect = subparsers.add_parser("inspect", parents=[parent_parser], help="Inspect an item and view direct URL")
    p_inspect.add_argument("item_id", help="Item ID (e.g. yt:VY9xO-tUQFc or VY9xO-tUQFc)")

    args = parser.parse_args()

    if args.command == "init":
        return cmd_init(args)
    elif args.command == "stats":
        return cmd_stats(args)
    elif args.command == "search":
        return cmd_search(args)
    elif args.command == "inspect":
        return cmd_inspect(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
