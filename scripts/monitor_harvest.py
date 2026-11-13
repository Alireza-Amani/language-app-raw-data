"""Harvest monitor daemon to keep track of staging progress and prevent idle shutdown."""
import time
import sqlite3
import sys
from pathlib import Path

def get_staging_stats():
    staging_dir = Path("data/staging/french")
    total_items = 0
    total_payloads = 0
    shards_info = []
    for s in range(1, 9):
        db_path = staging_dir / f"shard_{s}.db"
        if db_path.is_file():
            try:
                with sqlite3.connect(str(db_path)) as conn:
                    items = conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]
                    payloads = conn.execute("SELECT COUNT(*) FROM payloads").fetchone()[0]
                    total_items += items
                    total_payloads += payloads
                    shards_info.append(f"Shard {s}: {items} items, {payloads} payloads")
            except Exception as e:
                shards_info.append(f"Shard {s}: error {e}")
    return total_items, total_payloads, shards_info

def main():
    print("[*] Starting harvest monitor daemon...", flush=True)
    while True:
        try:
            total_items, total_payloads, shards_info = get_staging_stats()
            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}] Staging: {total_items} items, {total_payloads} payloads | " + " | ".join(shards_info[:4]), flush=True)
        except Exception as exc:
            print(f"[!] Error reading stats: {exc}", flush=True)
        time.sleep(30)

if __name__ == "__main__":
    main()
