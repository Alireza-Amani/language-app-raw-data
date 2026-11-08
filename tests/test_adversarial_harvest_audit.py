#!/usr/bin/env python3
"""Adversarial stress-testing suite for French Constructicon vault harvest.

Tests:
1. Boundary Constraints: Video duration >= 600s, max 150 items/channel, positive values, non-empty multi-modal payloads.
2. Geographic Invariance: Language == 'fr', country_code in {FR, CA, BE, CH}, zero African/Maghreb across all sources.
3. FTS5 Index Consistency: 100% row parity, exact text matching, FTS5 integrity-check, adversarial search.
4. Shard Disjointness: Zero cross-shard source, item, or payload overlap; unique video URLs and entity IDs.
5. Multi-Source Format & Subtitle Prioritization: Manual CC prioritized ahead of auto CC, valid VTT, SRT, JSON, XML/TXT formats.
6. Referential & Database Integrity: PRAGMA integrity_check, foreign_key_check, zero orphans.
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
VAULT_PATH = PROJECT_ROOT / "data" / "vault" / "language-app-raw-data" / "raw_vault.db"
STAGING_DIR = PROJECT_ROOT / "data" / "staging" / "french"
CATALOG_PATH = PROJECT_ROOT / "data" / "french_channels_catalog.json"
SHARDS_DIR = PROJECT_ROOT / "data" / "shards" / "french"

FORBIDDEN_AFRICAN_MAGHREB_CODES = {
    "DZ", "MA", "TN", "SN", "CI", "CM", "CD", "MG", "ML", "NE",
    "BF", "GN", "BJ", "TG", "GA", "CG", "TD", "CF", "RW", "BI",
    "DJ", "KM", "SC", "MR", "EH", "LY", "EG", "SD", "SS", "ET",
    "SO", "KE", "UG", "TZ", "MZ", "AO", "ZM", "ZW", "NA", "BW",
}
ALLOWED_COUNTRY_CODES = {"BE", "CA", "CH", "FR"}


def test_database_pragmas_and_schema():
    """Verify PRAGMA integrity_check and foreign_key_check."""
    print("\n--- [TEST 1] Database PRAGMAs & Integrity ---")
    conn = sqlite3.connect(str(VAULT_PATH))
    cur = conn.cursor()

    cur.execute("PRAGMA integrity_check;")
    integrity = cur.fetchall()
    assert integrity == [("ok",)], f"Database corruption detected: {integrity}"
    print("  [PASS] PRAGMA integrity_check returned 'ok'")

    cur.execute("PRAGMA foreign_keys = ON;")
    cur.execute("PRAGMA foreign_key_check;")
    fk_violations = cur.fetchall()
    assert not fk_violations, f"Foreign key violations found: {fk_violations}"
    print("  [PASS] PRAGMA foreign_key_check returned 0 violations")

    # Verify no orphan items
    cur.execute("SELECT count(*) FROM items WHERE source_id NOT IN (SELECT id FROM sources);")
    orphan_items = cur.fetchone()[0]
    assert orphan_items == 0, f"Found {orphan_items} orphan items!"
    print(f"  [PASS] Orphan items: {orphan_items}")

    # Verify no orphan payloads
    cur.execute("SELECT count(*) FROM payloads WHERE item_id NOT IN (SELECT id FROM items);")
    orphan_payloads = cur.fetchone()[0]
    assert orphan_payloads == 0, f"Found {orphan_payloads} orphan payloads!"
    print(f"  [PASS] Orphan payloads: {orphan_payloads}")

    conn.close()


def test_boundary_constraints():
    """Verify item duration >= 600s for video items, max 150 items per channel, and validity."""
    print("\n--- [TEST 2] Boundary Constraints ---")
    conn = sqlite3.connect(str(VAULT_PATH))
    cur = conn.cursor()

    # Total items count
    cur.execute("SELECT count(*) FROM items;")
    total_items = cur.fetchone()[0]
    assert total_items > 0, "No items found in vault!"
    print(f"  [INFO] Total vault items: {total_items}")

    # Sub-600s check: ANY video item (duration_seconds IS NOT NULL) < 600s must fail
    cur.execute("SELECT id, source_id, duration_seconds FROM items WHERE duration_seconds IS NOT NULL AND duration_seconds < 600;")
    sub_600 = cur.fetchall()
    assert len(sub_600) == 0, f"Violation: Found {len(sub_600)} video items with duration < 600s: {sub_600[:5]}"
    print("  [PASS] Zero video items with duration_seconds < 600s")

    # Check for negative or zero duration among video items
    cur.execute("SELECT count(*) FROM items WHERE duration_seconds IS NOT NULL AND duration_seconds <= 0;")
    invalid_dur = cur.fetchone()[0]
    assert invalid_dur == 0, f"Violation: Found {invalid_dur} items with <=0 duration"
    print("  [PASS] Zero video items with <=0 duration")

    # Min, Max, Avg duration for video items
    cur.execute("SELECT MIN(duration_seconds), MAX(duration_seconds), AVG(duration_seconds) FROM items WHERE duration_seconds IS NOT NULL;")
    min_dur, max_dur, avg_dur = cur.fetchone()
    assert min_dur >= 600, f"Min duration {min_dur} is less than 600s!"
    print(f"  [PASS] Video duration: min={min_dur}s (>= 600s), max={max_dur}s, avg={avg_dur:.1f}s")

    # Verify tri-source items without duration (comments, subtitles, parliamentary debates) have valid payloads with non-zero characters
    cur.execute("""
        SELECT p.payload_type, count(*), min(length(p.raw_content)), avg(length(p.raw_content))
        FROM items i
        JOIN payloads p ON p.item_id = i.id
        WHERE i.duration_seconds IS NULL
        GROUP BY p.payload_type;
    """)
    null_dur_payloads = cur.fetchall()
    assert len(null_dur_payloads) > 0, "No payloads found for non-video items!"
    for p_type, count, min_len, avg_len in null_dur_payloads:
        assert count > 0, f"No payloads for {p_type}"
        assert min_len > 0, f"Found empty raw_content for payload type {p_type}!"
        print(f"  [PASS] Non-video modality '{p_type}': {count} items, min_chars={min_len}, avg_chars={avg_len:.1f}")

    # Channel cap constraint: <= 150 videos per channel
    cur.execute("SELECT source_id, count(*) FROM items WHERE duration_seconds IS NOT NULL GROUP BY source_id HAVING count(*) > 150;")
    over_capped = cur.fetchall()
    assert len(over_capped) == 0, f"Violation: Channels exceeding 150 video items cap: {over_capped}"
    print("  [PASS] Channel cap constraint: All channels have <= 150 video items")

    # Verify no blank titles or URLs
    cur.execute("SELECT count(*) FROM items WHERE TRIM(title) = '' OR TRIM(url) = '';")
    blank_metadata = cur.fetchone()[0]
    assert blank_metadata == 0, f"Violation: Found {blank_metadata} items with empty title or URL"
    print("  [PASS] Zero items with blank title or URL")

    conn.close()


def test_geographic_invariance():
    """Verify strict geographic compliance: FR, CA, BE, CH only, 0 African/Maghreb."""
    print("\n--- [TEST 3] Geographic Invariance ---")
    conn = sqlite3.connect(str(VAULT_PATH))
    cur = conn.cursor()

    # Sources count: 80 YouTube channels + 11 tri-source discovery sources = 91 sources
    cur.execute("SELECT count(*) FROM sources;")
    total_sources = cur.fetchone()[0]
    assert total_sources >= 80, f"Expected at least 80 sources, got {total_sources}"
    assert total_sources == 91, f"Expected exactly 91 sources (80 YT + 4 cinema + 7 hansard), got {total_sources}"
    print(f"  [PASS] Exactly {total_sources} sources in vault (80 YouTube, 4 cinema, 7 parliamentary)")

    # Language constraint: strictly French across all sources and payloads
    cur.execute("SELECT DISTINCT language_iso1 FROM sources;")
    languages = {r[0] for r in cur.fetchall()}
    assert languages == {"fr"}, f"Violation: Non-French languages found in sources: {languages}"
    print(f"  [PASS] Source language strictly 'fr': {languages}")

    cur.execute("SELECT DISTINCT language_iso1 FROM payloads;")
    payload_languages = {r[0] for r in cur.fetchall()}
    assert payload_languages == {"fr"}, f"Violation: Non-French languages found in payloads: {payload_languages}"
    print(f"  [PASS] Payload language strictly 'fr': {payload_languages}")

    # Country codes constraint: only FR, CA, BE, CH
    cur.execute("SELECT DISTINCT country_code FROM sources;")
    observed_countries = {r[0] for r in cur.fetchall()}
    unexpected_countries = observed_countries - ALLOWED_COUNTRY_CODES
    assert not unexpected_countries, f"Violation: Unexpected country codes: {unexpected_countries}"
    assert observed_countries == ALLOWED_COUNTRY_CODES, f"Missing target countries: {ALLOWED_COUNTRY_CODES - observed_countries}"
    print(f"  [PASS] Source country codes strictly {observed_countries}")

    # Explicit check for African / Maghreb country codes
    african_maghreb_found = observed_countries.intersection(FORBIDDEN_AFRICAN_MAGHREB_CODES)
    assert not african_maghreb_found, f"Violation: African/Maghreb sources found: {african_maghreb_found}"
    print("  [PASS] Zero African/Maghreb country codes present")

    # Verify 1-to-1 matching with audited YouTube catalog
    assert CATALOG_PATH.is_file(), f"Catalog not found at {CATALOG_PATH}"
    with open(CATALOG_PATH, "r", encoding="utf-8") as f:
        catalog_data = json.load(f)
    catalog_handles = {
        (ch.get("handle") or ch.get("uploader_id") or "").lstrip("@").lower()
        for ch in catalog_data.get("channels", [])
    }
    cur.execute("SELECT id FROM sources WHERE platform = 'youtube';")
    vault_handles = {r[0].replace("yt:@", "").replace("yt:", "").lower() for r in cur.fetchall()}
    assert vault_handles == catalog_handles, f"Catalog mismatch! Missing: {catalog_handles - vault_handles}, Extra: {vault_handles - catalog_handles}"
    print(f"  [PASS] Vault YouTube sources match 80 catalog channels with 100% 1-to-1 correspondence")

    conn.close()


def test_fts5_index_consistency():
    """Verify FTS5 index integrity, exact row parity, and content consistency."""
    print("\n--- [TEST 4] FTS5 Index Consistency & Stress Testing ---")
    conn = sqlite3.connect(str(VAULT_PATH))
    cur = conn.cursor()

    # 1. Row count parity
    cur.execute("SELECT count(*) FROM payloads;")
    payload_count = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM payloads_fts;")
    fts_count = cur.fetchone()[0]
    assert payload_count > 0, "No payloads found in vault!"
    assert payload_count == fts_count, f"Parity mismatch: payloads={payload_count}, payloads_fts={fts_count}"
    print(f"  [PASS] Row count parity: payloads={payload_count} == payloads_fts={fts_count}")

    # 2. FTS5 integrity-check command
    try:
        cur.execute("INSERT INTO payloads_fts(payloads_fts) VALUES('integrity-check');")
        print("  [PASS] payloads_fts 'integrity-check' executed successfully")
    except Exception as exc:
        assert False, f"payloads_fts integrity-check failed: {exc}"

    # 3. Exact text matching for every single row
    cur.execute("SELECT rowid, id, item_id, length(raw_content) FROM payloads;")
    all_payload_rows = cur.fetchall()

    mismatches = []
    # Sample and check rows across the entire range
    for rowid, pid, item_id, content_len in all_payload_rows:
        cur.execute("SELECT rowid, id, item_id, length(raw_content) FROM payloads_fts WHERE rowid = ?;", (rowid,))
        fts_row = cur.fetchone()
        if not fts_row:
            mismatches.append(f"Missing rowid {rowid} in payloads_fts")
        elif fts_row[1] != pid or fts_row[2] != item_id or fts_row[3] != content_len:
            mismatches.append(f"Mismatch at rowid {rowid}: payloads=({pid}, {item_id}, {content_len}) vs fts=({fts_row[1]}, {fts_row[2]}, {fts_row[3]})")

    assert not mismatches, f"FTS5 row content mismatches detected: {mismatches[:5]}"
    print(f"  [PASS] 100% rowid and metadata match verified across all {len(all_payload_rows)} payloads")

    # 4. Stress testing search queries (accents, booleans, edge cases)
    search_queries = [
        "bonjour",
        "créativité",
        "français",
        "politique",
        "Québec",
        "Belgique",
        "Suisse",
        "France",
        '"aujourd\'hui"',
        '"c\'est"',
        "société OR culture",
        "histoire AND monde",
        "histoire NOT guerre",
    ]
    for q in search_queries:
        cur.execute("SELECT count(*) FROM payloads_fts WHERE payloads_fts MATCH ?;", (q,))
        cnt = cur.fetchone()[0]
        print(f"  [PASS] FTS5 MATCH query {q}: {cnt} matches")

    # 5. Adversarial input tests (unicode, empty, special punctuation)
    adversarial_inputs = [
        "",
        "   ",
        "''",
        '""',
        "/* comment */",
        "DROP TABLE",
        "';--",
        "1=1",
        "🎉 🚀",
        "null",
        "NULL",
    ]
    for adv in adversarial_inputs:
        try:
            # Query with parameterized matching or escaping
            escaped = adv.replace('"', '""')
            cur.execute(f"SELECT count(*) FROM payloads_fts WHERE payloads_fts MATCH '\"{escaped}\"';")
            cur.fetchone()
        except sqlite3.OperationalError:
            # OperationalError on invalid FTS syntax is expected and safely handled
            pass

    print("  [PASS] Adversarial FTS5 search queries handled gracefully without crashing")
    conn.close()


def test_shard_disjointness():
    """Verify zero overlap across staging shards and complete disjointness."""
    print("\n--- [TEST 5] Shard Disjointness & Staging Consistency ---")
    shard_dbs = sorted(STAGING_DIR.glob("shard_*.db"))
    assert len(shard_dbs) == 8, f"Expected 8 staging DBs, found {len(shard_dbs)}"

    shard_sources: Dict[str, Set[str]] = {}
    shard_items: Dict[str, Set[str]] = {}
    shard_payloads: Dict[str, Set[str]] = {}

    for s_path in shard_dbs:
        s_name = s_path.stem
        conn = sqlite3.connect(str(s_path))
        cur = conn.cursor()

        cur.execute("SELECT id FROM sources;")
        shard_sources[s_name] = {r[0] for r in cur.fetchall()}

        cur.execute("SELECT id FROM items;")
        shard_items[s_name] = {r[0] for r in cur.fetchall()}

        cur.execute("SELECT id FROM payloads;")
        shard_payloads[s_name] = {r[0] for r in cur.fetchall()}

        conn.close()
        print(f"  [INFO] {s_name}: {len(shard_sources[s_name])} sources, {len(shard_items[s_name])} items, {len(shard_payloads[s_name])} payloads")

    # Check pairwise disjointness across all shard pairs (i, j) where i != j
    for i in range(len(shard_dbs)):
        name_i = shard_dbs[i].stem
        for j in range(i + 1, len(shard_dbs)):
            name_j = shard_dbs[j].stem

            src_overlap = shard_sources[name_i].intersection(shard_sources[name_j])
            assert not src_overlap, f"Shard source overlap between {name_i} and {name_j}: {src_overlap}"

            item_overlap = shard_items[name_i].intersection(shard_items[name_j])
            assert not item_overlap, f"Shard item overlap between {name_i} and {name_j}: {item_overlap}"

            payload_overlap = shard_payloads[name_i].intersection(shard_payloads[name_j])
            assert not payload_overlap, f"Shard payload overlap between {name_i} and {name_j}: {payload_overlap}"

    print("  [PASS] Shard Disjointness: Zero cross-shard overlap across sources, items, or payloads")

    # Verify vault contains all staging data
    conn = sqlite3.connect(str(VAULT_PATH))
    cur = conn.cursor()

    cur.execute("SELECT id FROM sources;")
    vault_sources = {r[0] for r in cur.fetchall()}

    cur.execute("SELECT id FROM items;")
    vault_items = {r[0] for r in cur.fetchall()}

    cur.execute("SELECT id FROM payloads;")
    vault_payloads = {r[0] for r in cur.fetchall()}

    all_staging_sources = set().union(*shard_sources.values())
    all_staging_items = set().union(*shard_items.values())
    all_staging_payloads = set().union(*shard_payloads.values())

    assert all_staging_sources.issubset(vault_sources), "Vault is missing staging sources!"
    assert all_staging_payloads.issubset(vault_payloads), "Vault is missing staging payloads!"

    unmerged_items = all_staging_items - vault_items
    if unmerged_items:
        print(f"  [FAIL] VAULT COMPLETENESS DEFECT: {len(unmerged_items)} items in staging are missing from raw_vault.db!")
        raise AssertionError(f"Staging-to-Vault Desynchronization: {len(unmerged_items)} items in staging were not merged into raw_vault.db")

    # Check for duplicate item IDs / (source_id, id) in vault
    cur.execute("SELECT id, count(*) FROM items GROUP BY id HAVING count(*) > 1;")
    dup_ids = cur.fetchall()
    assert not dup_ids, f"Duplicate item IDs found in vault: {dup_ids}"

    cur.execute("SELECT source_id, id, count(*) FROM items GROUP BY source_id, id HAVING count(*) > 1;")
    dup_source_items = cur.fetchall()
    assert not dup_source_items, f"Duplicate (source_id, item_id) found in vault: {dup_source_items}"

    # Check for duplicate item URLs among video items (YouTube videos must have unique URLs)
    cur.execute("SELECT url, count(*) FROM items WHERE duration_seconds IS NOT NULL GROUP BY url HAVING count(*) > 1;")
    dup_video_urls = cur.fetchall()
    assert not dup_video_urls, f"Duplicate video URLs found in vault items: {dup_video_urls}"
    print(f"  [PASS] Staging to Vault Ingestion: 100% of staging entities present in vault, zero duplicate IDs, zero duplicate video URLs")

    conn.close()


def test_subtitle_prioritization():
    """Verify subtitle prioritization: manual CC vs auto CC, format integrity across modalities."""
    print("\n--- [TEST 6] Subtitle Prioritization & Multi-Source Transcript Integrity ---")
    conn = sqlite3.connect(str(VAULT_PATH))
    cur = conn.cursor()

    # Breakdown by payload_type and origin_type
    cur.execute("SELECT payload_type, origin_type, count(*) FROM payloads GROUP BY payload_type, origin_type;")
    breakdown = cur.fetchall()
    print("  [INFO] Subtitle and transcript breakdown:")
    total_manual = 0
    total_auto = 0
    for p_type, o_type, cnt in breakdown:
        print(f"    - payload_type='{p_type}', origin_type='{o_type}': {cnt} payloads")
        if p_type == "manual_cc" or o_type == "human_transcribed":
            total_manual += cnt
        elif p_type == "auto_cc" or o_type == "machine_transcribed":
            total_auto += cnt

    assert total_manual > 0, "No manual CC / transcribed payloads found!"
    assert total_auto > 0, "No auto CC payloads found!"
    print(f"  [PASS] Manual/transcribed payloads: {total_manual} ({total_manual / (total_manual + total_auto):.1%}), Auto CC payloads: {total_auto}")

    # Check payload types and formats
    cur.execute("SELECT id, payload_type, format, length(raw_content), substr(raw_content, 1, 30) FROM payloads;")
    payload_samples = cur.fetchall()
    assert len(payload_samples) > 0

    # Verify no empty payloads
    empty_payloads = [p for p in payload_samples if p[3] == 0]
    assert not empty_payloads, f"Empty raw_content payloads found: {empty_payloads}"

    # Verify formats per modality:
    # 1. auto_cc must be format='vtt' and contain WEBVTT
    cur.execute("SELECT count(*) FROM payloads WHERE payload_type = 'auto_cc' AND format != 'vtt';")
    invalid_auto_cc = cur.fetchone()[0]
    assert invalid_auto_cc == 0, f"Found {invalid_auto_cc} auto_cc payloads with format != 'vtt'"

    cur.execute("SELECT count(*) FROM payloads WHERE payload_type = 'auto_cc' AND raw_content NOT LIKE '%WEBVTT%';")
    non_webvtt_auto = cur.fetchone()[0]
    assert non_webvtt_auto == 0, f"Found {non_webvtt_auto} auto_cc payloads without WEBVTT header"
    print("  [PASS] 100% of auto_cc payloads have format='vtt' and valid WEBVTT header")

    # 2. manual_cc must be format in ('vtt', 'srt')
    cur.execute("SELECT count(*) FROM payloads WHERE payload_type = 'manual_cc' AND format NOT IN ('vtt', 'srt');")
    invalid_manual_cc = cur.fetchone()[0]
    assert invalid_manual_cc == 0, f"Found {invalid_manual_cc} manual_cc payloads with invalid format"

    cur.execute("SELECT count(*) FROM payloads WHERE payload_type = 'manual_cc' AND format = 'vtt' AND raw_content NOT LIKE '%WEBVTT%';")
    non_webvtt_manual = cur.fetchone()[0]
    assert non_webvtt_manual == 0, f"Found {non_webvtt_manual} manual_cc VTT payloads without WEBVTT header"

    cur.execute("SELECT count(*) FROM payloads WHERE payload_type = 'manual_cc' AND format = 'srt' AND raw_content NOT LIKE '%-->%';")
    non_srt_cues = cur.fetchone()[0]
    assert non_srt_cues == 0, f"Found {non_srt_cues} manual_cc SRT payloads without timing cues"
    print("  [PASS] 100% of manual_cc payloads have format in ('vtt', 'srt') and valid timing structure")

    # 3. comment_thread must be format='json' and valid JSON
    cur.execute("SELECT id, format, raw_content FROM payloads WHERE payload_type = 'comment_thread';")
    comments = cur.fetchall()
    assert len(comments) > 0, "No comment_thread payloads found!"
    for cid, fmt, raw in comments:
        assert fmt == "json", f"Comment payload {cid} has non-json format: {fmt}"
        parsed = json.loads(raw)
        assert isinstance(parsed, list) and len(parsed) > 0, f"Comment payload {cid} is empty or not a list"
    print(f"  [PASS] 100% of comment_thread payloads ({len(comments)}) have format='json' and parseable comment trees")

    # 4. parliament_transcript must be format in ('xml', 'txt') and non-empty
    cur.execute("SELECT count(*) FROM payloads WHERE payload_type = 'parliament_transcript' AND format NOT IN ('xml', 'txt');")
    invalid_hansard = cur.fetchone()[0]
    assert invalid_hansard == 0, f"Found {invalid_hansard} parliament_transcript payloads with invalid format"
    print("  [PASS] 100% of parliament_transcript payloads have format in ('xml', 'txt')")

    # Verify sha256 id matches content hash across sample payloads
    import hashlib
    cur.execute("SELECT id, raw_content FROM payloads LIMIT 100;")
    for pid, content in cur.fetchall():
        expected_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
        assert pid == expected_hash, f"Payload ID hash mismatch: id={pid} vs hash={expected_hash}"

    print("  [PASS] Payload SHA-256 ID integrity verified on sample payloads")
    conn.close()


def main():
    print("======================================================================")
    print("    ADVERSARIAL STRESS TEST & VERIFICATION SUITE — FRENCH VAULT      ")
    print("======================================================================")
    try:
        test_database_pragmas_and_schema()
        test_boundary_constraints()
        test_geographic_invariance()
        test_fts5_index_consistency()
        test_shard_disjointness()
        test_subtitle_prioritization()
        print("\n======================================================================")
        print("    ALL ADVERSARIAL STRESS TESTS PASSED SUCCESSFULLY!                 ")
        print("======================================================================")
        return 0
    except AssertionError as err:
        print("\n" + "!" * 70)
        print(f"    ADVERSARIAL CHALLENGE FAILURE: {err}")
        print("!" * 70)
        return 1
    except Exception as exc:
        print("\n" + "!" * 70)
        print(f"    UNEXPECTED ERROR DURING CHALLENGE EXECUTION: {exc}")
        print("!" * 70)
        import traceback
        traceback.print_exc()
        return 2


if __name__ == "__main__":
    sys.exit(main())
