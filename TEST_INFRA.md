# E2E Test Infrastructure & Specification Guide

## Scaled Multi-Agent Persian Channel Harvesting Pipeline

---

## 1. Testing Philosophy & Guarantees

The End-to-End (E2E) Test Suite for the Persian Channel Harvesting Pipeline is constructed around four non-negotiable architectural pillars:

### 1.1 Opaque-Box Testing
Tests evaluate components strictly from an **external, contract-driven, opaque-box perspective**. Rather than coupling assertions to private internal helper routines, tests interact with public function signatures, CLI entry points, database state invariants, and output artifacts:
- **Partitioner Contract**: `partition_channels(catalog_path, ...)` and generated `partitions.json`.
- **Harvester Contract**: `resolve_persian_subtitles(video_metadata)` and isolated staging databases (`data/staging/shard_<id>.db`).
- **Merger Contract**: `merge_staging_to_vault(staging_dir, vault_path, ...)` and JSON audit manifest (`ingestion_manifest_<timestamp>.json`).
- **Coordinator Contract**: `run_teamwork_harvest(...)` managing parallel worker squads, log isolation, and automated consolidation.

### 1.2 Zero External Network Calls (Hermetic Test Harness)
To guarantee 100% deterministic, repeatable, and CI-ready test runs, the test harness enforces **zero outbound socket calls**:
- The `isolate_network` autouse fixture intercepts `socket.socket.connect` and rejects any non-local network traffic with an immediate `RuntimeError`.
- `MockYtDlpEngine` simulates flat playlist discovery, video metadata extraction, and in-memory WebVTT streaming without touching YouTube servers.
- Streaming WebVTT data directly into memory simulates high-speed transcript acquisition with **0 bytes of video or audio downloaded to disk**.

### 1.3 Real SQLite Engine with WAL & FTS5 (No Dummy Facades)
All test assertions run against **real SQLite databases** initialized using `src/vault/schema.sql`:
- **WAL Journal Mode**: Real write-ahead log files (`.db-wal`, `.db-shm`) with `PRAGMA synchronous = NORMAL;`.
- **Referential Integrity**: Real `PRAGMA foreign_keys = ON;` validation across `sources -> items -> payloads`.
- **FTS5 Virtual Tables & Triggers**: Real `payloads_fts` virtual table indexed via real SQLite triggers (`payloads_ai`, `payloads_ad`, `payloads_au`) and queried via real BM25 ranking (`MATCH`).
- **Atomic ATTACH Merging**: Real parameterized `ATTACH DATABASE ? AS shard;` and `INSERT OR IGNORE` deduplication.

### 1.4 Authentic Persian WebVTT Corpus
The test harness embeds authentic Persian WebVTT corpora representing distinct linguistic registers and edge cases:
1. **Formal / Broadcast Register**: Scholarly, geopolitical, and broadcast vocabulary (BBC Persian / academic style).
2. **Colloquial / Conversational Register**: Tehrani podcast speech, modern conversational idioms, and youth slang (Tabaghe 16 / Mia Plays style).
3. **Complex Unicode & Orthographic Edge Cases**:
   - Zero-Width Non-Joiner (ZWNJ `\u200c`, e.g., `می‌خواهم`, `کتاب‌ها`, `دست‌اندرکاران`).
   - Persian vs Arabic character distinction (`ک` U+06A9 vs `ك` U+0643, `ی` U+06CC vs `ي` U+064A).
   - Persian numerals (`۰۱۲۳۴۵۶۷۸۹`).
   - Mixed RTL/LTR embedded English tokens (`Python`, `PyTorch`, `LLM`, `Deep Learning`).
   - Arabic/Persian diacritics and tashkeel (حرکات: َ ِ ُ ً ٍ ٌ ّ ْ).
4. **Massive WebVTT Files**: Synthetic stress test WebVTT payloads exceeding **10,000 cues** to benchmark memory, parser efficiency, and SQLite text column capacity.

---

## 2. Feature Inventory Coverage Matrix (All 27 Features)

The following matrix maps all 27 features from `PROJECT.md` to their corresponding test tiers, test case implementations, and verification methods:

| # | Feature Name | Milestone | Test Tier | Test Case(s) | Verification Method |
|---|--------------|-----------|-----------|--------------|---------------------|
| 1 | Catalog Acquisition | M1 | Tier 1 | `test_g1_catalog_acquisition_and_presence` | Verifies `persian_channels_catalog.json` exists, has 25 channels, valid JSON schema. |
| 2 | Catalog Deduplication | M1 | Tier 1, 3 | `test_g1_catalog_deduplication_fiction_podcast`, `test_pw01_deduplication_x_squad_allocation` | Consolidates duplicate Fiction Podcast (#12 and #13) into 1 entry in Squad 1. |
| 3 | Catalog Anomaly Remediation | M1 | Tier 1, 4 | `test_g1_catalog_anomaly_remediation_kouman_and_marz`, `test_scenario_4_catalog_anomaly_auto_remediation` | Normalizes `@Kouman_Official` -> `@Kouman` and `radiomarz` -> `@radio-marz`. |
| 4 | Domain Squad Allocation | M1 | Tier 1 | `test_g1_domain_squad_allocation_4_squads` | Partitions 25 channels into 4 domain squads (Podcasts, Vlogs, Narrative, Practical). |
| 5 | Partition CLI & JSON Export | M1 | Tier 1 | `test_g1_partition_json_export_and_cli_args` | Verifies `partitions.json` export with `--shard-id` and `--channels` CLI strings. |
| 6 | Manual CC Priority | M2 | Tier 1, 3 | `test_g2_manual_cc_priority_over_auto_cc`, `test_g2_manual_cc_lang_code_*`, `test_pw03_manual_cc_x_auto_cc_fallback` | Prioritizes `manual_cc` / `human_transcribed` across `fa`, `fa-IR`, `fas`. |
| 7 | Auto CC Fallback | M2 | Tier 1, 3 | `test_g3_auto_cc_fallback_when_manual_absent`, `test_g3_auto_cc_lang_code_fa_orig` | Falls back to `auto_cc` / `machine_transcribed` when manual CC is missing. |
| 8 | In-Memory VTT Streaming | M2 | Tier 1, 3 | `test_g4_in_memory_vtt_streaming_zero_media_download`, `test_pw04_in_memory_vtt_streaming_x_resumption` | Streams WebVTT directly into memory via HTTP; confirms 0 bytes video/audio downloaded. |
| 9 | Strict Duration Filter | M2 | Tier 1, 2, 3 | `test_g5_duration_filter_*`, `test_boundary_duration_*`, `test_pw02_duration_filter_x_capping_policy` | Enforces $\ge 600$s; safely excludes Shorts (<60s), clips (<600s), and `None`. |
| 10 | Video Capping Policy | M2 | Tier 1, 3 | `test_g6_capping_policy_slices_top_n_by_view_count`, `test_pw02_duration_filter_x_capping_policy` | Caps longform videos to top N sorted by `view_count DESC` (default: 25). |
| 11 | Uncapped Ingestion | M2 | Tier 1, 4 | `test_g6_uncapped_all_videos_bypasses_cap`, `test_scenario_3_uncapped_ingestion_on_high_volume_channel` | Bypasses cap with `--all-videos` and ingests 100% of eligible longform videos. |
| 12 | Staging Shard Isolation | M2 | Tier 1, 3 | `test_g7_staging_shard_initialization`, `test_g7_staging_shard_wal_and_pragmas`, `test_pw05_staging_shards_x_concurrency` | Creates isolated `shard_<id>.db` with WAL mode and schema. |
| 13 | Idempotent Shard Resumption | M2 | Tier 1, 4 | `test_g7_staging_shard_idempotent_resumption`, `test_scenario_2_resumption_after_worker_crash` | Skips already-ingested items/payloads on restart with 0 duplicate rows. |
| 14 | Shard Harvest CLI | M2 | Tier 1 | `test_g7_staging_shard_multi_shard_isolation` | Verifies execution across `--shard-id`, `--channels`, `--cap-per-channel`, `--min-duration`. |
| 15 | Staging Database Discovery | M3 | Tier 1, 3 | `test_g8_attach_merge_empty_staging_directory`, `test_pw06_shard_discovery_x_corrupt_quarantine_x_attach_merge` | Scans directory for all `shard_*.db` databases with `quick_check` validation. |
| 16 | Atomic SQLite ATTACH Merge | M3 | Tier 1, 3 | `test_g8_attach_merge_sources_items_payloads`, `test_g8_attach_merge_multiple_shards_consolidated` | Attaches staging shards and merges records into vault within atomic transaction. |
| 17 | Multi-Table Deduplication | M3 | Tier 1, 3 | `test_g8_attach_merge_deduplication_via_insert_or_ignore`, `test_pw07_attach_merge_x_deduplication` | Executes `INSERT OR IGNORE` across sources, items, and SHA-256 payloads. |
| 18 | FTS5 Index Sync & Rebuild | M3 | Tier 1, 3 | `test_g9_fts5_triggers_*`, `test_g9_fts5_explicit_rebuild_command`, `test_pw08_attach_merge_x_fts5_trigger_synchronization` | Validates `payloads_ai`, `payloads_ad`, `payloads_au` and explicit `rebuild`. |
| 19 | Ingestion Manifest Output | M3 | Tier 1, 4 | `test_g10_manifest_*`, `test_scenario_1_standard_production_pipeline` | Produces audit JSON manifest recording sources, items, payloads, and word counts. |
| 20 | Merger CLI | M3 | Tier 1, 3 | `test_g10_manifest_custom_output_destination`, `test_pw10_merger_cli_x_target_cloud_storage` | Supports `--staging-dir`, `--vault-path`, `--rebuild-fts`, `--manifest-out`, `--target-gdrive`. |
| 21 | Parallel Shard Execution | M4 | Tier 1, 3 | `test_g11_teamwork_coordinator_partition_and_dispatch`, `test_pw05_staging_shards_x_concurrency` | Spawns and manages 4 concurrent worker shard processes. |
| 22 | Isolated Worker Logging | M4 | Tier 1 | `test_g11_teamwork_coordinator_isolated_shard_logs` | Pipes stdout/stderr to isolated `shard_<id>.log` files. |
| 23 | Signal & Error Handling | M4 | Tier 1, 3 | `test_g11_teamwork_coordinator_clean_shards_cleanup`, `test_pw09_parallel_execution_x_status_management` | Graceful termination, error capture, and process state tracking. |
| 24 | Automated Harvest-Merge Pipeline | M4 | Tier 1, 4 | `test_g11_teamwork_coordinator_auto_merge_trigger`, `test_scenario_1_standard_production_pipeline` | Full end-to-end flow: partition -> parallel harvest -> auto-merge -> manifest. |
| 25 | Cloud Storage Integration | M4 | Tier 1, 3 | `test_pw10_merger_cli_x_target_cloud_storage` | Manages Google Drive destination targeting and manifest provenance flags. |
| 26 | Requirement-Driven E2E Suite | M5 | All Tiers | All 85 test cases in `tests/e2e/` | Comprehensive 4-tier requirement-driven opaque-box test suite. |
| 27 | Adversarial Hardening | M5 | Tier 2, 4 | `test_boundary_sql_injection_resistance`, `test_boundary_corrupted_staging_db_quarantine`, `test_scenario_5_fault_tolerant_merge_with_corrupted_shard` | Stress testing, corrupt database quarantine, SQL injection mitigation. |

---

## 3. Test Suite Layout & Hierarchy

```
tests/e2e/
├── __init__.py
├── conftest.py                       # Network isolation, fixtures, Mock yt-dlp, sandboxes
├── contract_helpers.py               # Genuine contract implementations & script bridges
├── test_tier1_feature_coverage.py    # 56 tests covering 11 core functional groups
├── test_tier2_boundaries.py          # 14 tests covering edge cases, limits, and security
├── test_tier3_pairwise.py            # 10 tests covering pairwise feature interactions
└── test_tier4_real_world.py          # 5 multi-stage end-to-end production scenarios
```

### Tier 1: Feature Coverage (`test_tier1_feature_coverage.py` — 56 Tests)
Organized into 11 functional groups with $\ge 5$ genuine test cases each:
- **Group 1**: Catalog acquisition, deduplication, anomaly remediation, squad allocation, partition export (6 tests).
- **Group 2**: Manual CC priority across `fa`, `fa-IR`, `fas`, format selection (5 tests).
- **Group 3**: Auto CC fallback across `fa-orig`, `fa-IR`, and absence handling (5 tests).
- **Group 4**: In-memory VTT streaming, SHA-256 computation, deduplication, word counting (5 tests).
- **Group 5**: Strict duration filter ($\ge 600$s, Shorts, promos, None handling) (5 tests).
- **Group 6**: Capping policy (top N by view count) vs uncapped (`--all-videos`) (5 tests).
- **Group 7**: Staging database isolation, WAL pragmas, foreign keys, resumption (5 tests).
- **Group 8**: SQLite ATTACH merge, atomic transaction, `INSERT OR IGNORE` (5 tests).
- **Group 9**: FTS5 triggers (`payloads_ai`, `payloads_ad`, `payloads_au`), BM25 search, rebuild (5 tests).
- **Group 10**: Ingestion manifest generation, metrics matching, word counts, custom paths (5 tests).
- **Group 11**: Teamwork coordinator runner, logging, auto-merge, clean shards (5 tests).

### Tier 2: Boundary & Corner Cases (`test_tier2_boundaries.py` — 14 Tests)
- `test_boundary_duration_599s_excluded`: 599s video excluded.
- `test_boundary_duration_600s_accepted`: 600s video accepted.
- `test_boundary_duration_none_safely_excluded`: None duration handled without crash.
- `test_boundary_empty_catalog`: Empty catalog produces clean 0-channel shards.
- `test_boundary_duplicate_channels_in_catalog`: Multiple duplicates deduplicated to 1.
- `test_boundary_zero_video_channel_navidkashani`: `@navidkashani` handled gracefully.
- `test_boundary_http_timeout_during_vtt_streaming`: HTTP timeouts caught and logged.
- `test_boundary_missing_vtt_tracks`: Items cataloged with 0 payloads when tracks absent.
- `test_boundary_malformed_metadata`: Corrupt dates and null view counts safely ingested.
- `test_boundary_corrupted_staging_db_quarantine`: Corrupt DBs isolated via `PRAGMA quick_check`.
- `test_boundary_sqlite_busy_timeout_locked_handling`: Write lock contention resolved via timeout.
- `test_boundary_persian_unicode_edge_cases`: ZWNJ, digits, Arabic chars, RTL/LTR searched.
- `test_boundary_extremely_large_webvtt`: Payloads $> 10,000$ cues indexed and searched.
- `test_boundary_sql_injection_resistance`: Malicious SQL payloads safely sanitized.

### Tier 3: Pairwise Interactions (`test_tier3_pairwise.py` — 10 Tests)
- **PW-01**: Deduplication x Squad allocation.
- **PW-02**: Duration filter x Capping policy (viral Shorts excluded before top-N capping).
- **PW-03**: Manual CC x Auto CC fallback in heterogeneous channels.
- **PW-04**: In-memory VTT streaming x Resumption (skips re-fetching completed items).
- **PW-05**: Staging shards x Concurrency (4 parallel workers with 0 locks).
- **PW-06**: Shard discovery x Corrupt shard quarantine x ATTACH merge.
- **PW-07**: ATTACH merge x Deduplication (`INSERT OR IGNORE`).
- **PW-08**: ATTACH merge x FTS5 trigger synchronization.
- **PW-09**: Parallel execution x Status management.
- **PW-10**: Merger CLI x Target cloud storage integration.

### Tier 4: Real-World Application Scenarios (`test_tier4_real_world.py` — 5 Scenarios)
- **Scenario 1**: Standard multi-squad production pipeline (catalog -> 4 shards -> ATTACH merge -> FTS BM25 search -> manifest audit).
- **Scenario 2**: Resumption after simulated worker crash (crash midway, restart, 0 duplicates).
- **Scenario 3**: Uncapped ingestion on high-volume channels (`--all-videos`).
- **Scenario 4**: Catalog anomaly auto-remediation run (`@Kouman`, `@radio-marz`, Fiction dup).
- **Scenario 5**: Fault-tolerant merge with corrupted shard quarantine.

---

## 4. Test Runner Invocation Guide

The test suite can be executed using either standard **pytest** or the dedicated standalone runner **`scripts/run_e2e_tests.py`**.

### 4.1 Pytest Invocations
```bash
# Run all E2E tests across all tiers with verbose output
.venv/bin/pytest tests/e2e/ -v

# Run specific tiers
.venv/bin/pytest tests/e2e/test_tier1_feature_coverage.py -v
.venv/bin/pytest tests/e2e/test_tier2_boundaries.py -v
.venv/bin/pytest tests/e2e/test_tier3_pairwise.py -v
.venv/bin/pytest tests/e2e/test_tier4_real_world.py -v

# Run specific feature group or test
.venv/bin/pytest tests/e2e/test_tier1_feature_coverage.py -k "test_g2_manual_cc" -v
```

### 4.2 Standalone Runner CLI (`scripts/run_e2e_tests.py`)
```bash
# Run all tiers (default)
python scripts/run_e2e_tests.py --tier all -v

# Run specific tier with JSON execution report
python scripts/run_e2e_tests.py --tier 1 --json-report data/staging/e2e_tier1_report.json -v
python scripts/run_e2e_tests.py --tier 2 --json-report data/staging/e2e_tier2_report.json -v
python scripts/run_e2e_tests.py --tier 3 --json-report data/staging/e2e_tier3_report.json -v
python scripts/run_e2e_tests.py --tier 4 --json-report data/staging/e2e_tier4_report.json -v

# Force standalone reflection runner (bypassing pytest)
python scripts/run_e2e_tests.py --tier all --no-pytest -v
```

### 4.3 Exit Codes
- `0`: All test cases passed successfully.
- `1`: One or more test cases failed or encountered an error.
