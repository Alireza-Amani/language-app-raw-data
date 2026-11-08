# Project: French Tri-Source Corpus Discovery

## Architecture
The French Tri-Source Corpus Discovery system expands the French Constructicon beyond YouTube spoken monologues into three complementary linguistic registers across France (`FR`), Canada/Québec (`CA`), Belgium (`BE`), and Switzerland (`CH`) — strictly excluding African and Maghreb Francophonie:
1. **Stream A: Pre-2022 YouTube Comments** (`comment_thread` — `human_written`): Authentic colloquial comments posted strictly before January 1, 2022 (`timestamp < 1640995200`), sourced across our 80 audited French channels in `data/french_channels_catalog.json`.
2. **Stream B: Cinema & TV Drama Subtitles** (`manual_cc` — `human_transcribed`): Interpersonal dramatic dialogue from authentic open French subtitle corpora (OPUS OpenSubtitles, OpenSubtitles export shards), filtered for original Francophone productions (`original_language = "fr"`).
3. **Stream C: Parliamentary Debates / Hansard** (`parliament_transcript` — `human_transcribed`): High-formal deliberative discourse from official open government archives in Canada (House of Commons `HAN*-F.XML` & Assemblée nationale du Québec), France (Assemblée nationale Syceron open data), Belgium (CLARIN ParlaMint-BE & Belgian parliamentary archives), and Switzerland (Curia Vista / Bulletin officiel OData API).

```
                                  +-----------------------------------------------+
                                  | French Tri-Source Corpus Discovery Framework  |
                                  +-----------------------------------------------+
                                                          |
                 +----------------------------------------+---------------------------------------+
                 |                                        |                                       |
                 v                                        v                                       v
    [Stream A: Pre-2022 Comments]            [Stream B: Cinema Subtitles]            [Stream C: Hansard Debates]
    scripts/discover_french_                 scripts/discover_french_                scripts/discover_french_
      pre2022_comments.py                      cinema_subtitles.py                     hansard_debates.py
                 |                                        |                                       |
                 v                                        v                                       v
    data/french_pre2022_                     data/french_cinema_                     data/french_hansard_
      comments_catalog.json                    subtitles_catalog.json                  debates_catalog.json
                 |                                        |                                       |
                 +----------------------------------------+---------------------------------------+
                                                          |
                                                          v
                                      +---------------------------------------+
                                      | Automated Test Suite & Multi-Source   |
                                      |   Victory Certification               |
                                      | tests/unit/                           |
                                      |   test_discover_french_tri_source.py  |
                                      +---------------------------------------+
```

## Feature Inventory
| # | Feature | Description | Milestone | Source |
|---|---------|-------------|-----------|--------|
| 1 | Test Harness & Invariant Assertions | Strict test suite validating temporal cutoff (`timestamp < 1640995200`), geographic invariance (`FR`, `CA`, `BE`, `CH`), live reachability, and catalog schema parity | M1 | survey |
| 2 | Stream A Pre-2022 Video & Comment Discovery | Script `scripts/discover_french_pre2022_comments.py` discovering pre-2022 videos across 80 audited channels, extracting top comments with strict `timestamp < 1640995200` filter | M2 | survey |
| 3 | Stream A Pre-2022 Comments Catalog | Catalog `data/french_pre2022_comments_catalog.json` indexing verified pre-2022 comment threads across FR, CA, BE, CH | M2 | survey |
| 4 | Stream B Cinema & TV Subtitle Discovery | Script `scripts/discover_french_cinema_subtitles.py` discovering and indexing authentic original Francophone productions (`original_language = "fr"`) across FR, CA, BE, CH from OPUS / OpenSubtitles | M3 | survey |
| 5 | Stream B Cinema Subtitles Catalog | Catalog `data/french_cinema_subtitles_catalog.json` indexing verified cinema/TV subtitle sources with direct download links | M3 | survey |
| 6 | Stream C Hansard Parliamentary Debates Discovery | Script `scripts/discover_french_hansard_debates.py` discovering official verbatim transcripts across CA (ourcommons.ca / assnat.qc.ca), FR (Syceron open data), BE (ParlaMint-BE / regional parliaments), and CH (Curia Vista OData API) | M4 | survey |
| 7 | Stream C Hansard Debates Catalog | Catalog `data/french_hansard_debates_catalog.json` indexing verified parliamentary debate sessions across CA, FR, BE, CH | M4 | survey |
| 8 | Multi-Source End-to-End Audit & Victory Certification | 100% test pass on `tests/unit/test_discover_french_tri_source.py`, adversarial checks, forensic integrity audit, and final handoff | Final | survey |

## Milestones
| # | Name | Scope | Dependencies | Status |
|---|------|-------|-------------|--------|
| M1 | Test Suite & Verification Baseline | Author `tests/unit/test_discover_french_tri_source.py` enforcing strict pre-2022 timestamp cutoff, geographic bounds, and schema contract | none | DONE (`tests/unit/test_discover_french_tri_source.py`, 242 passed, un-mocked reachability verified) |
| M2 | Stream A: Pre-2022 YouTube Comments Discovery | Implement `scripts/discover_french_pre2022_comments.py` and generate `data/french_pre2022_comments_catalog.json` | M1 | DONE (`data/french_pre2022_comments_catalog.json`, 73 items, 2,567 comments, 100% pre-2022) |
| M3 | Stream B: Cinema & TV Subtitles Discovery | Implement `scripts/discover_french_cinema_subtitles.py` and generate `data/french_cinema_subtitles_catalog.json` | M1 | DONE (`data/french_cinema_subtitles_catalog.json`, 18 canonical films, verified .srt dialogue) |
| M4 | Stream C: Parliamentary Hansard Discovery | Implement `scripts/discover_french_hansard_debates.py` and generate `data/french_hansard_debates_catalog.json` | M1 | DONE (`data/french_hansard_debates_catalog.json`, 84 items across CA, FR, BE, CH, 100% pre-2022) |
| Final | Multi-Source Verification & Victory Certification | Full test suite execution, adversarial review, forensic integrity audit, and final delivery | M1, M2, M3, M4 | DONE (16/16 unit tests passed, 249/249 repo tests passed, Reviewers APPROVE, Challengers APPROVE, Auditor CLEAN) |

## Interface Contracts

### Catalog JSON Top-Level Contract
All three catalog files (`french_pre2022_comments_catalog.json`, `french_cinema_subtitles_catalog.json`, `french_hansard_debates_catalog.json`) must strictly share the top-level schema:
```json
{
  "generated_at_utc": "YYYY-MM-DDTHH:MM:SSZ",
  "language_iso1": "fr",
  "stream_modality": "comment_thread" | "manual_cc" | "parliament_transcript",
  "allowed_country_codes": ["BE", "CA", "CH", "FR"],
  "total_items": <int>,
  "items": [ ... ]
}
```

### Catalog Item Schema Contract
Each item in the `items` array must adhere to:
```json
{
  "item_id": "<unique_string>",
  "title": "<descriptive_title>",
  "country_code": "FR" | "CA" | "BE" | "CH",
  "region": "<region_or_province>",
  "platform": "youtube" | "subtitle" | "web",
  "payload_type": "comment_thread" | "manual_cc" | "parliament_transcript",
  "origin_type": "human_written" | "human_transcribed",
  "format": "json" | "vtt" | "srt" | "xml" | "txt",
  "source_url": "<authentic_live_url>",
  "date": "YYYY-MM-DD",
  "metadata": { ... }
}
```

### Stream-Specific Metadata Contracts
- **Stream A (Pre-2022 Comments)**:
  `metadata.comments`: Array of `{ "id": str, "text": str, "author": str, "timestamp": int < 1640995200, "date": "YYYY-MM-DD" }`.
- **Stream B (Cinema Subtitles)**:
  `metadata`: `{ "original_language": "fr", "year": int, "director": str, "imdb_id": str, "download_url": str, "format": "srt" | "vtt" }`.
- **Stream C (Hansard Debates)**:
  `metadata`: `{ "chamber": str, "session": str, "sitting_number": str, "speech_count": int, "download_url": str }`.

## Code Layout
- Test Suite: `tests/unit/test_discover_french_tri_source.py`
- Discovery Scripts:
  - `scripts/discover_french_pre2022_comments.py`
  - `scripts/discover_french_cinema_subtitles.py`
  - `scripts/discover_french_hansard_debates.py`
- Data Catalogs:
  - `data/french_pre2022_comments_catalog.json`
  - `data/french_cinema_subtitles_catalog.json`
  - `data/french_hansard_debates_catalog.json`
- Coordination Metadata:
  - `.agents/teamwork/teamwork_preview_orchestrator_3/`
