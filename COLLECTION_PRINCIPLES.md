# Agent Collection Manifesto & Directives

> **Status:** Active & Mandatory  
> **Scope:** All harvesting agents, scrapers, crawlers, and ingestion pipelines interacting with the Raw Linguistic Vault (`data/vault/raw_vault.db`).

---

## Purpose & Philosophy

The **Raw Linguistic Vault** serves as the immutable ground truth for multi-register linguistic data collection across platforms (YouTube, podcasts, web publications, digital books, and subtitles). The integrity and utility of all downstream NLP tasks, dialectology analyses, translation benchmarks, and synthetic training runs depend strictly on the uncorrupted fidelity of this raw layer.

Any modification, premature normalization, or deceptive tagging introduced at collection permanently contaminates the vault. Therefore, all automated agents and human operators must adhere strictly to these **Six Inviolable Principles**.

---

## The Six Inviolable Principles

### Principle 1: Absolute Immutability of Raw Payloads
- **Never alter, clean, normalize, or spell-check raw text during ingestion.**
- Do not strip punctuation, remove emojis, replace colloquial/dialectal spellings, remove HTML/VTT markup, or convert casing.
- Do not "repair" broken sentences or strip profanities.
- All normalization (tokenization, sentence segmentation, punctuation restoration, diacritics removal, profanity filtering) belongs exclusively in downstream derived views or processing pipelines—**never in the raw vault**.

### Principle 2: Cryptographic Provenance & Deduplication
- Every payload stored in the `payloads` table must be identified by the exact **SHA-256 hex digest** of its raw UTF-8 byte stream:
  $$\text{id} = \text{SHA256}(\text{raw\_content})$$
- Every item and payload must record:
  - Exact upstream source URL (`url`).
  - Strict ISO-8601 UTC timestamp of fetch (`fetched_at`).
  - Full upstream metadata (in `metadata_json`), preserving original IDs, view counts, tags, and creator identifiers.
- Re-ingesting identical content must resolve via idempotent deduplication (`INSERT OR IGNORE`) without generating duplicate rows.

### Principle 3: Honest Origin & Register Tagging
Downstream models and linguistic evaluations require rigorous distinction between human craft and machine synthesis. Agents must classify the `origin_type` with 100% honesty:
- **`human_transcribed`**: Creator-uploaded captions, verified manual subtitles (`.srt`, `.vtt`), professional court/broadcast transcripts.
- **`machine_transcribed`**: Automatic speech recognition outputs (e.g., YouTube auto-generated captions, Whisper models).
- **`human_written`**: Authentic written text authored by humans (books, articles, essays, user comments, forum posts).
- **`machine_translated`**: Any text translated via MT or LLMs. If `source_language != content_language`, it **must** be marked as `machine_translated`.

Falsely tagging machine-generated transcripts as `human_transcribed` invalidates linguistic authenticity metrics and is strictly prohibited.

### Principle 4: Preservation of Native Timing Formats
- When harvesting speech, transcripts, or dialogue, **always preserve native timestamped formats** (`.vtt`, `.srt`, `json3`).
- Millisecond-level alignment between tokens and audio is irreplaceable; stripping timecodes to store plain text destroys the multimodal alignment capability for future speech synthesis/recognition tasks.
- If both structured timing (`.vtt`) and plain text are available, ingest the timing format as the primary payload, or ingest both as separate `payload_type` records (`vtt` and `article_text`).

### Principle 5: Capped Sampling by Popularity & Engagement
- **Never execute unbounded, recursive web crawls.**
- Collection per source must follow disciplined, capped sampling:
  - Default cap: **Top 15–25 items per source**, ordered by engagement (view count, upvotes, or citation count) or chronological relevance.
  - Ensures a balanced corpus across diverse linguistic registers, sociolects, and dialects rather than allowing a single high-volume source to dominate the dataset.
- Always record `view_count` and `duration_seconds` when available upstream to enable stratified downstream sampling.

### Principle 6: Single-Snapshot Ingestion (No Re-Fetch Loops)
- Ingestion is single-snapshot and append-only:
  - Once an item and its payloads are hashed and ingested into the vault, they are locked.
  - Agents must not poll, refresh, or overwrite existing items in automated loops.
  - If a creator updates a video's transcript upstream, the new transcript produces a distinct SHA-256 payload linked to the item, preserving the historical delta.

---

## Schema Reference for Ingestion Agents

```sql
sources (id, platform, language_iso1, country_code, name, url, metadata_json, created_at)
  └── items (id, source_id, title, url, published_at, duration_seconds, view_count, metadata_json, created_at)
        └── payloads (id [SHA-256], item_id, payload_type, language_iso1, locale, origin_type, format, raw_content, metadata_json, fetched_at)
```

## Permitted Values Matrix

| Field | Permitted Values | Notes |
|---|---|---|
| `platform` | `youtube`, `podcast`, `book`, `web`, `subtitle`, `telegram` | Identifies content medium |
| `payload_type` | `manual_cc`, `auto_cc`, `whisper`, `article_text`, `book_chapter`, `comment_thread` | Identifies ingestion channel |
| `origin_type` | `human_transcribed`, `machine_transcribed`, `human_written`, `machine_translated` | Mandatory origin verification |
| `format` | `vtt`, `srt`, `json3`, `txt`, `html`, `json` | File format of `raw_content` |
| `language_iso1` | Valid ISO-639-1 code (`fa`, `fr`, `es`, `en`, `de`, `ar`, `zh`, etc.) | Base 2-letter language code |
| `country_code` | Valid ISO 3166-1 alpha-2 code (`FR`, `CA`, `BE`, `CH`, `SN`, `CI`, `IR`, etc.) | Country of origin for source/creator |
| `locale` | Valid BCP-47 tag (`fr-CA`, `fr-FR`, `fr-BE`, `fr-CH`, `fa-IR`, `zh-Hans`, etc.) | Dialect, regional variety, or script tag |

## Standardized `metadata_json` Conventions

To support multi-register and multi-region linguistic analysis without relational schema bloat, agents should populate the following keys in `metadata_json` when known:

- **`sources.metadata_json`**:
  - `country_code`: ISO 3166-1 alpha-2 (`"FR"`, `"CA"`, `"BE"`, `"CH"`, `"SN"`)
  - `region`: Sub-national or cultural region (`"Quebec"`, `"Wallonia"`, `"Romandy"`, `"West Africa"`)
  - `register`: `"conversational"`, `"broadcast"`, `"formal"`, `"narrative"`, `"academic"`
  - `interaction_type`: `"dialogue"`, `"monologue"`, `"panel"`
  - `domain`: Content category (`"comedy_podcast"`, `"news"`, `"street_interview"`, `"documentary"`)
- **`payloads.metadata_json`**:
  - `track_key`: Upstream subtitle language key (`"fr-CA"`, `"fr-orig"`)
  - `track_name`: Upstream human-readable track label (`"Français (Canada)"`)
  - `asr_model`: Model identifier if machine-transcribed (`"youtube_asr"`, `"whisper-large-v3"`)
  - `code_switching`: Optional list of secondary language codes if mixed speech occurs (`["en"]`, `["wo"]`)
