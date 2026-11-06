PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;

-- 1. SOURCES: Where content originates (channels, feeds, publications, websites)
CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY,                       -- e.g. "yt:@Tabaghe16", "web:bbc_persian"
    platform TEXT NOT NULL,                    -- "youtube", "podcast", "book", "web", "subtitle"
    language_iso1 TEXT NOT NULL,               -- e.g. "fa", "es", "de"
    name TEXT NOT NULL,                        -- Human-readable name: "طبقه 16"
    url TEXT NOT NULL,                         -- Source homepage / feed URL
    metadata_json TEXT DEFAULT '{}',           -- Arbitrary JSON metadata (subscribers, tags)
    created_at TEXT NOT NULL                   -- ISO-8601 UTC timestamp
);

-- 2. ITEMS: Units of media or publication (videos, episodes, articles, novels)
CREATE TABLE IF NOT EXISTS items (
    id TEXT PRIMARY KEY,                       -- e.g. "yt:VY9xO-tUQFc", "book:divan_hafez"
    source_id TEXT NOT NULL,                   -- FK -> sources.id
    title TEXT NOT NULL,                       -- Title of video / chapter
    url TEXT NOT NULL,                         -- Direct URL to item
    published_at TEXT,                         -- Upload / publication date
    duration_seconds INTEGER,                  -- Video/audio duration (NULL for text/books)
    view_count INTEGER,                        -- View / read count (NULL if unmeasured)
    metadata_json TEXT DEFAULT '{}',           -- Raw upstream item metadata (tags, description)
    created_at TEXT NOT NULL,
    FOREIGN KEY (source_id) REFERENCES sources (id) ON DELETE CASCADE
);

-- 3. PAYLOADS: Raw, unmodified text / transcript byte streams
CREATE TABLE IF NOT EXISTS payloads (
    id TEXT PRIMARY KEY,                       -- SHA-256 hex digest of raw_content bytes
    item_id TEXT NOT NULL,                     -- FK -> items.id
    payload_type TEXT NOT NULL,                -- "manual_cc", "auto_cc", "whisper", "article_text"
    language_iso1 TEXT NOT NULL,               -- Target language code
    origin_type TEXT NOT NULL,                 -- "human_transcribed", "machine_transcribed", "human_written", "machine_translated"
    format TEXT NOT NULL,                      -- "vtt", "json3", "srt", "txt", "html"
    raw_content TEXT NOT NULL,                 -- Raw unmodified transcript / text payload
    fetched_at TEXT NOT NULL,                  -- ISO-8601 UTC timestamp of fetch
    FOREIGN KEY (item_id) REFERENCES items (id) ON DELETE CASCADE
);

-- Indexes for lightning-fast queries
CREATE INDEX IF NOT EXISTS idx_sources_lang ON sources (language_iso1);
CREATE INDEX IF NOT EXISTS idx_items_source ON items (source_id);
CREATE INDEX IF NOT EXISTS idx_items_views ON items (view_count DESC);
CREATE INDEX IF NOT EXISTS idx_payloads_item ON payloads (item_id);
CREATE INDEX IF NOT EXISTS idx_payloads_type ON payloads (payload_type);
CREATE INDEX IF NOT EXISTS idx_payloads_origin ON payloads (origin_type);

-- Full-Text Search (FTS5) for instant lexical / idiom discovery across transcripts
CREATE VIRTUAL TABLE IF NOT EXISTS payloads_fts USING fts5(
    id UNINDEXED,
    item_id UNINDEXED,
    raw_content,
    content='payloads',
    content_rowid='rowid'
);

-- Triggers to keep FTS index synchronized automatically
CREATE TRIGGER IF NOT EXISTS payloads_ai AFTER INSERT ON payloads BEGIN
    INSERT INTO payloads_fts(rowid, id, item_id, raw_content)
    VALUES (new.rowid, new.id, new.item_id, new.raw_content);
END;

CREATE TRIGGER IF NOT EXISTS payloads_ad AFTER DELETE ON payloads BEGIN
    INSERT INTO payloads_fts(payloads_fts, rowid, id, item_id, raw_content)
    VALUES ('delete', old.rowid, old.id, old.item_id, old.raw_content);
END;

CREATE TRIGGER IF NOT EXISTS payloads_au AFTER UPDATE ON payloads BEGIN
    INSERT INTO payloads_fts(payloads_fts, rowid, id, item_id, raw_content)
    VALUES ('delete', old.rowid, old.id, old.item_id, old.raw_content);
    INSERT INTO payloads_fts(rowid, id, item_id, raw_content)
    VALUES (new.rowid, new.id, new.item_id, new.raw_content);
END;
