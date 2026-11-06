# Data Brochure: Raw Linguistic Vault

High-performance, immutable multi-register linguistic storage layer and collection directives.

## Architecture

```mermaid
flowchart TD
    subgraph Storage["Storage Layer (SQLite)"]
        S["sources (Channels, Feeds, Books, Sites)"]
        I["items (Videos, Episodes, Articles, Chapters)"]
        P["payloads (Raw Transcripts, Text Blobs, SRT/VTT)"]
        FTS["payloads_fts (FTS5 Inverted Index)"]
        S -->|1:N| I -->|1:N| P
        P -.-> FTS
    end

    subgraph App["Python Management Layer"]
        VM["src/vault/db.py (VaultManager)"]
        VM -->|Atomic Transactions & SHA-256 Deduplication| Storage
    end

    subgraph Directives["Agent Guidelines"]
        CP["COLLECTION_PRINCIPLES.md (6 Inviolable Rules)"]
    end

    subgraph CLI["CLI & Tests"]
        CLI_T["scripts/vault_cli.py (Status, Init, Query)"]
        TESTS["tests/unit/test_vault.py (Schema & Integrity Tests)"]
    end
```

## Features

- **Relational Schema**: Clean 3-tier hierarchy (`sources` $\rightarrow$ `items` $\rightarrow$ `payloads`) with cascade deletes and foreign key constraints.
- **Concurrency & Speed**: SQLite Write-Ahead Logging (`WAL` mode) with synchronous normal and 5000ms busy timeout for concurrent multi-agent ingestion.
- **Cryptographic Deduplication**: SHA-256 payload identification ensures zero duplicate transcript storage.
- **Full-Text Search (FTS5)**: Integrated inverted index with automatic triggers for instant phrase search across multilingual text.
- **Strict Directives**: Binding rules of engagement documented in [`COLLECTION_PRINCIPLES.md`](COLLECTION_PRINCIPLES.md).

## Quick Start

### 1. Initialize the Vault Database
```bash
python scripts/vault_cli.py init --db-path data/vault/raw_vault.db
```

### 2. View Vault Statistics
```bash
python scripts/vault_cli.py stats --db-path data/vault/raw_vault.db
```

### 3. Full-Text Search
```bash
python scripts/vault_cli.py search "keyword or phrase" --db-path data/vault/raw_vault.db
```

### 4. Running the Tests
```bash
uv run pytest tests/unit/test_vault.py -v
```
