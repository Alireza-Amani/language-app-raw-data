"""Unit and integration test suite for the Raw Linguistic Vault."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
import sys
import threading
from pathlib import Path
from typing import Generator

import pytest

from src.vault.db import VaultManager


@pytest.fixture
def temp_db_path(tmp_path: Path) -> Path:
    """Fixture providing a temporary database file path."""
    return tmp_path / "test_vault.db"


@pytest.fixture
def vault(temp_db_path: Path) -> Generator[VaultManager, None, None]:
    """Fixture providing an initialized VaultManager connected to a temp file DB."""
    vm = VaultManager(db_path=temp_db_path)
    vm.init_db()
    yield vm
    vm.close()


def test_schema_initialization_idempotent(temp_db_path: Path):
    """Verify that init_db initializes tables and can be run multiple times safely."""
    vm = VaultManager(db_path=temp_db_path)
    vm.init_db()

    # Query tables
    tables = [
        row[0]
        for row in vm.conn.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'view');"
        ).fetchall()
    ]
    assert "sources" in tables
    assert "items" in tables
    assert "payloads" in tables
    assert "payloads_fts" in tables

    # Second initialization must be completely idempotent
    vm.init_db()
    vm.close()


def test_wal_mode_and_pragmas(temp_db_path: Path):
    """Verify WAL journal mode, busy timeout, and foreign key pragmas."""
    vm = VaultManager(db_path=temp_db_path)
    vm.init_db()

    cur = vm.conn.cursor()
    journal_mode = cur.execute("PRAGMA journal_mode;").fetchone()[0]
    foreign_keys = cur.execute("PRAGMA foreign_keys;").fetchone()[0]
    busy_timeout = cur.execute("PRAGMA busy_timeout;").fetchone()[0]
    synchronous = cur.execute("PRAGMA synchronous;").fetchone()[0]

    assert journal_mode.lower() == "wal"
    assert foreign_keys == 1
    assert busy_timeout == 5000
    assert synchronous == 1  # 1 = NORMAL
    vm.close()


def test_source_and_item_crud(vault: VaultManager):
    """Verify source and item upserts, retrieval, and metadata handling."""
    # Insert source
    source_id = vault.upsert_source(
        id="yt:@Tabaghe16",
        platform="youtube",
        language_iso1="fa",
        name="طبقه ۱۶",
        url="https://youtube.com/@Tabaghe16",
        metadata={"subscribers": 150000, "category": "technology"},
    )
    assert source_id == "yt:@Tabaghe16"

    source = vault.get_source(source_id)
    assert source is not None
    assert source["name"] == "طبقه ۱۶"
    assert source["language_iso1"] == "fa"
    assert "subscribers" in source["metadata_json"]

    # Upsert with modification
    vault.upsert_source(
        id="yt:@Tabaghe16",
        platform="youtube",
        language_iso1="fa",
        name="طبقه 16 - آپدیت",
        url="https://youtube.com/@Tabaghe16",
        metadata={"subscribers": 160000},
    )
    updated_source = vault.get_source(source_id)
    assert updated_source["name"] == "طبقه 16 - آپدیت"

    # Insert items
    vault.upsert_item(
        id="yt:vid001",
        source_id="yt:@Tabaghe16",
        title="گفتگو با مهمان اول",
        url="https://youtube.com/watch?v=vid001",
        published_at="2024-01-15T12:00:00Z",
        duration_seconds=3600,
        view_count=50000,
        metadata={"tags": ["ai", "tech"]},
    )
    vault.upsert_item(
        id="yt:vid002",
        source_id="yt:@Tabaghe16",
        title="گفتگو با مهمان دوم",
        url="https://youtube.com/watch?v=vid002",
        published_at="2024-01-20T12:00:00Z",
        duration_seconds=4200,
        view_count=120000,
    )

    item1 = vault.get_item("yt:vid001")
    assert item1 is not None
    assert item1["view_count"] == 50000

    # Retrieve items by source ordered by view count
    items_by_views = vault.get_items_by_source("yt:@Tabaghe16", order_by_views=True)
    assert len(items_by_views) == 2
    assert items_by_views[0]["id"] == "yt:vid002"  # 120,000 views first


def test_foreign_key_enforcement(vault: VaultManager):
    """Verify that foreign key constraints block orphaned records and cascade deletes."""
    # Item without existing source must raise IntegrityError
    with pytest.raises(sqlite3.IntegrityError):
        vault.upsert_item(
            id="yt:orphan_vid",
            source_id="yt:non_existent_channel",
            title="Orphan Video",
            url="https://youtube.com/watch?v=orphan",
        )

    # Payload without existing item must raise IntegrityError
    with pytest.raises(sqlite3.IntegrityError):
        vault.insert_payload(
            item_id="yt:non_existent_item",
            payload_type="manual_cc",
            language_iso1="en",
            origin_type="human_transcribed",
            format="vtt",
            raw_content="WEBVTT\n00:00.000 --> 00:05.000\nHello world",
        )

    # Test cascade delete: deleting source deletes items and payloads
    vault.upsert_source("src:del", "web", "en", "Delete Test", "https://example.com")
    vault.upsert_item("item:del", "src:del", "Delete Item", "https://example.com/1")
    payload_id = vault.insert_payload(
        "item:del", "article_text", "en", "human_written", "txt", "Cascade delete text"
    )

    assert vault.get_payload(payload_id) is not None

    # Delete source
    vault.conn.execute("DELETE FROM sources WHERE id = 'src:del';")
    vault.conn.commit()

    assert vault.get_source("src:del") is None
    assert vault.get_item("item:del") is None
    assert vault.get_payload(payload_id) is None


def test_payload_sha256_deduplication(vault: VaultManager):
    """Verify SHA-256 computation and automatic deduplication of identical payloads."""
    vault.upsert_source("src:dedup", "youtube", "fa", "Dedup Source", "https://yt.com")
    vault.upsert_item("item:dedup", "src:dedup", "Dedup Video", "https://yt.com/v")

    raw_text = "سلام دنیا! این یک متن تستی برای اعتبارسنجی هش است."
    expected_hash = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()

    # First insertion
    p_id_1 = vault.insert_payload(
        item_id="item:dedup",
        payload_type="manual_cc",
        language_iso1="fa",
        origin_type="human_transcribed",
        format="vtt",
        raw_content=raw_text,
    )
    assert p_id_1 == expected_hash

    # Second insertion with identical content must return same hash and not duplicate
    p_id_2 = vault.insert_payload(
        item_id="item:dedup",
        payload_type="manual_cc",
        language_iso1="fa",
        origin_type="human_transcribed",
        format="vtt",
        raw_content=raw_text,
    )
    assert p_id_2 == expected_hash

    count = vault.conn.execute("SELECT COUNT(*) FROM payloads WHERE id = ?;", (expected_hash,)).fetchone()[0]
    assert count == 1

    # Inserting slightly different content creates a distinct payload
    diff_text = raw_text + " (تغییر یافته)"
    p_id_3 = vault.insert_payload(
        item_id="item:dedup",
        payload_type="auto_cc",
        language_iso1="fa",
        origin_type="machine_transcribed",
        format="vtt",
        raw_content=diff_text,
    )
    assert p_id_3 != expected_hash
    total_payloads = vault.conn.execute("SELECT COUNT(*) FROM payloads;").fetchone()[0]
    assert total_payloads == 2


def test_fts5_full_text_search(vault: VaultManager):
    """Verify FTS5 full-text indexing, multi-lingual phrase search, and trigger sync."""
    vault.upsert_source("src:fts", "web", "multilingual", "FTS Source", "https://fts.org")
    vault.upsert_item("item:fa", "src:fts", "Persian Article", "https://fts.org/fa")
    vault.upsert_item("item:es", "src:fts", "Spanish Article", "https://fts.org/es")

    fa_text = "در این قسمت از گفتگو، درباره هوش مصنوعی و یادگیری ماشین عمیق صحبت می‌کنیم."
    es_text = "En este episodio discutimos sobre inteligencia artificial y redes neuronales profundas."

    pid_fa = vault.insert_payload("item:fa", "article_text", "fa", "human_written", "txt", fa_text)
    pid_es = vault.insert_payload("item:es", "article_text", "es", "human_written", "txt", es_text)

    # Search for Persian phrase
    results_fa = vault.search_text("هوش مصنوعی")
    assert len(results_fa) >= 1
    assert results_fa[0]["payload_id"] == pid_fa
    assert "<b>هوش</b>" in results_fa[0]["snippet"] or "هوش مصنوعی" in results_fa[0]["snippet"]

    # Search for Spanish phrase
    results_es = vault.search_text("redes neuronales")
    assert len(results_es) >= 1
    assert results_es[0]["payload_id"] == pid_es

    # Search with language filter
    filtered = vault.search_text("inteligencia", language_iso1="es")
    assert len(filtered) == 1
    assert filtered[0]["payload_id"] == pid_es

    mismatched = vault.search_text("inteligencia", language_iso1="fa")
    assert len(mismatched) == 0

    # Test FTS deletion synchronization via triggers
    vault.conn.execute("DELETE FROM payloads WHERE id = ?;", (pid_fa,))
    vault.conn.commit()

    after_del = vault.search_text("هوش مصنوعی")
    assert len(after_del) == 0


def test_atomic_transaction_rollback(vault: VaultManager):
    """Verify that an error inside transaction() context rolls back all changes."""
    with pytest.raises(RuntimeError):
        with vault.transaction():
            vault.upsert_source(
                id="src:tx_test",
                platform="youtube",
                language_iso1="en",
                name="Rollback Channel",
                url="https://youtube.com/test",
            )
            # Deliberate failure
            raise RuntimeError("Simulation of crash during transaction")

    # Source must not exist
    assert vault.get_source("src:tx_test") is None


def test_vault_stats(vault: VaultManager):
    """Verify calculation of aggregate statistics."""
    vault.upsert_source("s1", "youtube", "fa", "YT Persian", "https://yt.com/1")
    vault.upsert_source("s2", "podcast", "en", "Podcast Eng", "https://pod.com/2")

    vault.upsert_item("i1", "s1", "Ep 1", "https://yt.com/1", duration_seconds=3600, view_count=1000)
    vault.upsert_item("i2", "s2", "Ep 2", "https://pod.com/2", duration_seconds=7200, view_count=2000)

    vault.insert_payload("i1", "manual_cc", "fa", "human_transcribed", "vtt", "متن یک")
    vault.insert_payload("i1", "auto_cc", "fa", "machine_transcribed", "vtt", "متن دو اتوماتیک")
    vault.insert_payload("i2", "whisper", "en", "machine_transcribed", "srt", "Text three english")

    stats = vault.get_vault_stats()
    assert stats["total_sources"] == 2
    assert stats["total_items"] == 2
    assert stats["total_payloads"] == 3
    assert stats["total_duration_hours"] == 3.0  # (3600 + 7200) / 3600
    assert stats["total_views"] == 3000
    assert stats["sources_by_platform"]["youtube"] == 1
    assert stats["sources_by_platform"]["podcast"] == 1
    assert stats["payloads_by_origin"]["human_transcribed"] == 1
    assert stats["payloads_by_origin"]["machine_transcribed"] == 2
    assert stats["payloads_by_language"]["fa"] == 2
    assert stats["payloads_by_language"]["en"] == 1


def test_concurrency_resilience(temp_db_path: Path):
    """Verify that multiple concurrent threads can insert without SQLite lock issues."""
    vm = VaultManager(db_path=temp_db_path)
    vm.init_db()

    vm.upsert_source("src:concurrency", "youtube", "en", "Concurrent Channel", "https://example.com")

    errors: list[Exception] = []
    num_threads = 6
    items_per_thread = 5

    def worker(worker_id: int):
        try:
            # Each worker creates its own connection to test WAL concurrency
            local_vm = VaultManager(db_path=temp_db_path)
            for j in range(items_per_thread):
                item_id = f"item:w{worker_id}_i{j}"
                local_vm.upsert_item(
                    id=item_id,
                    source_id="src:concurrency",
                    title=f"Worker {worker_id} Item {j}",
                    url=f"https://example.com/{worker_id}/{j}",
                    duration_seconds=100,
                    view_count=50,
                )
                local_vm.insert_payload(
                    item_id=item_id,
                    payload_type="manual_cc",
                    language_iso1="en",
                    origin_type="human_transcribed",
                    format="txt",
                    raw_content=f"Payload content from worker {worker_id} iteration {j}",
                )
            local_vm.close()
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0, f"Encountered concurrency errors: {errors}"

    stats = vm.get_vault_stats()
    assert stats["total_items"] == num_threads * items_per_thread
    assert stats["total_payloads"] == num_threads * items_per_thread
    vm.close()


def test_cli_execution(temp_db_path: Path):
    """Verify the CLI script commands: init, stats, and search."""
    cli_path = Path(__file__).resolve().parent.parent.parent / "scripts" / "vault_cli.py"

    # Test init via CLI
    res = subprocess.run(
        [sys.executable, str(cli_path), "--db-path", str(temp_db_path), "init"],
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0
    assert "Vault initialized successfully" in res.stdout

    # Insert test data using Python
    with VaultManager(db_path=temp_db_path) as vm:
        vm.upsert_source("cli:src", "youtube", "fa", "CLI Test", "https://cli.test")
        vm.upsert_item("cli:item", "cli:src", "CLI Video", "https://cli.test/1", duration_seconds=600, view_count=100)
        vm.insert_payload("cli:item", "manual_cc", "fa", "human_transcribed", "txt", "جستجوی عبارات ویژه برای خط فرمان")

    # Test stats via CLI
    res_stats = subprocess.run(
        [sys.executable, str(cli_path), "--db-path", str(temp_db_path), "stats"],
        capture_output=True,
        text=True,
    )
    assert res_stats.returncode == 0
    assert "RAW LINGUISTIC VAULT STATISTICS" in res_stats.stdout
    assert "Total Sources       : 1" in res_stats.stdout

    # Test stats json via CLI
    res_stats_json = subprocess.run(
        [sys.executable, str(cli_path), "--db-path", str(temp_db_path), "stats", "--json"],
        capture_output=True,
        text=True,
    )
    assert res_stats_json.returncode == 0
    assert '"total_sources": 1' in res_stats_json.stdout

    # Test search via CLI
    res_search = subprocess.run(
        [sys.executable, str(cli_path), "--db-path", str(temp_db_path), "search", "جستجوی عبارات"],
        capture_output=True,
        text=True,
    )
    assert res_search.returncode == 0
    assert "Found: 1 matches" in res_search.stdout


def test_multi_region_french_country_locale_and_metadata(vault: VaultManager):
    """Verify country_code on sources, locale and metadata_json on payloads, and filtered FTS search."""
    vault.upsert_source(
        id="yt:@innerFrench",
        platform="youtube",
        language_iso1="fr",
        country_code="FR",
        name="innerFrench",
        url="https://youtube.com/@innerFrench",
        metadata={"region": "Metropolitan France", "register": "conversational"},
    )
    vault.upsert_source(
        id="yt:@SousEcoute",
        platform="youtube",
        language_iso1="fr",
        name="Mike Ward Sous Écoute",
        url="https://youtube.com/@SousEcoute",
        metadata={"country_code": "ca", "region": "Quebec", "register": "colloquial", "interaction_type": "dialogue"},
    )
    vault.upsert_source(
        id="yt:@RTBF",
        platform="youtube",
        language_iso1="fr",
        country_code="BE",
        name="RTBF",
        url="https://youtube.com/@RTBF",
    )

    # Verify country_code normalization (explicit and extracted from metadata dict)
    src_fr = vault.get_source("yt:@innerFrench")
    src_ca = vault.get_source("yt:@SousEcoute")
    assert src_fr["country_code"] == "FR"
    assert src_ca["country_code"] == "CA"

    vault.upsert_item("yt:fr_vid1", "yt:@innerFrench", "Comprendre les Français", "https://youtube.com/watch?v=fr_vid1")
    vault.upsert_item("yt:ca_vid1", "yt:@SousEcoute", "Épisode à Montréal", "https://youtube.com/watch?v=ca_vid1")
    vault.upsert_item("yt:be_vid1", "yt:@RTBF", "Journal parlé à Bruxelles", "https://youtube.com/watch?v=be_vid1")

    pid_fr = vault.insert_payload(
        item_id="yt:fr_vid1",
        payload_type="manual_cc",
        language_iso1="fr",
        locale="fr-FR",
        origin_type="human_transcribed",
        format="vtt",
        raw_content="WEBVTT\n\n00:00:01.000 --> 00:00:04.000\nAujourd'hui nous parlons de la francophonie internationale.",
        metadata={"track_key": "fr-FR", "track_name": "Français (France)"},
    )
    pid_ca = vault.insert_payload(
        item_id="yt:ca_vid1",
        payload_type="manual_cc",
        language_iso1="fr",
        locale="fr-CA",
        origin_type="human_transcribed",
        format="vtt",
        raw_content="WEBVTT\n\n00:00:01.000 --> 00:00:04.000\nAujourd'hui au Québec on jase de la francophonie internationale.",
        metadata={"track_key": "fr-CA", "track_name": "Français (Canada)", "code_switching": ["en"]},
    )
    pid_be = vault.insert_payload(
        item_id="yt:be_vid1",
        payload_type="auto_cc",
        language_iso1="fr",
        locale="fr-BE",
        origin_type="machine_transcribed",
        format="vtt",
        raw_content="WEBVTT\n\n00:00:01.000 --> 00:00:04.000\nÀ Bruxelles on célèbre septante ans de francophonie internationale.",
        metadata={"track_key": "fr-BE", "asr_model": "youtube_asr"},
    )

    payload_ca = vault.get_payload(pid_ca)
    assert payload_ca["locale"] == "fr-CA"
    assert json.loads(payload_ca["metadata_json"])["track_name"] == "Français (Canada)"

    # Search across all French varieties
    all_fr = vault.search_text("francophonie internationale", language_iso1="fr")
    assert len(all_fr) == 3

    # Filter by country_code
    only_ca = vault.search_text("francophonie internationale", language_iso1="fr", country_code="CA")
    assert len(only_ca) == 1
    assert only_ca[0]["payload_id"] == pid_ca
    assert only_ca[0]["country_code"] == "CA"
    assert only_ca[0]["locale"] == "fr-CA"

    # Filter by locale
    only_be = vault.search_text("francophonie internationale", locale="fr-BE")
    assert len(only_be) == 1
    assert only_be[0]["payload_id"] == pid_be
    assert only_be[0]["country_code"] == "BE"

    # Verify stats breakdowns
    stats = vault.get_vault_stats()
    assert stats["sources_by_country"] == {"FR": 1, "CA": 1, "BE": 1}
    assert stats["payloads_by_locale"] == {"fr-FR": 1, "fr-CA": 1, "fr-BE": 1}


def test_legacy_schema_auto_migration(temp_db_path: Path):
    """Verify that opening a pre-existing legacy DB without country_code/locale migrates cleanly."""
    conn = sqlite3.connect(str(temp_db_path))
    conn.executescript(
        """
        CREATE TABLE sources (
            id TEXT PRIMARY KEY,
            platform TEXT NOT NULL,
            language_iso1 TEXT NOT NULL,
            name TEXT NOT NULL,
            url TEXT NOT NULL,
            metadata_json TEXT DEFAULT '{}',
            created_at TEXT NOT NULL
        );
        CREATE TABLE items (
            id TEXT PRIMARY KEY,
            source_id TEXT NOT NULL,
            title TEXT NOT NULL,
            url TEXT NOT NULL,
            published_at TEXT,
            duration_seconds INTEGER,
            view_count INTEGER,
            metadata_json TEXT DEFAULT '{}',
            created_at TEXT NOT NULL
        );
        CREATE TABLE payloads (
            id TEXT PRIMARY KEY,
            item_id TEXT NOT NULL,
            payload_type TEXT NOT NULL,
            language_iso1 TEXT NOT NULL,
            origin_type TEXT NOT NULL,
            format TEXT NOT NULL,
            raw_content TEXT NOT NULL,
            fetched_at TEXT NOT NULL
        );
        INSERT INTO sources VALUES ('yt:@Legacy', 'youtube', 'fa', 'Legacy', 'https://yt.com', '{}', '2026-11-01T00:00:00Z');
        """
    )
    conn.commit()
    conn.close()

    with VaultManager(db_path=temp_db_path) as vm:
        vm.init_db()
        legacy_src = vm.get_source("yt:@Legacy")
        assert legacy_src is not None
        assert legacy_src["country_code"] is None

        # Upsert with country_code on migrated DB
        vm.upsert_source("yt:@Legacy", "youtube", "fa", "Legacy", "https://yt.com", country_code="IR")
        assert vm.get_source("yt:@Legacy")["country_code"] == "IR"

