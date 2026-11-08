"""Unit tests for French Tri-Source Corpus Harvester & Ingestion Engine.

Validates:
1. Cryptographic Provenance: Deterministic SHA-256 payload IDs.
2. Encoding Resilience: UTF-8, CP1252 / Windows-1252, and Latin-1 decoding.
3. Temporal Cutoff Validation: timestamp < 1640995200 and date < "2022-01-01".
4. Geographic Invariance: Strictly FR, CA, BE, CH; rejection of African/Maghreb codes.
5. Stream A (Comments) Staging Ingestion: Schema, sources, items, payloads, FTS5 sync.
6. Stream B (Subtitles) Staging Ingestion: Archive extraction, CP1252 decoding, manual CC tagging.
7. Stream C (Hansard) Staging Ingestion: Chamber resolution, XML/TXT formats, transcript payloads.
8. CLI and Full Ingestion Orchestration with --limit, --force, --output-dir.
"""

from __future__ import annotations

import hashlib
import io
import json
import sqlite3
import tempfile
import zipfile
from pathlib import Path
from typing import Any, Dict, List

import pytest

from scripts.harvest_french_tri_source import (
    ALLOWED_COUNTRY_CODES,
    DEFAULT_CATALOG_PATHS,
    FORBIDDEN_AFRICAN_MAGHREB_CODES,
    PRE_2022_DATE_CUTOFF,
    PRE_2022_EPOCH_CUTOFF,
    compute_payload_id,
    decode_content_with_fallback,
    extract_subtitle_from_zip_bytes,
    harvest_stream_a,
    harvest_stream_b,
    harvest_stream_c,
    harvest_tri_source,
    resolve_french_locale,
    validate_geographic_code,
    validate_temporal_cutoff,
)
from src.vault.db import VaultManager

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


# ---------------------------------------------------------------------------
# Test Suite 1: Cryptographic Provenance (SHA-256)
# ---------------------------------------------------------------------------

class TestCryptographicProvenance:
    """Tests for payload SHA-256 hashing."""

    def test_compute_payload_id_deterministic(self):
        text = "Bonjour le monde ! Ceci est un test de hachage cryptographique."
        expected_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        actual_hash = compute_payload_id(text)
        assert actual_hash == expected_hash
        assert len(actual_hash) == 64
        assert actual_hash == actual_hash.lower()

    def test_compute_payload_id_uniqueness(self):
        text1 = "Texte numéro 1"
        text2 = "Texte numéro 2"
        assert compute_payload_id(text1) != compute_payload_id(text2)

    def test_compute_payload_id_non_string_raises(self):
        with pytest.raises(TypeError):
            compute_payload_id(12345)  # type: ignore

        with pytest.raises(TypeError):
            compute_payload_id(b"bytes content")  # type: ignore

        with pytest.raises(TypeError):
            compute_payload_id(None)  # type: ignore


# ---------------------------------------------------------------------------
# Test Suite 2: Robust Encoding Resilience (UTF-8, CP1252, Latin-1)
# ---------------------------------------------------------------------------

class TestEncodingResilience:
    """Tests for robust decoding across UTF-8, CP1252, and Latin-1."""

    def test_utf8_decoding(self):
        original = "Ça va très bien à Montréal, Québec ! 100€"
        raw_bytes = original.encode("utf-8")
        decoded = decode_content_with_fallback(raw_bytes)
        assert decoded == original

    def test_cp1252_windows1252_decoding(self):
        original = "L'évènement de l'été : « chef-d'œuvre » à 50 €"
        raw_cp1252 = original.encode("cp1252")
        # Ensure that utf-8 would fail on these bytes
        with pytest.raises(UnicodeDecodeError):
            raw_cp1252.decode("utf-8")
        # Fallback should decode cleanly
        decoded = decode_content_with_fallback(raw_cp1252)
        assert decoded == original

    def test_iso8859_1_latin1_decoding(self):
        original = "Déjeuner à Genève et Bruxelles"
        raw_latin1 = original.encode("iso-8859-1")
        decoded = decode_content_with_fallback(raw_latin1)
        assert decoded == original

    def test_corrupt_bytes_fallback_with_replacement(self):
        corrupt = b"\xff\xfe\x00\x80\x99\xff\xaa"
        decoded = decode_content_with_fallback(corrupt)
        assert isinstance(decoded, str)
        assert len(decoded) > 0


# ---------------------------------------------------------------------------
# Test Suite 3: Temporal Cutoff Validation (< 1640995200, < 2022-01-01)
# ---------------------------------------------------------------------------

class TestTemporalCutoffValidation:
    """Tests for strict pre-2022 temporal filter."""

    def test_valid_pre2022_epoch_timestamps(self):
        assert validate_temporal_cutoff(1636416000)  # Nov 2021
        assert validate_temporal_cutoff(1383955200)  # Nov 2013
        assert validate_temporal_cutoff(PRE_2022_EPOCH_CUTOFF - 1)  # 2021-12-31T23:59:59Z

    def test_post2021_epoch_cutoff_rejected(self):
        assert not validate_temporal_cutoff(PRE_2022_EPOCH_CUTOFF)  # Exact boundary 2022-01-01T00:00:00Z
        assert not validate_temporal_cutoff(PRE_2022_EPOCH_CUTOFF + 1000)
        assert not validate_temporal_cutoff(1700000000)  # 2023

    def test_valid_pre2022_date_strings(self):
        assert validate_temporal_cutoff(None, "2021-12-31")
        assert validate_temporal_cutoff(None, "2018-05-14")
        assert validate_temporal_cutoff(1500000000, "2017-07-14")

    def test_post2021_date_strings_rejected(self):
        assert not validate_temporal_cutoff(None, "2022-01-01")
        assert not validate_temporal_cutoff(None, "2022-05-20")
        assert not validate_temporal_cutoff(None, "2026-11-08")


# ---------------------------------------------------------------------------
# Test Suite 4: Geographic Invariance & Dialect Locales
# ---------------------------------------------------------------------------

class TestGeographicInvariance:
    """Tests for allowed and forbidden geographic codes."""

    def test_allowed_country_codes(self):
        for cc in ALLOWED_COUNTRY_CODES:
            assert validate_geographic_code(cc) == cc
            assert validate_geographic_code(cc.lower()) == cc

    def test_forbidden_african_maghreb_codes(self):
        for code in FORBIDDEN_AFRICAN_MAGHREB_CODES:
            with pytest.raises(ValueError, match="Forbidden African/Maghreb jurisdiction"):
                validate_geographic_code(code)

    def test_other_disallowed_country_codes(self):
        for code in ("US", "GB", "DE", "ES", "IT"):
            with pytest.raises(ValueError, match="not in allowed set"):
                validate_geographic_code(code)

    def test_locale_resolution(self):
        assert resolve_french_locale("FR") == "fr-FR"
        assert resolve_french_locale("CA") == "fr-CA"
        assert resolve_french_locale("BE") == "fr-BE"
        assert resolve_french_locale("CH") == "fr-CH"
        assert resolve_french_locale(None) == "fr-FR"


# ---------------------------------------------------------------------------
# Test Suite 5: Subtitle Archive Zip Extraction
# ---------------------------------------------------------------------------

class TestSubtitleZipExtraction:
    """Tests for in-memory subtitle extraction and format detection."""

    def test_extract_srt_from_zip(self):
        srt_content = "1\n00:00:01,000 --> 00:00:03,000\nBonjour tout le monde !\n\n"
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("movie.srt", srt_content.encode("utf-8"))
            zf.writestr("movie.nfo", "Metadata info")

        text, fmt = extract_subtitle_from_zip_bytes(buf.getvalue())
        assert fmt == "srt"
        assert "Bonjour tout le monde" in text

    def test_extract_cp1252_srt_from_zip(self):
        srt_content = "1\n00:00:01,000 --> 00:00:03,000\nC'est l'été à l'opéra !\n\n"
        raw_cp1252 = srt_content.encode("cp1252")
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("sub.srt", raw_cp1252)

        text, fmt = extract_subtitle_from_zip_bytes(buf.getvalue())
        assert fmt == "srt"
        assert "C'est l'été à l'opéra" in text

    def test_zip_without_subtitle_raises(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("readme.txt", "No subtitles here")

        with pytest.raises(ValueError, match="no .srt or .vtt"):
            extract_subtitle_from_zip_bytes(buf.getvalue())


# ---------------------------------------------------------------------------
# Test Suite 6: Stream A (Comments) Staging Ingestion
# ---------------------------------------------------------------------------

class TestStreamAIngestion:
    """Tests for Stream A comments catalog ingestion into staging SQLite."""

    @pytest.fixture
    def sample_comments_catalog(self, tmp_path: Path) -> Path:
        cat_file = tmp_path / "sample_comments_catalog.json"
        data = {
            "generated_at_utc": "2026-11-08T12:00:00Z",
            "language_iso1": "fr",
            "stream_modality": "comment_thread",
            "allowed_country_codes": ["BE", "CA", "CH", "FR"],
            "total_items": 2,
            "items": [
                {
                    "item_id": "yt_comment_test01",
                    "title": "Vidéo Test France",
                    "country_code": "FR",
                    "region": "Metropolitan France",
                    "platform": "youtube",
                    "payload_type": "comment_thread",
                    "origin_type": "human_written",
                    "format": "json",
                    "source_url": "https://www.youtube.com/watch?v=test01",
                    "date": "2019-06-15",
                    "metadata": {
                        "channel_id": "UC_channel_fr",
                        "channel_title": "Chaîne France",
                        "uploader_id": "@ChaineFR",
                        "video_id": "test01",
                        "view_count": 50000,
                        "comments_count": 2,
                        "comments": [
                            {
                                "comment_id": "comm_01",
                                "id": "comm_01",
                                "text": "Superbe émission, merci beaucoup !",
                                "author": "@user1",
                                "timestamp": 1560600000,
                                "date": "2019-06-15",
                            },
                            {
                                "comment_id": "comm_02",
                                "id": "comm_02",
                                "text": "Très bonne analyse sociolinguistique.",
                                "author": "@user2",
                                "timestamp": 1560610000,
                                "date": "2019-06-15",
                            },
                        ],
                    },
                },
                {
                    "item_id": "yt_comment_test02",
                    "title": "Vidéo Test Québec avec un commentaire post-2021",
                    "country_code": "CA",
                    "region": "Québec",
                    "platform": "youtube",
                    "payload_type": "comment_thread",
                    "origin_type": "human_written",
                    "format": "json",
                    "source_url": "https://www.youtube.com/watch?v=test02",
                    "date": "2020-03-10",
                    "metadata": {
                        "channel_id": "UC_channel_qc",
                        "channel_title": "Chaîne Québec",
                        "uploader_id": "@ChaineQC",
                        "video_id": "test02",
                        "view_count": 12000,
                        "comments_count": 2,
                        "comments": [
                            {
                                "comment_id": "comm_valid",
                                "id": "comm_valid",
                                "text": "Vraiment bien fait ce topo québécois.",
                                "author": "@userQC1",
                                "timestamp": 1583840000,
                                "date": "2020-03-10",
                            },
                            {
                                "comment_id": "comm_post_2022",
                                "id": "comm_post_2022",
                                "text": "Commentaire post-2022 illégal !",
                                "author": "@userTroll",
                                "timestamp": 1672531200,  # 2023-01-01
                                "date": "2023-01-01",
                            },
                        ],
                    },
                },
            ],
        }
        with open(cat_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        return cat_file

    def test_harvest_stream_a_ingestion(self, sample_comments_catalog: Path, tmp_path: Path):
        staging_db = tmp_path / "shard_stream_a.db"
        stats = harvest_stream_a(sample_comments_catalog, staging_db)

        assert stats["stream"] == "A"
        assert stats["items_upserted"] == 2
        assert stats["payloads_inserted"] == 2
        # Exactly 3 valid pre-2022 comments ingested; 1 post-2022 comment filtered out
        assert stats["comments_ingested"] == 3

        # Direct database assertion
        conn = sqlite3.connect(str(staging_db))
        conn.row_factory = sqlite3.Row

        # 1. Foreign keys
        fks = conn.execute("PRAGMA foreign_key_check;").fetchall()
        assert len(fks) == 0

        # 2. Sources table
        sources = conn.execute("SELECT * FROM sources;").fetchall()
        assert len(sources) == 2
        source_ids = {s["id"] for s in sources}
        assert "yt:@ChaineFR" in source_ids
        assert "yt:@ChaineQC" in source_ids
        for s in sources:
            assert s["platform"] == "youtube"
            assert s["language_iso1"] == "fr"
            assert s["country_code"] in {"FR", "CA"}

        # 3. Items table
        items = conn.execute("SELECT * FROM items;").fetchall()
        assert len(items) == 2
        item_ids = {i["id"] for i in items}
        assert "yt_comment_test01" in item_ids
        assert "yt_comment_test02" in item_ids

        # 4. Payloads table
        payloads = conn.execute("SELECT * FROM payloads;").fetchall()
        assert len(payloads) == 2
        for p in payloads:
            assert p["payload_type"] == "comment_thread"
            assert p["origin_type"] == "human_written"
            assert p["format"] == "json"
            assert p["language_iso1"] == "fr"
            assert p["locale"] in {"fr-FR", "fr-CA"}
            # Verify cryptographic hash
            expected_id = hashlib.sha256(p["raw_content"].encode("utf-8")).hexdigest()
            assert p["id"] == expected_id

        # Verify filtered comment payload content for item test02
        p2 = conn.execute("SELECT * FROM payloads WHERE item_id = 'yt_comment_test02';").fetchone()
        comments_in_p2 = json.loads(p2["raw_content"])
        assert len(comments_in_p2) == 1
        assert comments_in_p2[0]["comment_id"] == "comm_valid"
        assert "comm_post_2022" not in p2["raw_content"]

        # 5. FTS5 Virtual Table
        fts_rows = conn.execute("SELECT count(*) FROM payloads_fts;").fetchone()[0]
        assert fts_rows == 2

        fts_search = conn.execute(
            "SELECT item_id FROM payloads_fts WHERE payloads_fts MATCH 'sociolinguistique';"
        ).fetchall()
        assert len(fts_search) == 1
        assert fts_search[0][0] == "yt_comment_test01"

        conn.close()


# ---------------------------------------------------------------------------
# Test Suite 7: Stream B (Cinema Subtitles) Staging Ingestion
# ---------------------------------------------------------------------------

class TestStreamBIngestion:
    """Tests for Stream B cinema subtitles catalog ingestion into staging SQLite."""

    @pytest.fixture
    def sample_subtitles_setup(self, tmp_path: Path) -> Tuple[Path, Path]:
        cat_file = tmp_path / "sample_cinema_catalog.json"
        cache_dir = tmp_path / "cache"
        sub_cache_dir = cache_dir / "subtitles"
        sub_cache_dir.mkdir(parents=True, exist_ok=True)

        # Create 2 sample subtitle files in cache: 1 UTF-8, 1 CP1252
        srt_utf8 = "1\n00:00:01,000 --> 00:00:03,000\nBienvenue dans le cinéma québécois.\n\n"
        srt_cp1252 = "1\n00:00:01,000 --> 00:00:03,000\nLa haine attire la haine, mon frère.\n\n"

        (sub_cache_dir / "3283706.srt").write_bytes(srt_cp1252.encode("cp1252"))
        (sub_cache_dir / "3386777.srt").write_bytes(srt_utf8.encode("utf-8"))

        data = {
            "generated_at_utc": "2026-11-08T12:00:00Z",
            "language_iso1": "fr",
            "stream_modality": "manual_cc",
            "allowed_country_codes": ["BE", "CA", "CH", "FR"],
            "total_items": 2,
            "items": [
                {
                    "item_id": "sub_opensubtitles_3283706",
                    "title": "La Haine (1995)",
                    "country_code": "FR",
                    "region": "Metropolitan France",
                    "platform": "subtitle",
                    "payload_type": "manual_cc",
                    "origin_type": "human_transcribed",
                    "format": "srt",
                    "source_url": "https://www.opensubtitles.org/subtitles/3283706",
                    "date": "1995-05-31",
                    "metadata": {
                        "subtitle_id": "3283706",
                        "original_language": "fr",
                        "year": 1995,
                        "director": "Mathieu Kassovitz",
                        "download_url": "https://dl.opensubtitles.org/en/download/sub/3283706",
                    },
                },
                {
                    "item_id": "sub_opensubtitles_3386777",
                    "title": "C.R.A.Z.Y. (2005)",
                    "country_code": "CA",
                    "region": "Québec",
                    "platform": "subtitle",
                    "payload_type": "manual_cc",
                    "origin_type": "human_transcribed",
                    "format": "srt",
                    "source_url": "https://www.opensubtitles.org/subtitles/3386777",
                    "date": "2005-05-27",
                    "metadata": {
                        "subtitle_id": "3386777",
                        "original_language": "fr",
                        "year": 2005,
                        "director": "Jean-Marc Vallée",
                        "download_url": "https://dl.opensubtitles.org/en/download/sub/3386777",
                    },
                },
            ],
        }
        with open(cat_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        return cat_file, cache_dir

    def test_harvest_stream_b_ingestion(self, sample_subtitles_setup: Tuple[Path, Path], tmp_path: Path):
        cat_file, cache_dir = sample_subtitles_setup
        staging_db = tmp_path / "shard_stream_b.db"

        stats = harvest_stream_b(cat_file, staging_db, cache_dir=cache_dir)
        assert stats["stream"] == "B"
        assert stats["items_upserted"] == 2
        assert stats["payloads_inserted"] == 2

        conn = sqlite3.connect(str(staging_db))
        conn.row_factory = sqlite3.Row

        # Foreign keys
        assert len(conn.execute("PRAGMA foreign_key_check;").fetchall()) == 0

        # Sources
        sources = conn.execute("SELECT * FROM sources;").fetchall()
        assert len(sources) == 2
        s_map = {s["id"]: s for s in sources}
        assert "subtitle:opensubtitles_fr" in s_map
        assert "subtitle:opensubtitles_ca" in s_map
        for s in sources:
            assert s["platform"] == "subtitle"
            assert s["language_iso1"] == "fr"

        # Payloads
        payloads = conn.execute("SELECT * FROM payloads;").fetchall()
        assert len(payloads) == 2
        for p in payloads:
            assert p["payload_type"] == "manual_cc"
            assert p["origin_type"] == "human_transcribed"
            assert p["format"] == "srt"
            assert p["locale"] in {"fr-FR", "fr-CA"}
            expected_id = hashlib.sha256(p["raw_content"].encode("utf-8")).hexdigest()
            assert p["id"] == expected_id

        # Verify CP1252 text was cleanly converted to UTF-8
        p_haine = conn.execute("SELECT * FROM payloads WHERE item_id = 'sub_opensubtitles_3283706';").fetchone()
        assert "La haine attire la haine, mon frère." in p_haine["raw_content"]

        conn.close()


# ---------------------------------------------------------------------------
# Test Suite 8: Stream C (Hansard Debates) Staging Ingestion
# ---------------------------------------------------------------------------

class TestStreamCIngestion:
    """Tests for Stream C parliamentary debates ingestion into staging SQLite."""

    @pytest.fixture
    def sample_hansard_setup(self, tmp_path: Path) -> Tuple[Path, Path]:
        cat_file = tmp_path / "sample_hansard_catalog.json"
        cache_dir = tmp_path / "cache"
        hansard_cache_dir = cache_dir / "hansard"
        hansard_cache_dir.mkdir(parents=True, exist_ok=True)

        xml_content = (
            '<?xml version="1.0" encoding="utf-8"?>\n'
            '<Hansard><Intervention><PersonSpeaking>M. le Président</PersonSpeaking>'
            '<Content><ParaText>La séance est ouverte.</ParaText></Content></Intervention></Hansard>'
        )
        txt_content = (
            "Assemblée nationale du Québec - Commission des institutions\n"
            "M. le Président : Nous reprenons nos travaux sur le projet de loi.\n"
        )

        (hansard_cache_dir / "ca_commons_test.xml").write_text(xml_content, encoding="utf-8")
        (hansard_cache_dir / "ca_qc_assnat_test.txt").write_text(txt_content, encoding="utf-8")

        data = {
            "generated_at_utc": "2026-11-08T12:00:00Z",
            "language_iso1": "fr",
            "stream_modality": "parliament_transcript",
            "allowed_country_codes": ["BE", "CA", "CH", "FR"],
            "total_items": 2,
            "items": [
                {
                    "item_id": "ca_commons_test",
                    "title": "Chambre des communes - Séance 100",
                    "country_code": "CA",
                    "region": "Federal",
                    "platform": "web",
                    "payload_type": "parliament_transcript",
                    "origin_type": "human_transcribed",
                    "format": "xml",
                    "source_url": "https://www.ourcommons.ca/hansard/100",
                    "date": "2021-05-10",
                    "metadata": {
                        "chamber": "Chambre des communes du Canada",
                        "session": "43-2",
                        "sitting_number": "100",
                        "download_url": "https://www.ourcommons.ca/xml/100.xml",
                    },
                },
                {
                    "item_id": "ca_qc_assnat_test",
                    "title": "Assemblée nationale du Québec - Séance 42",
                    "country_code": "CA",
                    "region": "Québec",
                    "platform": "web",
                    "payload_type": "parliament_transcript",
                    "origin_type": "human_transcribed",
                    "format": "txt",
                    "source_url": "https://www.assnat.qc.ca/hansard/42",
                    "date": "2021-04-12",
                    "metadata": {
                        "chamber": "Assemblée nationale du Québec",
                        "session": "42-1",
                        "sitting_number": "42",
                        "download_url": "https://www.assnat.qc.ca/hansard/42.html",
                    },
                },
            ],
        }
        with open(cat_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        return cat_file, cache_dir

    def test_harvest_stream_c_ingestion(self, sample_hansard_setup: Tuple[Path, Path], tmp_path: Path):
        cat_file, cache_dir = sample_hansard_setup
        staging_db = tmp_path / "shard_stream_c.db"

        stats = harvest_stream_c(cat_file, staging_db, cache_dir=cache_dir)
        assert stats["stream"] == "C"
        assert stats["items_upserted"] == 2
        assert stats["payloads_inserted"] == 2

        conn = sqlite3.connect(str(staging_db))
        conn.row_factory = sqlite3.Row

        assert len(conn.execute("PRAGMA foreign_key_check;").fetchall()) == 0

        # Sources
        sources = conn.execute("SELECT * FROM sources;").fetchall()
        assert len(sources) == 2
        s_map = {s["id"]: s for s in sources}
        assert "web:ca_commons" in s_map
        assert "web:assnat_qc" in s_map
        for s in sources:
            assert s["platform"] == "web"
            assert s["language_iso1"] == "fr"

        # Payloads
        payloads = conn.execute("SELECT * FROM payloads;").fetchall()
        assert len(payloads) == 2
        for p in payloads:
            assert p["payload_type"] == "parliament_transcript"
            assert p["origin_type"] == "human_transcribed"
            assert p["format"] in {"xml", "txt"}
            assert p["locale"] == "fr-CA"
            expected_id = hashlib.sha256(p["raw_content"].encode("utf-8")).hexdigest()
            assert p["id"] == expected_id

        # FTS5 search
        fts_search = conn.execute(
            "SELECT item_id FROM payloads_fts WHERE payloads_fts MATCH 'séance';"
        ).fetchall()
        assert len(fts_search) == 1
        assert fts_search[0][0] == "ca_commons_test"

        conn.close()

    def test_harvest_stream_c_cached_item_without_source_url_regression(self, tmp_path: Path):
        """Regression test for UnboundLocalError in harvest_stream_c:

        Verifies that an item with pre-existing cached transcript and NO root source_url
        ingests cleanly without UnboundLocalError and properly falls back to metadata download_url.
        """
        cache_dir = tmp_path / "cache"
        hansard_cache_dir = cache_dir / "hansard"
        hansard_cache_dir.mkdir(parents=True, exist_ok=True)

        item_id = "ca_commons_sitting_cached_no_source_url"
        xml_content = (
            '<?xml version="1.0" encoding="utf-8"?>\n'
            '<Hansard><Intervention><PersonSpeaking>L’honorable député</PersonSpeaking>'
            '<Content><ParaText>Débat parlementaire officiel en français sans source_url.</ParaText></Content>'
            '</Intervention></Hansard>'
        )
        (hansard_cache_dir / f"{item_id}.xml").write_text(xml_content, encoding="utf-8")

        cat_file = tmp_path / "regression_hansard_catalog.json"
        cat_data = {
            "generated_at_utc": "2026-11-08T15:10:00Z",
            "language_iso1": "fr",
            "stream_modality": "parliament_transcript",
            "allowed_country_codes": ["CA"],
            "total_items": 1,
            "items": [
                {
                    "item_id": item_id,
                    "title": "Chambre des communes - Séance Cache Regression",
                    "country_code": "CA",
                    "region": "Federal",
                    "platform": "web",
                    "payload_type": "parliament_transcript",
                    "origin_type": "human_transcribed",
                    "format": "xml",
                    # Explicitly omit source_url
                    "date": "2021-06-01",
                    "metadata": {
                        "chamber": "Chambre des communes du Canada",
                        "session": "43-2",
                        "sitting_number": "115",
                        "download_url": "https://www.ourcommons.ca/Content/House/432/Debates/115/HAN115-F.XML",
                    },
                }
            ],
        }
        cat_file.write_text(json.dumps(cat_data), encoding="utf-8")

        staging_db = tmp_path / "shard_stream_c_regression.db"
        # Must execute cleanly without UnboundLocalError
        stats = harvest_stream_c(cat_file, staging_db, cache_dir=cache_dir)
        assert stats["stream"] == "C"
        assert stats["items_upserted"] == 1
        assert stats["payloads_inserted"] == 1

        conn = sqlite3.connect(str(staging_db))
        conn.row_factory = sqlite3.Row

        item_row = conn.execute("SELECT * FROM items WHERE id = ?", (item_id,)).fetchone()
        assert item_row is not None
        # url should fall back to download_url
        assert item_row["url"] == "https://www.ourcommons.ca/Content/House/432/Debates/115/HAN115-F.XML"

        payload_row = conn.execute("SELECT * FROM payloads WHERE item_id = ?", (item_id,)).fetchone()
        assert payload_row is not None
        assert payload_row["id"] == hashlib.sha256(xml_content.encode("utf-8")).hexdigest()
        assert "Débat parlementaire officiel" in payload_row["raw_content"]

        conn.close()


# ---------------------------------------------------------------------------
# Test Suite 9: High-Level Orchestration API & Real Catalog Compatibility
# ---------------------------------------------------------------------------

class TestTriSourceOrchestration:
    """Tests for multi-stream orchestration and real catalog schema compliance."""

    def test_real_catalogs_exist_and_accessible(self):
        for s, p in DEFAULT_CATALOG_PATHS.items():
            assert p.is_file(), f"Expected catalog for Stream {s} at {p} does not exist!"

    def test_harvest_tri_source_with_limits(self, tmp_path: Path):
        output_dir = tmp_path / "staging"
        results = harvest_tri_source(
            stream="all",
            output_dir=output_dir,
            limit=2,
            force=True,
        )

        assert "A" in results
        assert "B" in results
        assert "C" in results

        assert (output_dir / "shard_stream_a.db").is_file()
        assert (output_dir / "shard_stream_b.db").is_file()
        assert (output_dir / "shard_stream_c.db").is_file()

        for s in ("a", "b", "c"):
            db_path = output_dir / f"shard_stream_{s}.db"
            conn = sqlite3.connect(str(db_path))
            # Schema tables must exist
            tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()}
            assert {"sources", "items", "payloads", "payloads_fts"}.issubset(tables)
            # Integrity check
            assert conn.execute("PRAGMA integrity_check;").fetchone()[0] == "ok"
            # Foreign keys check
            assert len(conn.execute("PRAGMA foreign_key_check;").fetchall()) == 0
            conn.close()

    def test_invalid_stream_name_raises(self, tmp_path: Path):
        with pytest.raises(ValueError, match="Invalid stream"):
            harvest_tri_source(stream="INVALID", output_dir=tmp_path)

    def test_harvest_stream_a_empty_comments_handled(self, tmp_path: Path):
        cat_file = tmp_path / "empty_comments_catalog.json"
        cat_data = {
            "generated_at_utc": "2026-11-08T12:00:00Z",
            "language_iso1": "fr",
            "stream_modality": "comment_thread",
            "allowed_country_codes": ["FR"],
            "total_items": 1,
            "items": [
                {
                    "item_id": "yt_empty",
                    "title": "Empty Comments Video",
                    "country_code": "FR",
                    "region": "Metropolitan France",
                    "platform": "youtube",
                    "payload_type": "comment_thread",
                    "origin_type": "human_written",
                    "format": "json",
                    "source_url": "https://www.youtube.com/watch?v=empty",
                    "date": "2020-01-01",
                    "metadata": {"comments": []},
                }
            ],
        }
        cat_file.write_text(json.dumps(cat_data), encoding="utf-8")
        staging_db = tmp_path / "shard_empty.db"
        stats = harvest_stream_a(cat_file, staging_db)
        assert stats["items_upserted"] == 0
        assert stats["payloads_inserted"] == 0

    def test_harvest_forbidden_country_in_item_raises(self, tmp_path: Path):
        cat_file = tmp_path / "forbidden_geo_catalog.json"
        cat_data = {
            "generated_at_utc": "2026-11-08T12:00:00Z",
            "language_iso1": "fr",
            "stream_modality": "comment_thread",
            "allowed_country_codes": ["FR"],
            "total_items": 1,
            "items": [
                {
                    "item_id": "yt_forbidden",
                    "title": "Forbidden Video",
                    "country_code": "DZ",  # Forbidden African/Maghreb code
                    "region": "North Africa",
                    "platform": "youtube",
                    "payload_type": "comment_thread",
                    "origin_type": "human_written",
                    "format": "json",
                    "source_url": "https://www.youtube.com/watch?v=forbidden",
                    "date": "2020-01-01",
                    "metadata": {"comments": [{"text": "bonjour", "timestamp": 1500000000}]},
                }
            ],
        }
        cat_file.write_text(json.dumps(cat_data), encoding="utf-8")
        staging_db = tmp_path / "shard_forbidden.db"
        with pytest.raises(ValueError, match="Forbidden African/Maghreb jurisdiction"):
            harvest_stream_a(cat_file, staging_db)

    def test_harvest_missing_catalog_raises(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            harvest_stream_a(tmp_path / "non_existent.json", tmp_path / "out.db")
        with pytest.raises(FileNotFoundError):
            harvest_stream_b(tmp_path / "non_existent.json", tmp_path / "out.db")
        with pytest.raises(FileNotFoundError):
            harvest_stream_c(tmp_path / "non_existent.json", tmp_path / "out.db")
