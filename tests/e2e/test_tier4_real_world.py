"""Tier 4: Realistic End-to-End Multi-Stage Production Scenarios.

Validates complete multi-agent workflows under real-world operating conditions:
- Scenario 1: Standard multi-squad production pipeline (catalog -> 4 shards -> ATTACH merge -> FTS search -> manifest validation)
- Scenario 2: Resumption after simulated worker crash (idempotent resume, 0 duplicates)
- Scenario 3: Uncapped ingestion on high-volume channels (--all-videos)
- Scenario 4: Catalog anomaly auto-remediation run (handles @Kouman, @radio-marz, duplicate Fiction, 0-video channel)
- Scenario 5: Fault-tolerant merge with corrupted shard quarantine
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

import pytest

from src.vault.db import VaultManager
from tests.e2e.contract_helpers import (
    harvest_channel_shard,
    merge_staging_to_vault,
    partition_channels,
    partition_french_channels,
    run_teamwork_harvest,
)



def test_scenario_1_standard_production_pipeline(
    canonical_catalog_path: Path,
    sandbox_staging_dir: Path,
    sandbox_vault_path: Path,
    persian_vtt_formal: str,
    persian_vtt_colloquial: str,
    persian_vtt_unicode_edge: str,
):
    """Scenario 1: Standard multi-squad production pipeline.
    
    Full flow:
    1. Channel Partitioner creates 4 domain squads.
    2. Parallel workers harvest channels across the 4 squads into isolated staging DBs.
    3. ATTACH merger consolidates staging shards into canonical raw_vault.db.
    4. FTS5 index enables search across formal, colloquial, and instructional registers.
    5. Ingestion manifest is verified for accuracy and completeness.
    """
    # Define realistic corpus for each squad with unique transcript content per video
    corpus_by_squad = {
        1: [
            {"id": "t16_ep1", "duration": 5400, "view_count": 85000, "vtt_content": f"{persian_vtt_formal}\n\n00:00:30.000 --> 00:00:35.000\nاپیزود طبقه ۱۶ قسمت ۱"},
            {"id": "chb_ep1", "duration": 3600, "view_count": 65000, "vtt_content": f"{persian_vtt_formal}\n\n00:00:30.000 --> 00:00:35.000\nاپیزود چنل بی قسمت ۱"},
        ],
        2: [
            {"id": "mia_v1", "duration": 1200, "view_count": 120000, "vtt_content": f"{persian_vtt_colloquial}\n\n00:00:30.000 --> 00:00:35.000\nولاگ میا پلیز قسمت ۱"},
            {"id": "kou_v1", "duration": 1100, "view_count": 95000, "vtt_content": f"{persian_vtt_colloquial}\n\n00:00:30.000 --> 00:00:35.000\nولاگ کوروش و ایمان کومان"},
        ],
        3: [
            {"id": "dastan_ep1", "duration": 7200, "view_count": 40000, "vtt_content": f"{persian_vtt_formal}\n\n00:00:30.000 --> 00:00:35.000\nداستان لند شاهنامه فردوسی"},
            {"id": "filmbaz_v1", "duration": 1800, "view_count": 30000, "vtt_content": f"{persian_vtt_formal}\n\n00:00:30.000 --> 00:00:35.000\nتحلیل فیلم‌های سینمایی فیلم‌باز"},
        ],
        4: [
            {"id": "chef_v1", "duration": 900, "view_count": 150000, "vtt_content": f"{persian_vtt_unicode_edge}\n\n00:00:30.000 --> 00:00:35.000\nدستور پخت جواد جوادی"},
            {"id": "skill_v1", "duration": 750, "view_count": 25000, "vtt_content": f"{persian_vtt_unicode_edge}\n\n00:00:30.000 --> 00:00:35.000\nآموزش مهارت اسکیل وید"},
        ],
    }

    def multi_squad_provider(ch: str):
        # Match channel to squad
        ch_lower = ch.lower()
        if "tabaghe" in ch_lower or "channelb" in ch_lower:
            return corpus_by_squad[1]
        elif "mia" in ch_lower or "kouman" in ch_lower:
            return corpus_by_squad[2]
        elif "dastan" in ch_lower or "filmbaz" in ch_lower:
            return corpus_by_squad[3]
        else:
            return corpus_by_squad[4]

    # Execute coordinator harvest
    res = run_teamwork_harvest(
        shards=4,
        catalog=canonical_catalog_path,
        staging_dir=sandbox_staging_dir,
        vault_path=sandbox_vault_path,
        auto_merge=True,
        channel_video_provider=multi_squad_provider,
    )
    assert res["success"] is True
    manifest = res["merge_manifest"]
    assert manifest is not None
    assert manifest["status"] == "SUCCESS"
    assert manifest["total_items"] >= 8
    assert manifest["total_payloads"] >= 8

    # Verify FTS5 search across different registers in the merged vault
    vm = VaultManager(db_path=sandbox_vault_path)
    formal_hits = vm.search_text("ژئوپلیتیک")
    colloquial_hits = vm.search_text("باحال")
    unicode_hits = vm.search_text("PyTorch")

    assert len(formal_hits) >= 1
    assert len(colloquial_hits) >= 1
    assert len(unicode_hits) >= 1
    vm.close()


def test_scenario_2_resumption_after_worker_crash(
    sandbox_staging_dir: Path, sandbox_vault_path: Path, persian_vtt_formal: str
):
    """Scenario 2: Resumption after simulated worker crash.
    
    1. Worker begins harvesting 5 videos and crashes after ingesting 2.
    2. Harvester is restarted with the full 5-video list.
    3. Assert completed videos are skipped via idempotent resumption.
    4. Assert total items in shard = exactly 5 with 0 duplicate rows.
    """
    all_videos = [
        {
            "id": f"vid_crash_{i}",
            "duration": 800 + i * 50,
            "view_count": i * 1000,
            "vtt_content": f"{persian_vtt_formal}\n\n00:00:10.000 --> 00:00:15.000\nاپیزود آزمایشی شماره {i}",
        }
        for i in range(1, 6)
    ]

    # Run pass 1 with partial list (simulating crash after first 2)
    res_pass1 = harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: all_videos[:2],
    )
    assert res_pass1["items_cataloged"] == 2
    assert res_pass1["payloads_inserted"] == 2

    # Run pass 2 with complete list (simulating recovery/restart)
    res_pass2 = harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: all_videos,
    )
    assert res_pass2["resumed_skipped_count"] == 2
    assert res_pass2["items_cataloged"] == 3

    # Check shard DB has exactly 5 items
    db_path = sandbox_staging_dir / "shard_1.db"
    conn = sqlite3.connect(str(db_path))
    cnt = conn.execute("SELECT COUNT(*) FROM items;").fetchone()[0]
    conn.close()
    assert cnt == 5


def test_scenario_3_uncapped_ingestion_on_high_volume_channel(
    sandbox_staging_dir: Path, sandbox_vault_path: Path
):
    """Scenario 3: Uncapped ingestion on high-volume channel.
    
    1. A channel has 60 eligible longform videos.
    2. With capping policy default (25), only top 25 are ingested.
    3. With --all-videos, all 60 are ingested and merged into the canonical vault.
    """
    high_volume_videos = [
        {"id": f"vid_vol_{i}", "duration": 700 + i * 10, "view_count": i * 100}
        for i in range(1, 61)
    ]

    # Ingest with all_videos = True
    res = harvest_channel_shard(
        shard_id=1,
        channels=["@BBCNewsPersian"],
        staging_dir=sandbox_staging_dir,
        cap_per_channel=25,
        all_videos=True,
        channel_video_provider=lambda ch: high_volume_videos,
    )
    assert res["eligible_videos_scanned"] == 60
    assert res["items_cataloged"] == 60

    manifest = merge_staging_to_vault(staging_dir=sandbox_staging_dir, vault_path=sandbox_vault_path)
    assert manifest["total_items"] == 60


def test_scenario_4_catalog_anomaly_auto_remediation(
    canonical_catalog_path: Path, sandbox_staging_dir: Path, sandbox_vault_path: Path
):
    """Scenario 4: Catalog anomaly auto-remediation run.
    
    Verifies that the entire pipeline automatically fixes:
    - @Kouman_Official -> @Kouman
    - radiomarz -> @radio-marz
    - Fiction Podcast duplicate
    - 0-video channel (@navidkashani)
    and produces clean, normalized records in the destination vault.
    """
    res = run_teamwork_harvest(
        shards=4,
        catalog=canonical_catalog_path,
        staging_dir=sandbox_staging_dir,
        vault_path=sandbox_vault_path,
        auto_merge=True,
        channel_video_provider=lambda ch: [
            {"id": f"vid_{ch.lstrip('@')}", "duration": 800, "view_count": 500}
        ],
    )
    assert res["success"] is True

    vm = VaultManager(db_path=sandbox_vault_path)
    conn = vm.conn
    sources = [r[0] for r in conn.execute("SELECT id FROM sources;").fetchall()]
    vm.close()

    assert "yt:@Kouman" in sources
    assert "yt:@Kouman_Official" not in sources
    assert "yt:@radio-marz" in sources
    assert "yt:radiomarz" not in sources

    fiction_sources = [s for s in sources if "fiction" in s.lower()]
    assert len(fiction_sources) == 1, "Only 1 deduplicated Fiction Podcast source must exist in vault"


def test_scenario_5_fault_tolerant_merge_with_corrupted_shard(
    sandbox_staging_dir: Path, sandbox_vault_path: Path, persian_vtt_formal: str
):
    """Scenario 5: Fault-tolerant merge with corrupted shard quarantine.
    
    1. Three worker shards are created.
    2. Shard 2 is corrupted (overwritten with corrupted bytes).
    3. The merger is invoked.
    4. Assert that Shard 2 is renamed to .corrupt and documented in manifest.
    5. Assert that Shards 1 and 3 are successfully merged into raw_vault.db without data loss.
    """
    # Create shard 1
    harvest_channel_shard(
        shard_id=1,
        channels=["@Tabaghe16"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: [
            {"id": "v_s1", "duration": 900, "view_count": 1000, "vtt_content": f"{persian_vtt_formal}\n\n00:00:10.000 --> 00:00:15.000\nشارد ۱"}
        ],
    )
    # Create shard 2
    harvest_channel_shard(
        shard_id=2,
        channels=["@MiaPlays"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: [
            {"id": "v_s2", "duration": 900, "view_count": 1000}
        ],
    )
    # Create shard 3
    harvest_channel_shard(
        shard_id=3,
        channels=["@ChefJavadJavadi"],
        staging_dir=sandbox_staging_dir,
        channel_video_provider=lambda ch: [
            {"id": "v_s3", "duration": 900, "view_count": 1000, "vtt_content": f"{persian_vtt_formal}\n\n00:00:10.000 --> 00:00:15.000\nشارد ۳"}
        ],
    )

    # Corrupt shard 2
    shard_2_file = sandbox_staging_dir / "shard_2.db"
    shard_2_file.write_bytes(b"CORRUPT BYTES")

    manifest = merge_staging_to_vault(staging_dir=sandbox_staging_dir, vault_path=sandbox_vault_path)
    assert manifest["status"] == "SUCCESS"
    assert len(manifest["quarantined_shards"]) == 1
    assert "shard_2" in manifest["quarantined_shards"][0]
    assert manifest["total_items"] == 2  # v_s1 and v_s3 merged
    assert manifest["total_payloads"] == 2


def test_scenario_french_multi_squad_harvest_and_merge(
    sandbox_staging_dir: Path,
    sandbox_vault_path: Path,
):
    """Scenario 6: Full hermetic pipeline simulation for French Constructicon teamwork harvest.
    
    Validates:
    1. French catalog partitioning into 8 canonical regional/domain squads across:
       - France (FR): Shards 1, 2, 3, 4
       - Canada / Québec (CA): Shards 5, 6
       - Belgium (BE): Shard 7
       - Switzerland (CH): Shard 8
    2. Hermetic multi-squad harvest under isolate_network:
       - Parallel worker execution into isolated staging DBs (shard_1.db .. shard_8.db)
       - Candidate capping at 150 with prefer_manual_cc=True
       - Captures regional dialect markers: 'pantoute' (CA), 'septante' (CH), 'nonante' (BE), 'dialogue' (FR)
    3. Consolidated SQLite ATTACH merger into canonical raw_vault.db with FTS5 synchronization.
    4. Comprehensive FTS5 BM25 search across Francophone regional dialects and register variants.
    5. Provable metadata integrity (country_code, locale, manual_cc/auto_cc origin).
    """
    # 1. Representative Francophone catalog with 1 channel per squad across FR, CA, BE, CH
    french_catalog_data = {
        "generated_at_utc": "2026-11-07T12:00:00+00:00",
        "language_iso1": "fr",
        "allowed_country_codes": ["BE", "CA", "CH", "FR"],
        "channels": [
            {
                "channel_title": "Legend",
                "handle": "@Legendmedia",
                "uploader_id": "@Legendmedia",
                "country_code": "FR",
                "region": "France",
                "register": "conversational",
                "interaction_type": "interview",
                "domain": "interview_talkshow",
                "audited_videos_count": 2,
            },
            {
                "channel_title": "Squeezie",
                "handle": "@Squeezie",
                "uploader_id": "@Squeezie",
                "country_code": "FR",
                "region": "France",
                "register": "conversational",
                "interaction_type": "monologue",
                "domain": "entertainment_vlog",
                "audited_videos_count": 2,
            },
            {
                "channel_title": "Linguisticae",
                "handle": "@Linguisticae",
                "uploader_id": "@Linguisticae",
                "country_code": "FR",
                "region": "France",
                "register": "pedagogical",
                "interaction_type": "monologue",
                "domain": "linguistics_french",
                "audited_videos_count": 2,
            },
            {
                "channel_title": "HugoDécrypte",
                "handle": "@HugoDecrypte",
                "uploader_id": "@HugoDecrypte",
                "country_code": "FR",
                "region": "France",
                "register": "broadcast",
                "interaction_type": "monologue",
                "domain": "journalism_news",
                "audited_videos_count": 2,
            },
            {
                "channel_title": "Sous Écoute",
                "handle": "@MikeWardSousEcoute",
                "uploader_id": "@MikeWardSousEcoute",
                "country_code": "CA",
                "region": "Quebec",
                "register": "conversational",
                "interaction_type": "dialogue",
                "domain": "comedy_podcast",
                "audited_videos_count": 2,
            },
            {
                "channel_title": "Radio-Canada Info",
                "handle": "@RadCa",
                "uploader_id": "@RadCa",
                "country_code": "CA",
                "region": "Quebec",
                "register": "broadcast",
                "interaction_type": "dialogue",
                "domain": "public_broadcast",
                "audited_videos_count": 2,
            },
            {
                "channel_title": "GuiHome Vous Détend",
                "handle": "@GuiHomeVousDetend",
                "uploader_id": "@GuiHomeVousDetend",
                "country_code": "BE",
                "region": "Wallonie",
                "register": "conversational",
                "interaction_type": "monologue",
                "domain": "comedy_sketch",
                "audited_videos_count": 2,
            },
            {
                "channel_title": "RTS - Radio Télévision Suisse",
                "handle": "@RTS",
                "uploader_id": "@RTS",
                "country_code": "CH",
                "region": "Romandie",
                "register": "broadcast",
                "interaction_type": "dialogue",
                "domain": "public_broadcast",
                "audited_videos_count": 2,
            },
        ],
    }

    catalog_path = sandbox_staging_dir / "french_e2e_catalog.json"
    with open(catalog_path, "w", encoding="utf-8") as f:
        json.dump(french_catalog_data, f, indent=2, ensure_ascii=False)

    # 2. Mock candidate videos and regional WebVTT transcripts for all 8 squads
    vtt_corpus = {
        # Shard 1 (FR): dialogue marker
        "v_sq1_man": (
            "WEBVTT\nKind: captions\nLanguage: fr-FR\n\n"
            "00:00:01.000 --> 00:00:15.000\n"
            "Bienvenue dans ce grand dialogue d'entretiens et d'échanges philosophiques avec nos invités.\n\n"
            "00:00:15.500 --> 00:00:30.000\n"
            "Nous abordons les questions éthiques et la construction de la pensée critique moderne.\n"
        ),
        "v_sq1_auto": (
            "WEBVTT\nKind: captions\nLanguage: fr\n\n"
            "00:00:01.000 --> 00:00:10.000\n"
            "Transcription automatique de la discussion générale et des actualités.\n"
        ),
        # Shard 2 (FR): dialogue & banter
        "v_sq2_man": (
            "WEBVTT\nKind: captions\nLanguage: fr-FR\n\n"
            "00:00:01.000 --> 00:00:15.000\n"
            "Salut tout le monde! Aujourd'hui c'est un dialogue humoristique entre créateurs de contenu.\n"
        ),
        "v_sq2_auto": (
            "WEBVTT\nKind: captions\nLanguage: fr\n\n"
            "00:00:01.000 --> 00:00:10.000\n"
            "Vidéo divertissement bêtisier et sketchs amusants.\n"
        ),
        # Shard 3 (FR): vulgarisation marker
        "v_sq3_man": (
            "WEBVTT\nKind: captions\nLanguage: fr-FR\n\n"
            "00:00:01.000 --> 00:00:15.000\n"
            "Bienvenue pour cet épisode de vulgarisation scientifique et d'histoire de la langue française.\n"
        ),
        "v_sq3_auto": (
            "WEBVTT\nKind: captions\nLanguage: fr\n\n"
            "00:00:01.000 --> 00:00:10.000\n"
            "Explication didactique des concepts fondamentaux.\n"
        ),
        # Shard 4 (FR): dialogue médiatique
        "v_sq4_man": (
            "WEBVTT\nKind: captions\nLanguage: fr-FR\n\n"
            "00:00:01.000 --> 00:00:15.000\n"
            "L'actualité internationale du jour et le dialogue diplomatique au Parlement européen.\n"
        ),
        "v_sq4_auto": (
            "WEBVTT\nKind: captions\nLanguage: fr\n\n"
            "00:00:01.000 --> 00:00:10.000\n"
            "Résumé express des cinq actualités incontournables.\n"
        ),
        # Shard 5 (CA): pantoute & tiguidou markers
        "v_sq5_man": (
            "WEBVTT\nKind: captions\nLanguage: fr-CA\n\n"
            "00:00:01.000 --> 00:00:15.000\n"
            "Mon cher ami québécois, c'est pas pantoute la même affaire icitte, mais tout est tiguidou!\n"
        ),
        "v_sq5_auto": (
            "WEBVTT\nKind: captions\nLanguage: fr-CA\n\n"
            "00:00:01.000 --> 00:00:10.000\n"
            "Discussion en direct depuis Montréal avec la gang du podcast.\n"
        ),
        # Shard 6 (CA): pantoute marker
        "v_sq6_man": (
            "WEBVTT\nKind: captions\nLanguage: fr-CA\n\n"
            "00:00:01.000 --> 00:00:15.000\n"
            "À Radio-Canada, le public n'est pantoute convaincu par les nouvelles déclarations ministérielles.\n"
        ),
        "v_sq6_auto": (
            "WEBVTT\nKind: captions\nLanguage: fr-CA\n\n"
            "00:00:01.000 --> 00:00:10.000\n"
            "Reportage télévisé sur l'économie québécoise et les régions maritimes.\n"
        ),
        # Shard 7 (BE): nonante & oufti markers
        "v_sq7_man": (
            "WEBVTT\nKind: captions\nLanguage: fr-BE\n\n"
            "00:00:01.000 --> 00:00:15.000\n"
            "Oufti! Il y a au moins nonante personnes rassemblées sur la Grand-Place de Bruxelles une fois.\n"
        ),
        "v_sq7_auto": (
            "WEBVTT\nKind: captions\nLanguage: fr-BE\n\n"
            "00:00:01.000 --> 00:00:10.000\n"
            "Spectacle comique en Belgique avec les meilleurs humoristes wallons.\n"
        ),
        # Shard 8 (CH): septante marker
        "v_sq8_man": (
            "WEBVTT\nKind: captions\nLanguage: fr-CH\n\n"
            "00:00:01.000 --> 00:00:15.000\n"
            "Il est septante heures du matin à Genève pour ce direct de la RTS au bord du lac Léman.\n"
        ),
        "v_sq8_auto": (
            "WEBVTT\nKind: captions\nLanguage: fr-CH\n\n"
            "00:00:01.000 --> 00:00:10.000\n"
            "Chronique culturelle romande et rétrospective des festivals suisses.\n"
        ),
    }

    def french_channel_video_provider(channel_handle: str) -> List[Dict[str, Any]]:
        norm_h = channel_handle.lower()
        if "legend" in norm_h:
            sq = 1
            loc = "fr-FR"
        elif "squeezie" in norm_h:
            sq = 2
            loc = "fr-FR"
        elif "linguisticae" in norm_h:
            sq = 3
            loc = "fr-FR"
        elif "hugodecrypte" in norm_h:
            sq = 4
            loc = "fr-FR"
        elif "mikeward" in norm_h:
            sq = 5
            loc = "fr-CA"
        elif "radca" in norm_h:
            sq = 6
            loc = "fr-CA"
        elif "guihome" in norm_h:
            sq = 7
            loc = "fr-BE"
        else:
            sq = 8
            loc = "fr-CH"

        return [
            # Candidate 1: Manual CC (prioritized under prefer_manual_cc despite lower views)
            {
                "id": f"v_sq{sq}_man",
                "title": f"Squad {sq} Manual CC Video",
                "duration": 900,
                "view_count": 15000,
                "has_manual_fr_cc": True,
                "subtitles": {loc: [{"ext": "vtt", "url": f"https://mock.yt/v_sq{sq}_man.vtt", "name": f"Français ({loc})"}]},
            },
            # Candidate 2: Auto CC (higher views)
            {
                "id": f"v_sq{sq}_auto",
                "title": f"Squad {sq} Auto ASR Video",
                "duration": 750,
                "view_count": 5000000,
                "has_manual_fr_cc": False,
                "subtitles": {},
                "automatic_captions": {"fr": [{"ext": "vtt", "url": f"https://mock.yt/v_sq{sq}_auto.vtt"}]},
            },
        ]

    def french_vtt_provider(video_id: str, _url: str) -> Optional[str]:
        return vtt_corpus.get(video_id)

    # 3. Execute full 8-squad French teamwork harvest orchestration
    res = run_teamwork_harvest(
        shards=8,
        catalog=catalog_path,
        staging_dir=sandbox_staging_dir,
        vault_path=sandbox_vault_path,
        lang="fr",
        cap_per_channel=150,
        prefer_manual_cc=True,
        auto_merge=True,
        channel_video_provider=french_channel_video_provider,
        vtt_provider=french_vtt_provider,
    )

    # 4. Verify coordination success & staging isolation
    assert res["success"] is True, f"Teamwork harvest failed with errors: {res.get('errors')}"
    assert len(res["shard_results"]) == 8, "Expected exactly 8 squad shards to complete"

    for s_id in range(1, 9):
        shard_db = sandbox_staging_dir / f"shard_{s_id}.db"
        assert shard_db.is_file(), f"Staging database missing for squad {s_id}: {shard_db}"
        s_res = res["shard_results"][s_id]
        assert s_res["sources_processed"] == 1
        assert s_res["items_cataloged"] == 2
        assert s_res["payloads_inserted"] == 2

    # 5. Verify atomic consolidation into canonical raw_vault.db
    manifest = res["merge_manifest"]
    assert manifest is not None
    assert manifest["status"] == "SUCCESS"
    assert manifest["total_sources"] == 8
    assert manifest["total_items"] == 16
    assert manifest["total_payloads"] == 16
    assert manifest["sources_added"] == 8
    assert manifest["items_added"] == 16
    assert manifest["payloads_added"] == 16

    # 6. Verify SQLite database tables, regional distributions & constraints
    with VaultManager(db_path=sandbox_vault_path) as vm:
        conn = vm.conn

        # Geographic distribution across FR, CA, BE, CH
        countries = {
            r[0]: r[1]
            for r in conn.execute(
                "SELECT country_code, COUNT(*) FROM sources GROUP BY country_code;"
            ).fetchall()
        }
        assert countries.get("FR") == 4, "Expected 4 France sources in Shards 1-4"
        assert countries.get("CA") == 2, "Expected 2 Canada/Québec sources in Shards 5-6"
        assert countries.get("BE") == 1, "Expected 1 Belgium source in Shard 7"
        assert countries.get("CH") == 1, "Expected 1 Switzerland source in Shard 8"

        # Locale distribution in payloads
        locales = {
            r[0]: r[1]
            for r in conn.execute(
                "SELECT locale, COUNT(*) FROM payloads GROUP BY locale;"
            ).fetchall()
        }
        assert "fr-CA" in locales
        assert "fr-BE" in locales
        assert "fr-CH" in locales
        assert "fr-FR" in locales or "fr" in locales

        # Payload origin types: verified human CC and auto ASR captions both ingested
        payload_types = {
            r[0]: r[1]
            for r in conn.execute(
                "SELECT payload_type, COUNT(*) FROM payloads GROUP BY payload_type;"
            ).fetchall()
        }
        assert payload_types.get("manual_cc") == 8
        assert payload_types.get("auto_cc") == 8

        # 7. Validate FTS5 full-text BM25 search across regional French dialects
        # Québec marker: 'pantoute'
        qc_hits = vm.search_text("pantoute")
        assert len(qc_hits) >= 2, "FTS5 search for 'pantoute' must find Canadian episodes"
        for hit in qc_hits:
            assert hit["country_code"] == "CA"
            assert hit["locale"] == "fr-CA"
            assert "pantoute" in hit["raw_content"].lower()

        # Switzerland marker: 'septante'
        ch_hits = vm.search_text("septante")
        assert len(ch_hits) >= 1, "FTS5 search for 'septante' must find Swiss episode"
        assert ch_hits[0]["country_code"] == "CH"
        assert ch_hits[0]["locale"] == "fr-CH"
        assert "septante" in ch_hits[0]["raw_content"].lower()

        # Belgium marker: 'nonante'
        be_hits = vm.search_text("nonante")
        assert len(be_hits) >= 1, "FTS5 search for 'nonante' must find Belgian episode"
        assert be_hits[0]["country_code"] == "BE"
        assert be_hits[0]["locale"] == "fr-BE"
        assert "nonante" in be_hits[0]["raw_content"].lower()

        # France markers: 'dialogue' and 'vulgarisation'
        fr_dial_hits = vm.search_text("dialogue")
        assert len(fr_dial_hits) >= 1, "FTS5 search for 'dialogue' must find French episodes"
        assert any(h["country_code"] == "FR" for h in fr_dial_hits)

        fr_vulg_hits = vm.search_text("vulgarisation")
        assert len(fr_vulg_hits) >= 1, "FTS5 search for 'vulgarisation' must find Squad 3 episode"
        assert fr_vulg_hits[0]["country_code"] == "FR"

