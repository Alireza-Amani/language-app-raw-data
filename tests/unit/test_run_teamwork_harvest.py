"""Unit tests for run_teamwork_harvest.py orchestrator."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from scripts.run_teamwork_harvest import (
    build_parser,
    resolve_language_defaults,
    run_orchestration,
    run_shard_worker,
)



def test_run_shard_worker_mocked():
    """Verify run_shard_worker executes the subprocess and parses output."""
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="[+] Shard 1 complete: 10 items",
            stderr="",
        )
        s_id, code, el, out = run_shard_worker(
            shard_id=1,
            catalog_path="data/test.json",
            staging_dir="data/staging",
            cap_per_channel=5,
            all_videos=False,
            min_duration=600,
            workers=1,
            dry_run=True,
        )
        assert s_id == 1
        assert code == 0
        assert "complete" in out


def test_run_orchestration_mocked(tmp_path: Path):
    """Verify orchestration partitions channels, runs workers, and executes merger."""
    mock_catalog = tmp_path / "catalog.json"
    mock_catalog.write_text('{"channels": [{"handle": "@chan1", "channel_id": "c1", "domain": "podcasts"}]}')
    staging_dir = tmp_path / "staging"
    vault_path = tmp_path / "vault.db"

    mock_manifest = {
        "status": "SUCCESS",
        "total_sources": 1,
        "total_items": 5,
        "total_payloads": 5,
        "sources_added": 1,
        "items_added": 5,
        "payloads_added": 5,
        "total_persian_words": 12000,
        "manifest_path": str(tmp_path / "manifest.json"),
    }

    with patch("scripts.run_teamwork_harvest.load_catalog", return_value=[{"handle": "@chan1"}]) as mock_load:
        with patch("scripts.run_teamwork_harvest.partition_channels", return_value={"channels": [{"handle": "@chan1"}]}) as mock_part:
            with patch("scripts.run_teamwork_harvest.save_partitions") as mock_save:
                with patch("scripts.run_teamwork_harvest.run_shard_worker") as mock_worker:
                    mock_worker.return_value = (1, 0, 1.5, "OK")
                    with patch("scripts.run_teamwork_harvest.merge_staging_to_vault") as mock_merge:
                        mock_merge.return_value = mock_manifest

                        res = run_orchestration(
                            shards=1,
                            catalog=str(mock_catalog),
                            staging_dir=str(staging_dir),
                            cap_per_channel=5,
                            auto_merge=True,
                            vault_path=str(vault_path),
                            parallel=False,
                            dry_run=False,
                        )

    assert res["status"] == "SUCCESS"
    assert res["failed_shards"] == []
    assert res["manifest"]["total_items"] == 5
    assert mock_merge.called


def test_run_teamwork_harvest_french_orchestration(tmp_path: Path):
    """Verify French teamwork orchestrator executes 8 squads with --lang fr,
    --cap-per-channel 150, and --prefer-manual-cc, mocking worker invocations and verifying manifest and parameter forwarding."""
    # 1. Parameter forwarding in run_shard_worker subprocess invocation
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="[+] Shard 1 complete: 20 items", stderr="")
        s_id, code, el, out = run_shard_worker(
            shard_id=1,
            catalog_path="data/french_channels_catalog.json",
            staging_dir=str(tmp_path / "staging_worker"),
            cap_per_channel=150,
            all_videos=False,
            min_duration=600,
            workers=2,
            dry_run=False,
            lang="fr",
            prefer_manual_cc=True,
        )
        assert s_id == 1
        assert code == 0
        cmd = mock_run.call_args[0][0]
        assert "--lang" in cmd
        assert cmd[cmd.index("--lang") + 1] == "fr"
        assert "--prefer-manual-cc" in cmd
        assert "--cap-per-channel" in cmd
        assert cmd[cmd.index("--cap-per-channel") + 1] == "150"

    # 2. Complete 8-squad French orchestration flow
    mock_catalog = tmp_path / "french_catalog.json"
    mock_catalog.write_text('{"channels": []}')
    staging_dir = tmp_path / "staging" / "french"
    vault_path = tmp_path / "vault.db"

    mock_manifest = {
        "status": "SUCCESS",
        "total_sources": 8,
        "total_items": 40,
        "total_payloads": 40,
        "sources_added": 8,
        "items_added": 40,
        "payloads_added": 40,
        "manifest_path": str(tmp_path / "manifest.json"),
    }

    mock_channels = [{"handle": f"@ch_{i}", "country_code": "FR"} for i in range(1, 9)]
    mock_squads = [
        {
            "shard_id": i,
            "squad_name": f"Squad {i}",
            "country_code": "FR" if i <= 4 else ("CA" if i <= 6 else ("BE" if i == 7 else "CH")),
            "channels": [mock_channels[i - 1]],
        }
        for i in range(1, 9)
    ]

    with patch("scripts.run_teamwork_harvest.load_french_catalog", return_value=mock_channels) as mock_load, \
         patch("scripts.run_teamwork_harvest.partition_by_squads", return_value=mock_squads) as mock_part, \
         patch("scripts.run_teamwork_harvest.run_shard_worker") as mock_worker, \
         patch("scripts.run_teamwork_harvest.merge_staging_to_vault") as mock_merge:

        mock_worker.side_effect = [(i, 0, 0.5, f"Shard {i} OK") for i in range(1, 9)]
        mock_merge.return_value = mock_manifest

        res = run_orchestration(
            lang="fr",
            catalog=str(mock_catalog),
            staging_dir=str(staging_dir),
            vault_path=str(vault_path),
            auto_merge=True,
            parallel=False,
            dry_run=False,
        )

        assert res["status"] == "SUCCESS"
        assert res["shards"] == 8
        assert mock_worker.call_count == 8

        # Verify each of the 8 squads received proper parameters
        for call_idx, call_args in enumerate(mock_worker.call_args_list, start=1):
            kwargs = call_args[1]
            assert kwargs["shard_id"] == call_idx
            assert kwargs["lang"] == "fr"
            assert kwargs["cap_per_channel"] == 150
            assert kwargs["prefer_manual_cc"] is True
            assert kwargs["staging_dir"] == str(staging_dir)

        # Verify partitions file written
        partitions_file = staging_dir / "partitions.json"
        assert partitions_file.is_file()
        part_content = json.loads(partitions_file.read_text(encoding="utf-8"))
        assert part_content["language_iso1"] == "fr"
        assert len(part_content["shards"]) == 8

        # Verify manifest enriched with Francophone statistics
        assert mock_merge.called
        assert "country_breakdown" in res["manifest"]
        assert "locale_distribution" in res["manifest"]
        assert "manual_cc_count" in res["manifest"]
        assert "auto_cc_count" in res["manifest"]

    # 3. Verify CLI parser defaults resolution for French
    parser = build_parser()
    args = parser.parse_args(["--lang", "fr"])
    lang, shards, cat, st_dir, cap, pref = resolve_language_defaults(
        lang=args.lang,
        shards=args.shards,
        catalog=args.catalog,
        staging_dir=args.staging_dir,
        cap_per_channel=args.cap_per_channel,
        prefer_manual_cc=args.prefer_manual_cc,
    )
    assert lang == "fr"
    assert shards == 8
    assert "french" in cat
    assert "french" in st_dir
    assert cap == 150
    assert pref is True

