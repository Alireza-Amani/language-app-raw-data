"""Unit tests for run_teamwork_harvest.py orchestrator."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from scripts.run_teamwork_harvest import run_orchestration, run_shard_worker


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
