"""Pytest configuration and hermetic test harness for E2E testing.

Provides:
1. `isolate_network` fixture: Enforces zero external network calls.
2. Authentic Persian WebVTT fixtures across formal, colloquial, Unicode edge, and massive payloads.
3. Mock yt-dlp & subtitle streaming engine fixture.
4. Isolated sandbox environment fixtures for staging shards and canonical vault.
5. Contract helpers adhering strictly to PROJECT.md specifications.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import socket
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import pytest

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.vault.db import VaultManager
from tests.e2e.contract_helpers import (
    count_persian_words_in_vtt,
    harvest_channel_shard,
    merge_staging_to_vault,
    partition_channels,
    resolve_persian_subtitles,
    run_teamwork_harvest,
)


# -----------------------------------------------------------------------------
# 1. Hermetic Network Isolation Guard
# -----------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def isolate_network(monkeypatch) -> Generator[None, None, None]:
    """Guarantees zero outbound socket calls to external network during test runs.
    
    Permits local unix/loopback domain sockets (127.0.0.1, ::1) while strictly
    rejecting any external TCP/UDP connections.
    """
    orig_connect = socket.socket.connect

    def guarded_connect(self, address):
        if isinstance(address, tuple) and len(address) >= 2:
            host, port = address[0], address[1]
            if host in ("127.0.0.1", "localhost", "::1", None):
                return orig_connect(self, address)
            raise RuntimeError(
                f"External network call forbidden in hermetic E2E test harness: {host}:{port}"
            )
        # Unix domain sockets (string path)
        return orig_connect(self, address)

    monkeypatch.setattr(socket.socket, "connect", guarded_connect)
    yield


# -----------------------------------------------------------------------------
# 2. Authentic Persian WebVTT Corpus Fixtures
# -----------------------------------------------------------------------------

@pytest.fixture
def persian_vtt_formal() -> str:
    """Authentic formal/broadcast Persian WebVTT corpus (news, academic terminology)."""
    return (
        "WEBVTT\n"
        "Kind: captions\n"
        "Language: fa\n\n"
        "00:00:01.000 --> 00:00:05.200\n"
        "با درود و احترام، به بخش ویژه بررسی تحولات ژئوپلیتیک خاورمیانه خوش آمدید.\n\n"
        "00:00:05.500 --> 00:00:10.800\n"
        "بر اساس تازه‌ترین داده‌های آماری، شاخص‌های کلان اقتصادی و روابط بین‌الملل مورد بازبینی قرار گرفتند.\n\n"
        "00:00:11.000 --> 00:00:16.400\n"
        "پژوهش‌های دانشگاهی حاکی از آن است که تغییرات اقلیمی تأثیر مستقیمی بر سیاست‌گذاری‌های عمومی دارد.\n\n"
        "00:00:16.800 --> 00:00:22.000\n"
        "جامعه‌شناسان تأکید دارند که همبستگی اجتماعی و توسعه پایدار مستلزم گفتگوی سازنده نهادهای مدنی است.\n\n"
        "00:00:22.500 --> 00:00:28.000\n"
        "در ادامه این گفتگوی تفصیلی، ابعاد حقوقی و ساختار حاکمیت قانون به بحث گذاشته خواهد شد.\n"
    )


@pytest.fixture
def persian_vtt_colloquial() -> str:
    """Authentic colloquial conversational Persian WebVTT corpus (Tehrani podcast, slang)."""
    return (
        "WEBVTT\n"
        "Kind: captions\n"
        "Language: fa\n\n"
        "00:00:01.200 --> 00:00:04.800\n"
        "آقا سلام خیلی چاکریم، دمتون گرم که دوباره با پادکست خودتون همراه شدید!\n\n"
        "00:00:05.000 --> 00:00:09.500\n"
        "امروز یه موضوع خیلی باحال داریم، طرف فکر می‌کنه خیلی بارشونه ولی قشنگ هیچی بارش نیست.\n\n"
        "00:00:09.800 --> 00:00:14.200\n"
        "یعنی رسماً قفل کرده بودم روش، مگه میشه یه نفر این‌قدر با اعتماد به نفس سوتی بده؟!\n\n"
        "00:00:14.500 --> 00:00:18.900\n"
        "خلاصه داداش ترکوندی با این سوال، تا ته این اپیزود حتماً باهامون باش که کلی می‌خندیم.\n\n"
        "00:00:19.200 --> 00:00:24.000\n"
        "راستی تو کامنت‌ها واسم بنویسید شما اگه بودید چه واکنشی نشون می‌دادید، حتماً کامنت‌ها رو می‌خونم.\n"
    )


@pytest.fixture
def persian_vtt_unicode_edge() -> str:
    """Persian WebVTT corpus with complex orthographic & Unicode edge cases.
    
    Includes:
    - Zero-Width Non-Joiner (ZWNJ \\u200c): 'می‌خواهم', 'کتاب‌ها', 'بی‌تجربه', 'دست‌اندرکاران'
    - Persian vs Arabic characters: 'ک' vs 'ك', 'ی' vs 'ي'
    - Persian digits: '۰۱۲۳۴۵۶۷۸۹'
    - Mixed RTL/LTR English words ('Python', 'PyTorch', 'LLM', 'AI')
    - Persian/Arabic diacritics (harakat / tashkeel: َ ِ ُ ً ٍ ٌ ّ)
    """
    return (
        "WEBVTT\n"
        "Kind: captions\n"
        "Language: fa\n\n"
        "00:00:01.000 --> 00:00:06.000\n"
        "من می‌خواهم کتاب‌ها و دست‌اندرکاران این پروژه‌ی بی‌نظیر را معرفی کنم.\n\n"
        "00:00:06.500 --> 00:00:11.500\n"
        "تفاوت حروف عربی ك و ي با حروف فارسی ک و ی بسیار با‌اهمیت است: شرکت‌ها و هدایت.\n\n"
        "00:00:12.000 --> 00:00:17.000\n"
        "در سال ۱۴۰۳ بیش از ۲۵۰ هزار مخاطب با میانگین امتیاز ۴.۸ ثبت شده‌اند (شماره‌های ۰۱۲۳۴۵۶۷۸۹).\n\n"
        "00:00:17.500 --> 00:00:23.000\n"
        "در این قسمت فریم‌ورک‌های Python و PyTorch برای مدل‌های زبانی LLM و Deep Learning آموزش داده می‌شود.\n\n"
        "00:00:23.500 --> 00:00:29.000\n"
        "مَتْنِ مُعَرَّبْ: کِتَابٌ قَیِّمٌ وَ عَمَلٌ مُتْقَنٌ که دارای اِعْرَابْ و تَشْدِیدْ می‌باشد.\n"
    )


@pytest.fixture
def persian_vtt_massive() -> str:
    """Large authentic Persian WebVTT payload exceeding 10,000 cues."""
    cues = ["WEBVTT\nKind: captions\nLanguage: fa\n"]
    for i in range(1, 10051):
        start_sec = i * 2
        end_sec = start_sec + 1
        s_min, s_s = divmod(start_sec, 60)
        s_h, s_min = divmod(s_min, 60)
        e_min, e_s = divmod(end_sec, 60)
        e_h, e_min = divmod(e_min, 60)

        t_start = f"{s_h:02d}:{s_min:02d}:{s_s:02d}.000"
        t_end = f"{e_h:02d}:{e_min:02d}:{e_s:02d}.500"

        cues.append(
            f"{t_start} --> {t_end}\n"
            f"فراز شماره {i}: گفتگو پیرامون زبان فارسی، پردازش متن و داده‌کاوی در فضای کلان داده‌ها.\n"
        )
    return "\n".join(cues)


# -----------------------------------------------------------------------------
# 3. Mock yt-dlp & Subtitle Streaming Engine Fixture
# -----------------------------------------------------------------------------

class MockYtDlpEngine:
    """Hermetic yt-dlp & WebVTT streaming mock engine."""

    def __init__(self):
        self.playlists: Dict[str, List[Dict[str, Any]]] = {}
        self.videos: Dict[str, Dict[str, Any]] = {}
        self.vtt_corpus: Dict[str, str] = {}

    def register_playlist(self, channel_url: str, entries: List[Dict[str, Any]]):
        self.playlists[channel_url] = entries

    def register_video(
        self,
        video_id: str,
        title: str,
        duration: int,
        view_count: int,
        has_manual_cc: bool = False,
        has_auto_cc: bool = True,
        vtt_content: Optional[str] = None,
    ):
        vtt_url = f"https://mock-yt.local/vtt/{video_id}.vtt"
        
        subtitles = {}
        auto_subs = {}

        if has_manual_cc:
            subtitles["fa"] = [{"ext": "vtt", "url": vtt_url, "name": "Persian"}]
        if has_auto_cc:
            auto_subs["fa"] = [{"ext": "vtt", "url": vtt_url, "name": "Persian (auto)"}]

        video_meta = {
            "id": video_id,
            "title": title,
            "duration": duration,
            "duration_seconds": duration,
            "view_count": view_count,
            "upload_date": "20261101",
            "webpage_url": f"https://www.youtube.com/watch?v={video_id}",
            "subtitles": subtitles,
            "automatic_captions": auto_subs,
        }
        self.videos[video_id] = video_meta

        if vtt_content is not None:
            self.vtt_corpus[vtt_url] = vtt_content
        else:
            default_vtt = (
                f"WEBVTT\n\n00:00:01.000 --> 00:00:05.000\n"
                f"محتوای زیرنویس ویدیو {title} با کیفیت عالی فارسی.\n"
            )
            self.vtt_corpus[vtt_url] = default_vtt

    def extract_flat(self, channel_url: str) -> List[Dict[str, Any]]:
        return self.playlists.get(channel_url, [])

    def extract_info(self, video_id_or_url: str) -> Optional[Dict[str, Any]]:
        v_id = video_id_or_url.split("watch?v=")[-1] if "watch?v=" in video_id_or_url else video_id_or_url
        return self.videos.get(v_id)

    def stream_vtt_in_memory(self, vtt_url: str) -> Tuple[str, str]:
        """Stream WebVTT text into memory and compute genuine SHA-256."""
        if vtt_url not in self.vtt_corpus:
            raise ValueError(f"Mock VTT URL not found: {vtt_url}")
        content = self.vtt_corpus[vtt_url]
        content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
        return content, content_hash


@pytest.fixture
def mock_ytdlp_engine(
    persian_vtt_formal: str,
    persian_vtt_colloquial: str,
    persian_vtt_unicode_edge: str,
) -> MockYtDlpEngine:
    """Initialized deterministic Mock yt-dlp and VTT streaming engine."""
    engine = MockYtDlpEngine()

    # Pre-register standard sample videos
    engine.register_video(
        video_id="vid_manual_formal",
        title="گفتگوی فلسفی و ژئوپلیتیک",
        duration=1800,
        view_count=50000,
        has_manual_cc=True,
        has_auto_cc=True,
        vtt_content=persian_vtt_formal,
    )
    engine.register_video(
        video_id="vid_auto_colloquial",
        title="ولاگ فان و خنده‌دار در تهران",
        duration=950,
        view_count=85000,
        has_manual_cc=False,
        has_auto_cc=True,
        vtt_content=persian_vtt_colloquial,
    )
    engine.register_video(
        video_id="vid_edge_unicode",
        title="آموزش هوش مصنوعی و زبان فارسی",
        duration=1200,
        view_count=12000,
        has_manual_cc=True,
        has_auto_cc=False,
        vtt_content=persian_vtt_unicode_edge,
    )
    engine.register_video(
        video_id="vid_short_clip",
        title="شورت ویدیوی ۳۰ ثانیه‌ای",
        duration=45,
        view_count=150000,
        has_manual_cc=False,
        has_auto_cc=True,
    )
    engine.register_video(
        video_id="vid_no_subs",
        title="ویدیوی بدون هیچ زیرنویس",
        duration=800,
        view_count=3000,
        has_manual_cc=False,
        has_auto_cc=False,
    )
    return engine


# -----------------------------------------------------------------------------
# 4. Sandbox Environment Fixtures
# -----------------------------------------------------------------------------

@pytest.fixture
def sandbox_staging_dir() -> Generator[Path, None, None]:
    """Isolated temporary staging directory for worker shards."""
    temp_dir = tempfile.mkdtemp(prefix="staging_shard_test_")
    staging_path = Path(temp_dir)
    yield staging_path
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def sandbox_vault_path() -> Generator[Path, None, None]:
    """Isolated temporary path for destination canonical vault SQLite database."""
    temp_dir = tempfile.mkdtemp(prefix="vault_test_")
    vault_file = Path(temp_dir) / "raw_vault.db"
    yield vault_file
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def schema_sql_path() -> Path:
    """Path to the real vault schema.sql."""
    schema_p = PROJECT_ROOT / "src" / "vault" / "schema.sql"
    assert schema_p.is_file(), f"Vault schema not found at {schema_p}"
    return schema_p


@pytest.fixture
def canonical_catalog_path() -> Path:
    """Path to the canonical data/persian_channels_catalog.json."""
    cat_p = PROJECT_ROOT / "data" / "persian_channels_catalog.json"
    assert cat_p.is_file(), f"Catalog not found at {cat_p}"
    return cat_p


@pytest.fixture
def real_vault_manager(sandbox_vault_path: Path) -> Generator[VaultManager, None, None]:
    """VaultManager initialized on a real, isolated SQLite database file."""
    vm = VaultManager(db_path=sandbox_vault_path)
    vm.init_db()
    yield vm
    vm.close()
