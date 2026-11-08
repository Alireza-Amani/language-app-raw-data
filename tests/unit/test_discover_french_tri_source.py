"""Unit and regression tests for French Tri-Source Corpus Discovery.

Validates:
1. Strict pre-2022 temporal compliance: timestamp < 1640995200 and date < "2022-01-01"
   for 100% of discovered comments.
2. Geographic invariance strictly restricted to FR, CA, BE, CH (Africa & Maghreb excluded).
3. Authenticity and reachability of live endpoints (un-mocked live tests for Canadian Hansard,
   Swiss Curia Vista, French Open Data, OpenSubtitles).
4. Catalog schema parity across all 3 tri-source modalities:
   - comment_thread / human_written
   - manual_cc / human_transcribed
   - parliament_transcript / human_transcribed
5. Graceful skipping when catalog JSON files are not yet created on disk, with full
   strict validation when catalogs exist.
"""

from __future__ import annotations

import json
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import pytest

# ---------------------------------------------------------------------------
# Strict Contract Constants
# ---------------------------------------------------------------------------

# Unix epoch cutoff: 2022-01-01T00:00:00Z
PRE_2022_EPOCH_CUTOFF: int = 1640995200
PRE_2022_DATE_CUTOFF: str = "2022-01-01"

# Allowed Francophone jurisdiction country codes
ALLOWED_COUNTRY_CODES: Set[str] = {"FR", "CA", "BE", "CH"}

# Strictly forbidden African and Maghreb ISO country codes
FORBIDDEN_AFRICAN_MAGHREB_CODES: Set[str] = {
    "DZ", "MA", "TN", "EG", "LY", "MR", "SN", "CI", "CM", "CD",
    "CG", "ML", "BF", "NE", "GN", "TD", "RW", "BI", "BJ", "TG",
    "GA", "DJ", "CF", "KM", "SC", "GQ", "MU", "MG", "HT", "VU",
}

ROOT_DIR: Path = Path(__file__).resolve().parent.parent.parent
DATA_DIR: Path = ROOT_DIR / "data"

CATALOG_FILES: Dict[str, Path] = {
    "comments": DATA_DIR / "french_pre2022_comments_catalog.json",
    "cinema": DATA_DIR / "french_cinema_subtitles_catalog.json",
    "hansard": DATA_DIR / "french_hansard_debates_catalog.json",
}

REQUIRED_TOP_LEVEL_KEYS: Set[str] = {
    "generated_at_utc",
    "language_iso1",
    "stream_modality",
    "allowed_country_codes",
    "total_items",
    "items",
}

REQUIRED_ITEM_KEYS: Set[str] = {
    "item_id",
    "title",
    "country_code",
    "region",
    "platform",
    "payload_type",
    "origin_type",
    "format",
    "source_url",
    "date",
    "metadata",
}

MODALITY_SPECIFICATIONS: Dict[str, Dict[str, str]] = {
    "comments": {
        "stream_modality": "comment_thread",
        "payload_type": "comment_thread",
        "origin_type": "human_written",
        "platform": "youtube",
    },
    "cinema": {
        "stream_modality": "manual_cc",
        "payload_type": "manual_cc",
        "origin_type": "human_transcribed",
        "platform": "subtitle",
    },
    "hansard": {
        "stream_modality": "parliament_transcript",
        "payload_type": "parliament_transcript",
        "origin_type": "human_transcribed",
        "platform": "web",
    },
}


# ---------------------------------------------------------------------------
# Core Reusable Validation Helpers
# ---------------------------------------------------------------------------

def validate_comment_temporal_record(comment: Dict[str, Any], item_id: str = "") -> List[str]:
    """Validate a single comment record against pre-2022 temporal constraints.

    Must strictly satisfy:
      - timestamp < 1640995200 (if timestamp present)
      - date < "2022-01-01" (if date present)
      - At least one of timestamp or date must be present.
    """
    violations: List[str] = []
    ts = comment.get("timestamp")
    dt = comment.get("date", "")
    cid = comment.get("comment_id", "unknown")

    if ts is None and not dt:
        violations.append(f"Comment {cid} in item {item_id} lacks both timestamp and date.")
        return violations

    if ts is not None:
        if not isinstance(ts, (int, float)):
            violations.append(f"Comment {cid} timestamp '{ts}' is not numeric.")
        elif ts >= PRE_2022_EPOCH_CUTOFF:
            violations.append(
                f"Comment {cid} timestamp {ts} >= {PRE_2022_EPOCH_CUTOFF} (violates pre-2022 cutoff)."
            )

    if dt:
        if not isinstance(dt, str):
            violations.append(f"Comment {cid} date '{dt}' is not a string.")
        elif dt >= PRE_2022_DATE_CUTOFF:
            violations.append(
                f"Comment {cid} date '{dt}' >= '{PRE_2022_DATE_CUTOFF}' (violates pre-2022 cutoff)."
            )

    return violations


def validate_item_geographic_invariance(item: Dict[str, Any]) -> List[str]:
    """Validate an item's country_code against geographic invariance rules."""
    violations: List[str] = []
    item_id = item.get("item_id", "unknown")
    cc = item.get("country_code")

    if not cc:
        violations.append(f"Item {item_id} missing required 'country_code'.")
    elif cc not in ALLOWED_COUNTRY_CODES:
        violations.append(
            f"Item {item_id} has disallowed country code '{cc}'. Allowed: {ALLOWED_COUNTRY_CODES}."
        )

    if cc in FORBIDDEN_AFRICAN_MAGHREB_CODES:
        violations.append(
            f"Item {item_id} has forbidden African/Maghreb country code '{cc}'."
        )

    return violations


def validate_catalog_schema(catalog: Dict[str, Any], catalog_key: str) -> List[str]:
    """Validate top-level and item-level schema parity for a tri-source catalog."""
    violations: List[str] = []
    expected_spec = MODALITY_SPECIFICATIONS.get(catalog_key)
    if not expected_spec:
        violations.append(f"Unknown catalog key '{catalog_key}'.")
        return violations

    # Check top-level keys
    missing_top = REQUIRED_TOP_LEVEL_KEYS - set(catalog.keys())
    if missing_top:
        violations.append(f"Missing top-level keys: {sorted(missing_top)}")

    if catalog.get("language_iso1") != "fr":
        violations.append(f"Invalid language_iso1 '{catalog.get('language_iso1')}', expected 'fr'.")

    if catalog.get("stream_modality") != expected_spec["stream_modality"]:
        violations.append(
            f"Invalid stream_modality '{catalog.get('stream_modality')}', "
            f"expected '{expected_spec['stream_modality']}'."
        )

    allowed_codes = set(catalog.get("allowed_country_codes", []))
    if allowed_codes != ALLOWED_COUNTRY_CODES:
        violations.append(
            f"allowed_country_codes {allowed_codes} does not match {ALLOWED_COUNTRY_CODES}."
        )

    items = catalog.get("items")
    if not isinstance(items, list):
        violations.append(f"'items' is not a list (got {type(items)}).")
        return violations

    if catalog.get("total_items") != len(items):
        violations.append(
            f"total_items ({catalog.get('total_items')}) does not match len(items) ({len(items)})."
        )

    # Check item-level schema
    for idx, item in enumerate(items):
        item_id = item.get("item_id", f"index_{idx}")
        missing_item_keys = REQUIRED_ITEM_KEYS - set(item.keys())
        if missing_item_keys:
            violations.append(f"Item {item_id} missing keys: {sorted(missing_item_keys)}")

        if item.get("payload_type") != expected_spec["payload_type"]:
            violations.append(
                f"Item {item_id} invalid payload_type '{item.get('payload_type')}', "
                f"expected '{expected_spec['payload_type']}'."
            )

        if item.get("origin_type") != expected_spec["origin_type"]:
            violations.append(
                f"Item {item_id} invalid origin_type '{item.get('origin_type')}', "
                f"expected '{expected_spec['origin_type']}'."
            )

        if item.get("platform") != expected_spec["platform"]:
            violations.append(
                f"Item {item_id} invalid platform '{item.get('platform')}', "
                f"expected '{expected_spec['platform']}'."
            )

        if not isinstance(item.get("metadata"), dict):
            violations.append(f"Item {item_id} 'metadata' must be a dict.")

    return violations


# ---------------------------------------------------------------------------
# Pytest Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def loaded_catalogs() -> Dict[str, Dict[str, Any]]:
    """Loads available catalogs from disk into memory."""
    catalogs: Dict[str, Dict[str, Any]] = {}
    for name, path in CATALOG_FILES.items():
        if path.is_file():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    catalogs[name] = json.load(f)
            except Exception as e:
                pytest.fail(f"Failed to read catalog file {path}: {e}")
    return catalogs


# ---------------------------------------------------------------------------
# Category 1: Unit Tests for Validation Engine Logic (Self-Contained)
# ---------------------------------------------------------------------------

def test_comment_temporal_validation_accepts_valid_pre2022():
    """Verify pre-2022 comments pass temporal validation cleanly."""
    valid_comments = [
        {"comment_id": "c1", "timestamp": 1609459200, "date": "2021-01-01"},
        {"comment_id": "c2", "timestamp": 1500000000, "date": "2017-07-14"},
        {"comment_id": "c3", "timestamp": 1640995199, "date": "2021-12-31T23:59:59Z"},
    ]
    for c in valid_comments:
        violations = validate_comment_temporal_record(c, item_id="test_item")
        assert len(violations) == 0, f"Expected 0 violations for {c}, got {violations}"


def test_comment_temporal_validation_rejects_boundary_and_post2022():
    """Verify exact boundary 1640995200 and post-2022 dates are strictly rejected."""
    invalid_comments = [
        {"comment_id": "exact_cutoff", "timestamp": 1640995200, "date": "2022-01-01T00:00:00Z"},
        {"comment_id": "after_epoch", "timestamp": 1640995201, "date": "2022-01-01"},
        {"comment_id": "after_date", "timestamp": 1600000000, "date": "2022-01-01"},
        {"comment_id": "late_2023", "timestamp": 1680000000, "date": "2023-03-28"},
        {"comment_id": "empty", "timestamp": None, "date": ""},
    ]
    for c in invalid_comments:
        violations = validate_comment_temporal_record(c, item_id="test_item")
        assert len(violations) > 0, f"Expected rejection for {c}, but passed!"


def test_geographic_invariance_accepts_allowed_countries():
    """Verify FR, CA, BE, CH are accepted."""
    for cc in ALLOWED_COUNTRY_CODES:
        item = {"item_id": f"test_{cc}", "country_code": cc}
        violations = validate_item_geographic_invariance(item)
        assert len(violations) == 0, f"Expected allowed code {cc} to pass, got {violations}"


def test_geographic_invariance_rejects_african_maghreb_and_foreign():
    """Verify African/Maghreb and non-target codes are rejected."""
    forbidden_samples = ["DZ", "MA", "TN", "SN", "CI", "CM", "CD", "MG", "US", "DE", "GB", ""]
    for cc in forbidden_samples:
        item = {"item_id": f"test_{cc}", "country_code": cc}
        violations = validate_item_geographic_invariance(item)
        assert len(violations) > 0, f"Expected rejection for code '{cc}', but passed!"


def test_catalog_schema_validation_parity_logic():
    """Verify schema parity rules identify matching schemas and flag mismatches."""
    valid_sample_catalog = {
        "generated_at_utc": "2026-11-08T12:00:00Z",
        "language_iso1": "fr",
        "stream_modality": "comment_thread",
        "allowed_country_codes": ["BE", "CA", "CH", "FR"],
        "total_items": 1,
        "items": [
            {
                "item_id": "yt_123",
                "title": "Discussion Politique [Pre-2022 Comments]",
                "country_code": "FR",
                "region": "Metropolitan France",
                "platform": "youtube",
                "payload_type": "comment_thread",
                "origin_type": "human_written",
                "format": "json",
                "source_url": "https://www.youtube.com/watch?v=123",
                "date": "2021-06-15",
                "metadata": {"comments_count": 25},
            }
        ],
    }
    violations = validate_catalog_schema(valid_sample_catalog, "comments")
    assert len(violations) == 0, f"Conforming catalog should have 0 violations: {violations}"

    # Invalid catalog: wrong origin_type for comments
    invalid_catalog = dict(valid_sample_catalog)
    invalid_catalog["items"] = [dict(valid_sample_catalog["items"][0])]
    invalid_catalog["items"][0]["origin_type"] = "human_transcribed"
    violations_invalid = validate_catalog_schema(invalid_catalog, "comments")
    assert any("origin_type" in v for v in violations_invalid)


# ---------------------------------------------------------------------------
# Category 2: Live Un-mocked Endpoint Reachability Tests (Authentic HTTP 200)
# ---------------------------------------------------------------------------

def test_unmocked_canadian_hansard_reachability():
    """Verify live reachability and authenticity of Canadian House of Commons XML."""
    url = "https://www.ourcommons.ca/Content/House/432/Debates/118/HAN118-F.XML"
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        assert resp.status == 200, f"Expected HTTP 200, got {resp.status}"
        content = resp.read()
        assert len(content) > 100_000, f"Expected substantial Hansard XML, got {len(content)} bytes"

        root = ET.fromstring(content)
        assert root.tag == "Hansard", f"Expected root tag 'Hansard', got '{root.tag}'"
        assert root.attrib.get("{http://www.w3.org/XML/1998/namespace}lang") == "FR"

        text_sample = content[:5000].decode("utf-8", errors="ignore")
        assert any(
            token in text_sample for token in ["Chambre des communes", "Règlement", "Président"]
        ), "Payload does not contain expected Canadian verbatim Hansard tokens."


def test_unmocked_swiss_curia_vista_reachability():
    """Verify live reachability and authenticity of Swiss Curia Vista OData API."""
    import urllib.parse
    params = urllib.parse.urlencode({
        "$filter": "LanguageOfText eq 'FR' and MeetingDate ge '20210101' and MeetingDate lt '20220101'",
        "$top": "2",
        "$format": "json",
    })
    url = f"https://ws.parlament.ch/odata.svc/Transcript?{params}"
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
            "Accept": "application/json",
        }
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        assert resp.status == 200, f"Expected HTTP 200, got {resp.status}"
        payload = json.loads(resp.read().decode("utf-8"))
        results = payload.get("d", [])
        assert len(results) > 0, "Swiss OData query returned no records."
        record = results[0]
        assert record.get("LanguageOfText") == "FR"
        meeting_date = str(record.get("MeetingDate", ""))
        assert meeting_date < "20220101", f"MeetingDate {meeting_date} violates pre-2022 filter."
        assert len(record.get("Text", "")) > 10, "Debate speech text must be non-empty."


def test_unmocked_french_open_data_reachability():
    """Verify live reachability of French Assemblée nationale open data plenary repository."""
    url = "https://data.assemblee-nationale.fr/static/openData/repository/15/vp/syceronbrut/syseron.xml.zip"
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"},
        method="HEAD",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        assert resp.status == 200, f"Expected HTTP 200 for French open data, got {resp.status}"
        content_len = resp.headers.get("Content-Length")
        assert content_len is not None, "French open data archive should report Content-Length."
        assert int(content_len) > 50_000_000, f"Expected large plenary archive, got {content_len} bytes."


def test_unmocked_opensubtitles_reachability():
    """Verify live reachability and authenticity of OpenSubtitles French resources."""
    # 1. OpenSubtitles REST API languages check
    api_url = "https://api.opensubtitles.com/api/v1/infos/languages"
    req_api = urllib.request.Request(
        api_url,
        headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}
    )
    with urllib.request.urlopen(req_api, timeout=15) as resp:
        assert resp.status == 200, f"Expected HTTP 200 from OpenSubtitles API, got {resp.status}"
        data = json.loads(resp.read().decode("utf-8"))
        languages = data.get("data", [])
        fr_entries = [lang for lang in languages if lang.get("language_code") == "fr"]
        assert len(fr_entries) > 0, "OpenSubtitles API must support French ('fr')."

    # 2. OPUS OpenSubtitles repository check
    opus_url = "https://object.pouta.csc.fi/OPUS-OpenSubtitles/v2018/mono/fr.txt.gz"
    req_opus = urllib.request.Request(
        opus_url,
        headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"},
        method="HEAD",
    )
    with urllib.request.urlopen(req_opus, timeout=15) as resp:
        assert resp.status == 200, f"Expected HTTP 200 from OPUS archive, got {resp.status}"
        content_len = resp.headers.get("Content-Length")
        assert content_len is not None, "OPUS archive should return Content-Length."
        assert int(content_len) > 100_000_000, f"Expected large subtitle corpus, got {content_len} bytes."


# ---------------------------------------------------------------------------
# Category 3: Strict Validation of Live Catalogs on Disk (Skips if not present)
# ---------------------------------------------------------------------------

def test_live_comments_catalog_temporal_strictness(loaded_catalogs: Dict[str, Dict[str, Any]]):
    """Assert 100% of discovered comments in french_pre2022_comments_catalog.json precede 2022-01-01."""
    if "comments" not in loaded_catalogs:
        pytest.skip("french_pre2022_comments_catalog.json not yet generated on disk.")

    catalog = loaded_catalogs["comments"]
    items = catalog.get("items", [])
    assert len(items) > 0, "french_pre2022_comments_catalog.json must contain items."

    violations: List[str] = []
    total_comments_checked = 0

    for item in items:
        item_id = item.get("item_id", "unknown")
        # Item-level publication date check
        item_date = item.get("date", "")
        if item_date and item_date >= PRE_2022_DATE_CUTOFF:
            violations.append(f"Item {item_id} date '{item_date}' violates cutoff {PRE_2022_DATE_CUTOFF}")

        comments = item.get("metadata", {}).get("comments", [])
        for c in comments:
            total_comments_checked += 1
            v_list = validate_comment_temporal_record(c, item_id=item_id)
            violations.extend(v_list)

    assert total_comments_checked > 0, "Must have checked at least one comment in catalog."
    assert len(violations) == 0, f"Found {len(violations)} pre-2022 violations: {violations[:5]}"


@pytest.mark.parametrize("catalog_key", ["comments", "cinema", "hansard"])
def test_live_catalogs_geographic_invariance(
    loaded_catalogs: Dict[str, Dict[str, Any]],
    catalog_key: str
):
    """Assert all items in each live catalog are strictly from FR, CA, BE, CH with zero African codes."""
    if catalog_key not in loaded_catalogs:
        pytest.skip(f"Catalog '{catalog_key}' ({CATALOG_FILES[catalog_key].name}) not yet on disk.")

    catalog = loaded_catalogs[catalog_key]
    items = catalog.get("items", [])
    assert len(items) > 0, f"Catalog '{catalog_key}' must have items."

    violations: List[str] = []
    observed_countries: Set[str] = set()

    for item in items:
        v_list = validate_item_geographic_invariance(item)
        violations.extend(v_list)
        cc = item.get("country_code")
        if cc:
            observed_countries.add(cc)

    assert len(violations) == 0, f"Found {len(violations)} geographic violations: {violations[:5]}"
    assert observed_countries.issubset(ALLOWED_COUNTRY_CODES)


@pytest.mark.parametrize("catalog_key", ["comments", "cinema", "hansard"])
def test_live_catalogs_schema_parity(
    loaded_catalogs: Dict[str, Dict[str, Any]],
    catalog_key: str
):
    """Assert full top-level and item-level schema parity for each live catalog on disk."""
    if catalog_key not in loaded_catalogs:
        pytest.skip(f"Catalog '{catalog_key}' ({CATALOG_FILES[catalog_key].name}) not yet on disk.")

    catalog = loaded_catalogs[catalog_key]
    violations = validate_catalog_schema(catalog, catalog_key)
    assert len(violations) == 0, f"Found {len(violations)} schema parity violations: {violations[:5]}"
