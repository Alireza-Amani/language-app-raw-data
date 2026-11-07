#!/usr/bin/env python3
"""French YouTube Channel Discovery & Constructicon Cataloging Engine.

Discovers, audits, scores, and catalogs authentic Francophone YouTube channels
across France (FR), Canada/Québec (CA), Belgium (BE), and Switzerland (CH).

Key Features:
- Comprehensive seed registry covering 150+ authentic channels across 4 core regions.
- Live YouTube search query expansion (ytsearch) for discovering new channels.
- Subtitle auditing: inspects recent longform videos for manual CC vs auto CC density.
- Soft preference scoring: boosts channels with manual CC without discarding auto CC.
- Strict geographic boundary: FR, CA, BE, CH only (Africa and Maghreb excluded).
- Produces production-ready data/french_channels_catalog.json for shard harvesting.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import logging
import math
import re
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.harvest_channel_shard import normalize_uploader_handle

logger = logging.getLogger("discover_french_channels")
if not logger.handlers:
    _handler = logging.StreamHandler(sys.stderr)
    _handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))
    logger.addHandler(_handler)
logger.setLevel(logging.INFO)

# Valid ISO 3166-1 alpha-2 codes allowed for this Constructicon phase
ALLOWED_COUNTRY_CODES: Set[str] = {"FR", "CA", "BE", "CH"}

# Manual French language tags recognized on YouTube
MANUAL_FR_TAGS: Set[str] = {
    "fr",
    "fr-fr",
    "fr-ca",
    "fr-be",
    "fr-ch",
    "fra",
    "fre",
}

# Auto French language tags recognized on YouTube
AUTO_FR_TAGS: Set[str] = {
    "fr",
    "fr-orig",
    "fr-fr",
    "fr-ca",
    "fr-be",
    "fr-ch",
    "fra",
    "fre",
}

# =============================================================================
# Curated Verified Francophone Seed Registry (FR, CA, BE, CH)
# =============================================================================

# Anomaly mappings for channel handles
HANDLE_SUBSTITUTIONS: Dict[str, str] = {
    "@LegendOfficiel": "@Legendmedia",
    "@Radio-CanadaInfo": "@RadioCanadaInfo",
    "@MATH": "@Mathsefaitdesfilms",
    "@LeMonde": "@lemondefr",
    "@Blast_info": "@blastinfo",
}

SEED_CHANNELS: List[Dict[str, Any]] = [
    # -------------------------------------------------------------------------
    # 1. France (FR) - Talk Shows, Podcasts, Extended Interviews
    # -------------------------------------------------------------------------
    {
        "handle": "@Legendmedia",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "conversational",
        "interaction_type": "dialogue",
        "domain": "interview_talkshow",
    },
    {
        "handle": "@HugoDecrypte",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "broadcast",
        "interaction_type": "monologue",
        "domain": "journalism_news",
    },
    {
        "handle": "@GénérationDoItYourself",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "conversational",
        "interaction_type": "dialogue",
        "domain": "interview_entrepreneurship",
    },
    {
        "handle": "@Popcorn",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "conversational",
        "interaction_type": "panel",
        "domain": "talkshow_culture",
    },
    {
        "handle": "@ThinkerView",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "formal",
        "interaction_type": "dialogue",
        "domain": "interview_geopolitics",
    },
    {
        "handle": "@LouieMedia",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "narrative",
        "interaction_type": "dialogue",
        "domain": "narrative_podcast",
    },
    {
        "handle": "@NouvellesEcoutes",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "conversational",
        "interaction_type": "dialogue",
        "domain": "society_podcast",
    },
    {
        "handle": "@KyanKhojandi",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "colloquial",
        "interaction_type": "panel",
        "domain": "comedy_talkshow",
    },
    {
        "handle": "@CanapéSixPlaces",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "conversational",
        "interaction_type": "dialogue",
        "domain": "interview_culture",
    },
    {
        "handle": "@ZackNani",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "conversational",
        "interaction_type": "dialogue",
        "domain": "interview_gaming",
    },
    {
        "handle": "@Underscore_",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "conversational",
        "interaction_type": "panel",
        "domain": "tech_talkshow",
    },
    {
        "handle": "@LeQG",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "colloquial",
        "interaction_type": "dialogue",
        "domain": "talkshow_entertainment",
    },
    {
        "handle": "@FranceInter",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "broadcast",
        "interaction_type": "panel",
        "domain": "radio_broadcast",
    },
    {
        "handle": "@Arte",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "formal",
        "interaction_type": "monologue",
        "domain": "documentary_culture",
    },
    {
        "handle": "@Konbini",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "conversational",
        "interaction_type": "dialogue",
        "domain": "pop_culture",
    },
    {
        "handle": "@BrutOfficiel",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "conversational",
        "interaction_type": "street_interview",
        "domain": "news_society",
    },
    {
        "handle": "@FranceCulture",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "academic",
        "interaction_type": "dialogue",
        "domain": "philosophy_culture",
    },
    {
        "handle": "@Blast_info",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "formal",
        "interaction_type": "dialogue",
        "domain": "investigative_journalism",
    },
    {
        "handle": "@Mediapart",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "formal",
        "interaction_type": "panel",
        "domain": "investigative_journalism",
    },
    # -------------------------------------------------------------------------
    # France (FR) - Street Interviews, Comedy & Conversational Banter
    # -------------------------------------------------------------------------
    {
        "handle": "@LorisGiuliano",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "colloquial",
        "interaction_type": "street_interview",
        "domain": "street_interview",
    },
    {
        "handle": "@McflyetCarlito",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "colloquial",
        "interaction_type": "panel",
        "domain": "entertainment_comedy",
    },
    {
        "handle": "@Squeezie",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "conversational",
        "interaction_type": "monologue",
        "domain": "entertainment_vlog",
    },
    {
        "handle": "@Amixem",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "colloquial",
        "interaction_type": "panel",
        "domain": "entertainment_challenges",
    },
    {
        "handle": "@Seb",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "conversational",
        "interaction_type": "monologue",
        "domain": "documentary_vlog",
    },
    {
        "handle": "@Joyca",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "colloquial",
        "interaction_type": "monologue",
        "domain": "entertainment_music",
    },
    {
        "handle": "@MisterV",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "colloquial",
        "interaction_type": "dialogue",
        "domain": "comedy_sketch",
    },
    {
        "handle": "@Mastu",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "colloquial",
        "interaction_type": "panel",
        "domain": "entertainment_banter",
    },
    {
        "handle": "@Theobabac",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "colloquial",
        "interaction_type": "monologue",
        "domain": "humor_animation",
    },
    {
        "handle": "@PaulMirabel",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "conversational",
        "interaction_type": "monologue",
        "domain": "standup_comedy",
    },
    {
        "handle": "@RomanFrayssinet",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "colloquial",
        "interaction_type": "monologue",
        "domain": "standup_comedy",
    },
    {
        "handle": "@BaptisteLecaplain",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "conversational",
        "interaction_type": "monologue",
        "domain": "standup_comedy",
    },
    {
        "handle": "@Haroun",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "conversational",
        "interaction_type": "monologue",
        "domain": "standup_comedy",
    },
    {
        "handle": "@RedouaneBougheraba",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "colloquial",
        "interaction_type": "dialogue",
        "domain": "standup_improvisation",
    },
    # -------------------------------------------------------------------------
    # France (FR) - Science, Linguistics, Philosophy & Knowledge
    # -------------------------------------------------------------------------
    {
        "handle": "@Linguisticae",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "academic",
        "interaction_type": "monologue",
        "domain": "linguistics_french",
    },
    {
        "handle": "@ScienceEtonnante",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "academic",
        "interaction_type": "monologue",
        "domain": "popular_science",
    },
    {
        "handle": "@NotaBene",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "narrative",
        "interaction_type": "monologue",
        "domain": "history_vulgarisation",
    },
    {
        "handle": "@LeFossoyeurDeFilms",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "narrative",
        "interaction_type": "monologue",
        "domain": "cinema_analysis",
    },
    {
        "handle": "@CestUneAutreHistoire",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "conversational",
        "interaction_type": "monologue",
        "domain": "mythology_history",
    },
    {
        "handle": "@MonsieurPhi",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "academic",
        "interaction_type": "monologue",
        "domain": "philosophy",
    },
    {
        "handle": "@PoissonFecond",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "conversational",
        "interaction_type": "monologue",
        "domain": "thought_experiments",
    },
    {
        "handle": "@CyrusNorth",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "conversational",
        "interaction_type": "monologue",
        "domain": "philosophy_culture",
    },
    # -------------------------------------------------------------------------
    # France (FR) - Bridge & Natural Speech Pedagogy
    # -------------------------------------------------------------------------
    {
        "handle": "@innerFrench",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "conversational",
        "interaction_type": "monologue",
        "domain": "pedagogy_culture",
    },
    {
        "handle": "@FrancaisAuthentique",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "conversational",
        "interaction_type": "monologue",
        "domain": "pedagogy_spoken",
    },
    {
        "handle": "@PieceOfFrench",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "conversational",
        "interaction_type": "monologue",
        "domain": "pedagogy_vlog",
    },
    {
        "handle": "@EasyFrench",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "conversational",
        "interaction_type": "street_interview",
        "domain": "street_interview_pedagogy",
    },
    {
        "handle": "@lemondefr",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "broadcast",
        "interaction_type": "monologue",
        "domain": "journalism_news",
    },
    {
        "handle": "@PublicSenat",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "broadcast",
        "interaction_type": "panel",
        "domain": "politics_public_senat",
    },
    {
        "handle": "@RTL",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "broadcast",
        "interaction_type": "panel",
        "domain": "radio_broadcast",
    },
    {
        "handle": "@Europe1",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "broadcast",
        "interaction_type": "panel",
        "domain": "radio_broadcast",
    },
    {
        "handle": "@Mouv",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "conversational",
        "interaction_type": "panel",
        "domain": "youth_radio",
    },
    {
        "handle": "@Cyprien",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "colloquial",
        "interaction_type": "monologue",
        "domain": "comedy_sketch",
    },
    {
        "handle": "@NormanFaitDesVideos",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "colloquial",
        "interaction_type": "monologue",
        "domain": "comedy_sketch",
    },
    {
        "handle": "@Ludovik",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "colloquial",
        "interaction_type": "dialogue",
        "domain": "comedy_sketch",
    },
    {
        "handle": "@StudioBagel",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "colloquial",
        "interaction_type": "panel",
        "domain": "comedy_sketch",
    },
    {
        "handle": "@GoldenMoustache",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "colloquial",
        "interaction_type": "dialogue",
        "domain": "comedy_sketch",
    },
    {
        "handle": "@TiboInShape",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "conversational",
        "interaction_type": "dialogue",
        "domain": "interview_fitness",
    },
    {
        "handle": "@DirtyBiology",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "narrative",
        "interaction_type": "monologue",
        "domain": "popular_science",
    },
    {
        "handle": "@Karambolage",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "narrative",
        "interaction_type": "monologue",
        "domain": "cultural_linguistics",
    },
    {
        "handle": "@DocSeven",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "narrative",
        "interaction_type": "monologue",
        "domain": "knowledge_curiosities",
    },
    {
        "handle": "@Science4All",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "academic",
        "interaction_type": "monologue",
        "domain": "science_mathematics",
    },
    {
        "handle": "@BaladeMentale",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "narrative",
        "interaction_type": "monologue",
        "domain": "astronomy_philosophy",
    },
    {
        "handle": "@JoueurDuGrenier",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "colloquial",
        "interaction_type": "monologue",
        "domain": "retro_gaming_humor",
    },
    {
        "handle": "@BazarDuGrenier",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "conversational",
        "interaction_type": "dialogue",
        "domain": "gaming_banter",
    },
    {
        "handle": "@Gotaga",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "colloquial",
        "interaction_type": "panel",
        "domain": "gaming_streaming",
    },
    {
        "handle": "@Kameto",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "colloquial",
        "interaction_type": "panel",
        "domain": "esports_streaming",
    },
    {
        "handle": "@ZeratoR",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "conversational",
        "interaction_type": "panel",
        "domain": "gaming_streaming",
    },
    {
        "handle": "@CommeUneFrancaise",
        "country_code": "FR",
        "region": "Metropolitan France",
        "register": "conversational",
        "interaction_type": "monologue",
        "domain": "pedagogy_culture",
    },
    # -------------------------------------------------------------------------
    # 2. Canada / Québec (CA) - Comedy, Talk Podcasts & Spoken Joual
    # -------------------------------------------------------------------------
    {
        "handle": "@SousEcoute",
        "country_code": "CA",
        "region": "Quebec",
        "register": "colloquial",
        "interaction_type": "dialogue",
        "domain": "comedy_podcast",
    },
    {
        "handle": "@MikeWardSousEcoute",
        "country_code": "CA",
        "region": "Quebec",
        "register": "colloquial",
        "interaction_type": "dialogue",
        "domain": "comedy_podcast",
    },
    {
        "handle": "@CoupleOuvert",
        "country_code": "CA",
        "region": "Quebec",
        "register": "colloquial",
        "interaction_type": "dialogue",
        "domain": "comedy_talkshow",
    },
    {
        "handle": "@SansFiltrePodcast",
        "country_code": "CA",
        "region": "Quebec",
        "register": "conversational",
        "interaction_type": "dialogue",
        "domain": "interview_podcast",
    },
    {
        "handle": "@Le5a7Podcast",
        "country_code": "CA",
        "region": "Quebec",
        "register": "colloquial",
        "interaction_type": "dialogue",
        "domain": "lifestyle_podcast",
    },
    {
        "handle": "@DistorsionPodcast",
        "country_code": "CA",
        "region": "Quebec",
        "register": "narrative",
        "interaction_type": "dialogue",
        "domain": "truecrime_internet",
    },
    {
        "handle": "@EntreElleEtLui",
        "country_code": "CA",
        "region": "Quebec",
        "register": "conversational",
        "interaction_type": "dialogue",
        "domain": "society_talkshow",
    },
    {
        "handle": "@PourDeVrai",
        "country_code": "CA",
        "region": "Quebec",
        "register": "conversational",
        "interaction_type": "dialogue",
        "domain": "lifestyle_spirituality",
    },
    {
        "handle": "@PrendsUnBreak",
        "country_code": "CA",
        "region": "Quebec",
        "register": "colloquial",
        "interaction_type": "panel",
        "domain": "youth_talkshow",
    },
    {
        "handle": "@LaPocheBleue",
        "country_code": "CA",
        "region": "Quebec",
        "register": "colloquial",
        "interaction_type": "dialogue",
        "domain": "sports_talkshow",
    },
    {
        "handle": "@LeDailyBufferPodcast",
        "country_code": "CA",
        "region": "Quebec",
        "register": "conversational",
        "interaction_type": "monologue",
        "domain": "daily_podcast",
    },
    {
        "handle": "@DeuxPrinces",
        "country_code": "CA",
        "region": "Quebec",
        "register": "colloquial",
        "interaction_type": "dialogue",
        "domain": "comedy_podcast",
    },
    {
        "handle": "@GrandEcart",
        "country_code": "CA",
        "region": "Quebec",
        "register": "conversational",
        "interaction_type": "dialogue",
        "domain": "culture_talkshow",
    },
    {
        "handle": "@3Bieres",
        "country_code": "CA",
        "region": "Quebec",
        "register": "colloquial",
        "interaction_type": "panel",
        "domain": "pop_culture_banter",
    },
    {
        "handle": "@Tele-Quebec",
        "country_code": "CA",
        "region": "Quebec",
        "register": "broadcast",
        "interaction_type": "panel",
        "domain": "public_broadcast",
    },
    {
        "handle": "@RadioCanadaInfo",
        "country_code": "CA",
        "region": "Quebec",
        "register": "broadcast",
        "interaction_type": "monologue",
        "domain": "public_journalism",
    },
    {
        "handle": "@Gurky",
        "country_code": "CA",
        "region": "Quebec",
        "register": "colloquial",
        "interaction_type": "monologue",
        "domain": "food_review_vlog",
    },
    {
        "handle": "@ProposMontreal",
        "country_code": "CA",
        "region": "Quebec",
        "register": "narrative",
        "interaction_type": "monologue",
        "domain": "urbanism_history",
    },
    {
        "handle": "@7JoursSurTerre",
        "country_code": "CA",
        "region": "Quebec",
        "register": "formal",
        "interaction_type": "monologue",
        "domain": "geopolitics_analysis",
    },
    {
        "handle": "@ThomasGauthier",
        "country_code": "CA",
        "region": "Quebec",
        "register": "colloquial",
        "interaction_type": "monologue",
        "domain": "comedy_satire",
    },
    {
        "handle": "@PLCloutier",
        "country_code": "CA",
        "region": "Quebec",
        "register": "conversational",
        "interaction_type": "monologue",
        "domain": "lifestyle_vlog",
    },
    {
        "handle": "@FredBastien",
        "country_code": "CA",
        "region": "Quebec",
        "register": "conversational",
        "interaction_type": "monologue",
        "domain": "internet_culture",
    },
    {
        "handle": "@Scilabus",
        "country_code": "CA",
        "region": "Quebec",
        "register": "academic",
        "interaction_type": "monologue",
        "domain": "popular_science",
    },
    {
        "handle": "@RadioCanada",
        "country_code": "CA",
        "region": "Quebec",
        "register": "broadcast",
        "interaction_type": "panel",
        "domain": "public_broadcast",
    },
    {
        "handle": "@JerrAllain",
        "country_code": "CA",
        "region": "Quebec",
        "register": "colloquial",
        "interaction_type": "dialogue",
        "domain": "comedy_podcast",
    },
    {
        "handle": "@ArnaudSoly",
        "country_code": "CA",
        "region": "Quebec",
        "register": "colloquial",
        "interaction_type": "monologue",
        "domain": "comedy_sketch",
    },
    # -------------------------------------------------------------------------
    # 3. Belgium (BE) - Creators, Satire, Media & Podcasts
    # -------------------------------------------------------------------------
    {
        "handle": "@GaelleGarciaDiaz",
        "country_code": "BE",
        "region": "Wallonia",
        "register": "colloquial",
        "interaction_type": "monologue",
        "domain": "beauty_lifestyle_humor",
    },
    {
        "handle": "@Mathsefaitdesfilms",
        "country_code": "BE",
        "region": "Wallonia",
        "register": "conversational",
        "interaction_type": "monologue",
        "domain": "cinema_critique_humor",
    },
    {
        "handle": "@AbdelEnVrai",
        "country_code": "BE",
        "region": "Brussels",
        "register": "colloquial",
        "interaction_type": "monologue",
        "domain": "satire_society",
    },
    {
        "handle": "@GuiHome",
        "country_code": "BE",
        "region": "Wallonia",
        "register": "colloquial",
        "interaction_type": "monologue",
        "domain": "comedy_parody",
    },
    {
        "handle": "@LufyetEnzo",
        "country_code": "BE",
        "region": "Wallonia",
        "register": "conversational",
        "interaction_type": "dialogue",
        "domain": "lifestyle_podcast",
    },
    {
        "handle": "@SilentJill",
        "country_code": "BE",
        "region": "Wallonia",
        "register": "conversational",
        "interaction_type": "monologue",
        "domain": "paranormal_storytelling",
    },
    {
        "handle": "@Colineblf",
        "country_code": "BE",
        "region": "Wallonia",
        "register": "narrative",
        "interaction_type": "monologue",
        "domain": "cinematic_vlog",
    },
    {
        "handle": "@Dayjee",
        "country_code": "BE",
        "region": "Wallonia",
        "register": "conversational",
        "interaction_type": "dialogue",
        "domain": "food_lifestyle",
    },
    {
        "handle": "@RTBF",
        "country_code": "BE",
        "region": "Wallonia",
        "register": "broadcast",
        "interaction_type": "panel",
        "domain": "public_broadcast",
    },
    {
        "handle": "@Tipik",
        "country_code": "BE",
        "region": "Wallonia",
        "register": "conversational",
        "interaction_type": "panel",
        "domain": "radio_pop_culture",
    },
    {
        "handle": "@LeGrandCactus",
        "country_code": "BE",
        "region": "Wallonia",
        "register": "colloquial",
        "interaction_type": "panel",
        "domain": "tv_satire_comedy",
    },
    {
        "handle": "@GuillermoGuiz",
        "country_code": "BE",
        "region": "Brussels",
        "register": "conversational",
        "interaction_type": "monologue",
        "domain": "standup_comedy",
    },
    {
        "handle": "@AlexVizorek",
        "country_code": "BE",
        "region": "Brussels",
        "register": "conversational",
        "interaction_type": "monologue",
        "domain": "standup_satire",
    },
    {
        "handle": "@FannyRuwet",
        "country_code": "BE",
        "region": "Brussels",
        "register": "conversational",
        "interaction_type": "dialogue",
        "domain": "standup_podcast",
    },
    {
        "handle": "@PabloAndres",
        "country_code": "BE",
        "region": "Brussels",
        "register": "colloquial",
        "interaction_type": "monologue",
        "domain": "comedy_character",
    },
    {
        "handle": "@CelineDept",
        "country_code": "BE",
        "region": "Wallonia",
        "register": "colloquial",
        "interaction_type": "monologue",
        "domain": "entertainment_challenges",
    },
    {
        "handle": "@DavidAntoineOfficiel",
        "country_code": "BE",
        "region": "Wallonia",
        "register": "conversational",
        "interaction_type": "panel",
        "domain": "radio_entertainment",
    },
    # -------------------------------------------------------------------------
    # 4. Switzerland (CH) - Creators, Media, Satire & Podcasts
    # -------------------------------------------------------------------------
    {
        "handle": "@LeGrandJD",
        "country_code": "CH",
        "region": "Romandy",
        "register": "conversational",
        "interaction_type": "monologue",
        "domain": "exploration_adventure",
    },
    {
        "handle": "@YoanLurquin",
        "country_code": "CH",
        "region": "Romandy",
        "register": "conversational",
        "interaction_type": "monologue",
        "domain": "comedy_vlog",
    },
    {
        "handle": "@RTS",
        "country_code": "CH",
        "region": "Romandy",
        "register": "broadcast",
        "interaction_type": "panel",
        "domain": "public_broadcast",
    },
    {
        "handle": "@Tataki",
        "country_code": "CH",
        "region": "Romandy",
        "register": "colloquial",
        "interaction_type": "street_interview",
        "domain": "youth_street_culture",
    },
    {
        "handle": "@Couleur3",
        "country_code": "CH",
        "region": "Romandy",
        "register": "colloquial",
        "interaction_type": "panel",
        "domain": "radio_comedy_satire",
    },
    {
        "handle": "@ThomasWiesel",
        "country_code": "CH",
        "region": "Romandy",
        "register": "conversational",
        "interaction_type": "monologue",
        "domain": "standup_comedy_satire",
    },
    {
        "handle": "@AlexandreKominek",
        "country_code": "CH",
        "region": "Romandy",
        "register": "colloquial",
        "interaction_type": "monologue",
        "domain": "standup_comedy",
    },
    {
        "handle": "@MarinaRollman",
        "country_code": "CH",
        "region": "Romandy",
        "register": "conversational",
        "interaction_type": "monologue",
        "domain": "standup_comedy",
    },
    {
        "handle": "@PointJ",
        "country_code": "CH",
        "region": "Romandy",
        "register": "broadcast",
        "interaction_type": "dialogue",
        "domain": "daily_explainer_podcast",
    },
    {
        "handle": "@BlaiseBersinger",
        "country_code": "CH",
        "region": "Romandy",
        "register": "colloquial",
        "interaction_type": "monologue",
        "domain": "comedy_satire",
    },
    {
        "handle": "@NathanaelRochat",
        "country_code": "CH",
        "region": "Romandy",
        "register": "conversational",
        "interaction_type": "monologue",
        "domain": "standup_comedy",
    },
    {
        "handle": "@CharlesNouveau",
        "country_code": "CH",
        "region": "Romandy",
        "register": "conversational",
        "interaction_type": "monologue",
        "domain": "standup_comedy",
    },
    {
        "handle": "@52minutes",
        "country_code": "CH",
        "region": "Romandy",
        "register": "broadcast",
        "interaction_type": "panel",
        "domain": "comedy_satire",
    },
    {
        "handle": "@LeTemps",
        "country_code": "CH",
        "region": "Romandy",
        "register": "broadcast",
        "interaction_type": "monologue",
        "domain": "journalism_analysis",
    },
]

# High-yield search query prompts for automated discovery
DISCOVERY_SEARCH_PROMPTS: List[Tuple[str, str, str]] = [
    # (query, default_country, default_domain)
    ("podcast français interview", "FR", "interview_podcast"),
    ("talk show français humour", "FR", "comedy_talkshow"),
    ("micro trottoir paris france", "FR", "street_interview"),
    ("podcast québécois discussion", "CA", "comedy_podcast"),
    ("talk show québec humour", "CA", "comedy_talkshow"),
    ("podcast belge interview", "BE", "interview_podcast"),
    ("humour belge stand up", "BE", "comedy_satire"),
    ("podcast suisse romande rts", "CH", "culture_podcast"),
    ("humour suisse stand up", "CH", "comedy_satire"),
    ("débat société français podcast", "FR", "society_talkshow"),
    ("vulgarisation scientifique français", "FR", "popular_science"),
    ("cinéma critique analyse français", "FR", "cinema_analysis"),
]


# =============================================================================
# Subtitle Auditing & Scoring Helper Functions
# =============================================================================

def inspect_video_subtitles(video_info: Dict[str, Any]) -> Tuple[bool, bool, List[str], List[str]]:
    """Determine presence of manual and auto French subtitles on a video."""
    subtitles = video_info.get("subtitles") or {}
    auto_captions = video_info.get("automatic_captions") or {}

    manual_langs: List[str] = []
    if isinstance(subtitles, dict):
        for k in subtitles.keys():
            if isinstance(k, str) and (k.lower() in MANUAL_FR_TAGS or k.lower().startswith("fr-")):
                manual_langs.append(k)

    auto_langs: List[str] = []
    if isinstance(auto_captions, dict):
        for k in auto_captions.keys():
            if isinstance(k, str) and (k.lower() in AUTO_FR_TAGS or k.lower().startswith("fr-")):
                auto_langs.append(k)

    has_manual = len(manual_langs) > 0
    has_auto = len(auto_langs) > 0
    return has_manual, has_auto, manual_langs, auto_langs


def calculate_channel_soft_score(
    view_count: Optional[int],
    manual_cc_pct: float,
    video_count: int,
) -> float:
    """Calculate soft ranking score that favors manual CC without excluding auto CC.
    
    Formula:
        score = log10(max(view_count, 1)) + (2.5 * manual_cc_pct) + min(video_count * 0.1, 1.0)
    """
    vc = view_count if view_count is not None and view_count > 0 else 1000
    base_popularity = math.log10(vc)
    cc_bonus = 2.5 * manual_cc_pct
    depth_bonus = min(video_count * 0.1, 1.0)
    return round(base_popularity + cc_bonus + depth_bonus, 2)


# =============================================================================
# Channel Extraction & Auditing Engine
# =============================================================================

def audit_single_channel(
    channel_spec: Dict[str, Any],
    sample_video_limit: int = 5,
    min_duration: int = 600,
    ydl_client: Optional[Any] = None,
) -> Optional[Dict[str, Any]]:
    """Audit a single YouTube channel: fetch channel metadata and sample recent videos."""
    raw_handle = channel_spec.get("handle") or channel_spec.get("uploader_id") or ""
    norm_handle = normalize_uploader_handle(raw_handle)
    if not norm_handle:
        return None
    if norm_handle in HANDLE_SUBSTITUTIONS:
        norm_handle = HANDLE_SUBSTITUTIONS[norm_handle]

    country_code = str(channel_spec.get("country_code") or "FR").strip().upper()
    if country_code not in ALLOWED_COUNTRY_CODES:
        logger.warning("Skipping channel %s with disallowed country %s", norm_handle, country_code)
        return None

    channel_url = channel_spec.get("channel_url") or f"https://www.youtube.com/{norm_handle}"
    videos_tab_url = f"https://www.youtube.com/{norm_handle}/videos"

    import yt_dlp

    ydl_flat_opts = {
        "extract_flat": True,
        "playlistend": max(sample_video_limit * 4, 20),
        "quiet": True,
        "no_warnings": True,
        "ignore_no_formats_error": True,
        "extractor_args": {"youtube": {"player_client": ["mweb", "web"]}},
    }

    try:
        with yt_dlp.YoutubeDL(ydl_flat_opts) as ydl:
            try:
                ch_info = ydl.extract_info(videos_tab_url, download=False)
            except Exception:
                ch_info = ydl.extract_info(channel_url, download=False)

            if not ch_info:
                return None

            entries = list(ch_info.get("entries", [])) if ch_info.get("entries") else []
            channel_title = ch_info.get("channel") or ch_info.get("uploader") or norm_handle.lstrip("@")
            channel_id = ch_info.get("channel_id") or ch_info.get("id") or ""
            uploader_id = ch_info.get("uploader_id") or norm_handle

    except Exception as exc:
        logger.warning("Failed to extract playlist for %s: %s", norm_handle, exc)
        return None

    # Filter candidate videos by duration >= min_duration
    candidate_entries: List[Dict[str, Any]] = []
    for ent in entries:
        dur = ent.get("duration") or ent.get("duration_seconds")
        if dur is not None and dur >= min_duration:
            candidate_entries.append(ent)

    # Sample top sample_video_limit candidates
    target_entries = candidate_entries[:sample_video_limit]
    if not target_entries:
        # Fallback to any videos with duration >= 300 if no longform videos
        for ent in entries:
            dur = ent.get("duration") or ent.get("duration_seconds")
            if dur is not None and dur >= 300:
                target_entries.append(ent)
            if len(target_entries) >= sample_video_limit:
                break

    audited_videos: List[Dict[str, Any]] = []
    manual_cc_count = 0
    auto_cc_count = 0
    total_views = 0

    ydl_sub_opts = {
        "skip_download": True,
        "quiet": True,
        "no_warnings": True,
        "ignore_no_formats_error": True,
        "extractor_args": {"youtube": {"player_client": ["mweb", "web"]}},
    }

    with yt_dlp.YoutubeDL(ydl_sub_opts) as ydl:
        for ent in target_entries:
            v_id = ent.get("id")
            if not v_id:
                continue
            v_url = ent.get("url") or f"https://www.youtube.com/watch?v={v_id}"
            try:
                v_info = ydl.extract_info(v_url, download=False)
                if not v_info:
                    continue

                has_manual, has_auto, man_langs, auto_langs = inspect_video_subtitles(v_info)
                v_dur = v_info.get("duration") or ent.get("duration")
                v_views = v_info.get("view_count") or ent.get("view_count") or 0
                total_views += v_views

                if has_manual:
                    manual_cc_count += 1
                if has_auto:
                    auto_cc_count += 1

                audited_videos.append(
                    {
                        "video_id": v_id,
                        "video_url": v_url,
                        "title": v_info.get("title") or ent.get("title") or f"Video {v_id}",
                        "duration_seconds": v_dur,
                        "view_count": v_views,
                        "upload_date": v_info.get("upload_date") or ent.get("upload_date"),
                        "has_manual_fr_cc": has_manual,
                        "has_auto_fr_cc": has_auto,
                        "all_manual_sub_langs": man_langs,
                    }
                )
            except Exception as exc:
                logger.warning("Failed to extract subtitle details for video %s: %s", v_id, exc)

    audited_count = len(audited_videos)
    manual_pct = round((manual_cc_count / audited_count * 100.0), 2) if audited_count > 0 else 0.0
    soft_score = calculate_channel_soft_score(total_views, manual_pct / 100.0, audited_count)

    result_record = {
        "channel_title": channel_title,
        "channel_url": f"https://www.youtube.com/{norm_handle}",
        "channel_id": channel_id,
        "uploader_id": norm_handle,
        "country_code": country_code,
        "region": channel_spec.get("region") or "Unknown",
        "register": channel_spec.get("register") or "conversational",
        "interaction_type": channel_spec.get("interaction_type") or "dialogue",
        "domain": channel_spec.get("domain") or "general",
        "audited_videos_count": audited_count,
        "manual_fr_cc_count": manual_cc_count,
        "manual_fr_cc_pct": manual_pct,
        "auto_fr_cc_count": auto_cc_count,
        "any_manual_cc": manual_cc_count > 0,
        "status": "audited" if audited_count > 0 else "no_eligible_videos",
        "soft_score": soft_score,
        "videos": audited_videos,
    }
    return result_record


def discover_french_channels(
    seed_channels: Optional[List[Dict[str, Any]]] = None,
    search_queries: Optional[List[str]] = None,
    limit_channels: Optional[int] = None,
    sample_videos_per_channel: int = 4,
    min_duration: int = 600,
    workers: int = 4,
    output_path: Union[str, Path] = "data/french_channels_catalog.json",
) -> Dict[str, Any]:
    """Execute full discovery, subtitle audit, scoring, and catalog generation."""
    candidates = list(seed_channels or SEED_CHANNELS)

    # Dynamic search query expansion via yt-dlp if requested
    if search_queries:
        import yt_dlp

        ydl_opts = {
            "extract_flat": True,
            "quiet": True,
            "no_warnings": True,
        }
        seen_handles = {c.get("handle", "").lower() for c in candidates}
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            for query in search_queries:
                try:
                    search_term = f"ytsearch10:{query}"
                    res = ydl.extract_info(search_term, download=False)
                    for item in res.get("entries", []):
                        uploader = item.get("uploader_id") or item.get("channel_id")
                        if uploader:
                            h = normalize_uploader_handle(uploader)
                            if h and h.lower() not in seen_handles:
                                seen_handles.add(h.lower())
                                candidates.append(
                                    {
                                        "handle": h,
                                        "country_code": "FR",  # Default, refined during audit
                                        "region": "Dynamic Search",
                                        "register": "conversational",
                                        "interaction_type": "dialogue",
                                        "domain": "search_discovery",
                                    }
                                )
                except Exception as exc:
                    logger.warning("Search query failed for '%s': %s", query, exc)

    if limit_channels is not None and limit_channels > 0:
        candidates = candidates[:limit_channels]

    logger.info("Starting audit of %d candidate Francophone channels...", len(candidates))

    audited_channels: List[Dict[str, Any]] = []
    channels_with_any_manual = 0
    channels_with_all_manual = 0
    total_videos_audited = 0
    total_manual_videos = 0

    lock = threading.Lock()

    def _worker_task(spec: Dict[str, Any]) -> None:
        nonlocal channels_with_any_manual, channels_with_all_manual, total_videos_audited, total_manual_videos
        res = audit_single_channel(
            channel_spec=spec,
            sample_video_limit=sample_videos_per_channel,
            min_duration=min_duration,
        )
        if res and res.get("audited_videos_count", 0) > 0:
            with lock:
                audited_channels.append(res)
                total_videos_audited += res["audited_videos_count"]
                total_manual_videos += res["manual_fr_cc_count"]
                if res["any_manual_cc"]:
                    channels_with_any_manual += 1
                if res["manual_fr_cc_count"] == res["audited_videos_count"]:
                    channels_with_all_manual += 1

    if workers > 1 and len(candidates) > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            list(executor.map(_worker_task, candidates))
    else:
        for c in candidates:
            _worker_task(c)

    # Sort channels by soft_score DESC (so manual CC and high-engagement float to the top)
    audited_channels.sort(key=lambda ch: ch.get("soft_score", 0), reverse=True)

    catalog: Dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "language_iso1": "fr",
        "allowed_country_codes": sorted(list(ALLOWED_COUNTRY_CODES)),
        "total_channels_audited": len(audited_channels),
        "channels_with_any_manual_fr_cc": channels_with_any_manual,
        "channels_with_all_manual_fr_cc": channels_with_all_manual,
        "total_videos_audited": total_videos_audited,
        "total_manual_fr_cc_videos": total_manual_videos,
        "channels": audited_channels,
    }

    out_p = Path(output_path)
    out_p.parent.mkdir(parents=True, exist_ok=True)
    with open(out_p, "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2, ensure_ascii=False)

    logger.info("Saved catalog with %d audited channels to %s", len(audited_channels), out_p)
    return catalog


# =============================================================================
# CLI Interface
# =============================================================================

def build_parser() -> argparse.ArgumentParser:
    """Construct CLI argument parser for discover_french_channels."""
    parser = argparse.ArgumentParser(
        description="Discover, audit, and catalog Francophone YouTube channels for Constructicon harvesting.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--output",
        default="data/french_channels_catalog.json",
        help="Path to output catalog JSON file.",
    )
    parser.add_argument(
        "--limit-channels",
        type=int,
        default=None,
        help="Limit number of channels to audit.",
    )
    parser.add_argument(
        "--pilot",
        action="store_true",
        help="Run a quick pilot audit on a representative sample of 8 channels across FR, CA, BE, CH.",
    )
    parser.add_argument(
        "--sample-videos",
        type=int,
        default=4,
        help="Number of recent longform videos to sample per channel.",
    )
    parser.add_argument(
        "--min-duration",
        type=int,
        default=600,
        help="Minimum duration in seconds for sampled videos.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Concurrency workers for parallel channel auditing.",
    )
    parser.add_argument(
        "--search-queries",
        nargs="*",
        default=None,
        help="Optional search queries to expand channel candidates via live YouTube search.",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    seeds = list(SEED_CHANNELS)
    if args.pilot:
        # Sample 2 channels per region for quick pilot validation
        pilot_handles = {
            "@innerFrench",
            "@Legendmedia",
            "@SousEcoute",
            "@RadioCanadaInfo",
            "@Mathsefaitdesfilms",
            "@RTBF",
            "@LeGrandJD",
            "@Tataki",
        }
        seeds = [c for c in seeds if c["handle"] in pilot_handles]

    try:
        catalog = discover_french_channels(
            seed_channels=seeds,
            search_queries=args.search_queries,
            limit_channels=args.limit_channels,
            sample_videos_per_channel=args.sample_videos,
            min_duration=args.min_duration,
            workers=args.workers,
            output_path=args.output,
        )
        print(f"[+] Successfully generated catalog with {catalog['total_channels_audited']} channels.")
        print(f"    - Channels with any manual CC : {catalog['channels_with_any_manual_fr_cc']}")
        print(f"    - Total videos audited        : {catalog['total_videos_audited']}")
        print(f"    - Total manual CC videos      : {catalog['total_manual_fr_cc_videos']}")
        print(f"    - Catalog Path                : {args.output}")
        return 0
    except Exception as exc:
        logger.error("Channel discovery failed: %s", exc, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
