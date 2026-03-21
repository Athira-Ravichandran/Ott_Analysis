"""
scraper_justwatch.py  (v3 — TMDB API approach)
------------------------------------------------
JustWatch now requires browser session tokens — not scrapable without a browser.

This version uses the FREE TMDB (The Movie Database) API instead, which provides:
  - Watch provider data (Netflix / Prime / Hotstar) for every title
  - No auth token issues
  - Official, documented, free API

Steps to get your free API key (2 minutes):
  1. Go to https://www.themoviedb.org/signup  → create free account
  2. Go to https://www.themoviedb.org/settings/api → click "Create" under Developer
  3. Fill the form (say "personal project / portfolio"), copy your API Read Access Token
  4. Paste it in your .env file as:  TMDB_API_KEY=your_token_here

Run: python scrapers/scraper_justwatch.py
"""

import requests
import pandas as pd
import time
import random
import os
import logging
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)

TMDB_BASE    = "https://api.themoviedb.org/3"
TMDB_API_KEY = os.getenv("TMDB_API_KEY", "")   # Bearer token from .env

# TMDB watch provider IDs for India (region=IN)
# Full list: https://api.themoviedb.org/3/watch/providers/movie?watch_region=IN
PROVIDER_MAP = {
    8:    "Netflix",
    119:  "Amazon Prime Video",
    122:  "Hotstar",
    337:  "Disney+ Hotstar",
    350:  "Apple TV+",
    283:  "Crunchyroll",
    531:  "Paramount+",
    1773: "JioCinema",
    232:  "ZEE5",
    315:  "SonyLIV",
    11:   "MUBI",
    384:  "HBO Max",
    386:  "Peacock",
}

HEADERS_TMDB = {
    "accept": "application/json",
}


def get_headers():
    """Return auth headers. Supports both Bearer token and v3 API key."""
    if TMDB_API_KEY.startswith("ey"):
        # JWT / Bearer token (Read Access Token)
        return {**HEADERS_TMDB, "Authorization": f"Bearer {TMDB_API_KEY}"}
    else:
        # Legacy v3 API key — append to URL params instead
        return HEADERS_TMDB


def api_params(extra: dict = None) -> dict:
    """Build query params — add api_key if not using Bearer token."""
    params = extra or {}
    if TMDB_API_KEY and not TMDB_API_KEY.startswith("ey"):
        params["api_key"] = TMDB_API_KEY
    return params


def search_tmdb(title: str, year: int | None, content_type: str) -> int | None:
    """
    Search TMDB for a title and return its TMDB ID.
    content_type: 'movie' or 'tv_show'
    """
    endpoint  = "search/movie" if content_type == "movie" else "search/tv"
    query_key = "query"
    params    = api_params({"query": title, "language": "en-US", "page": 1})
    if year:
        params["year" if content_type == "movie" else "first_air_date_year"] = year

    try:
        resp = requests.get(
            f"{TMDB_BASE}/{endpoint}",
            headers=get_headers(),
            params=params,
            timeout=10,
        )
        resp.raise_for_status()
        results = resp.json().get("results", [])
        if results:
            return results[0]["id"]
    except Exception as e:
        log.debug(f"TMDB search error for '{title}': {e}")
    return None


def get_watch_providers(tmdb_id: int, content_type: str) -> list[dict]:
    """
    Fetch watch providers for a title from TMDB (India region).
    Returns list of {platform, monetization, provider_id}
    """
    endpoint = f"movie/{tmdb_id}/watch/providers" if content_type == "movie" \
               else f"tv/{tmdb_id}/watch/providers"
    try:
        resp = requests.get(
            f"{TMDB_BASE}/{endpoint}",
            headers=get_headers(),
            params=api_params(),
            timeout=10,
        )
        resp.raise_for_status()
        results = resp.json().get("results", {})

        # Try IN first, fall back to US if not available for India
        for region in ["IN", "US"]:
            region_data = results.get(region, {})
            if not region_data:
                continue

            offers     = []
            seen       = set()
            # flatrate = subscription (Netflix, Prime), rent, buy, free, ads
            for mono_type in ["flatrate", "free", "ads", "rent", "buy"]:
                for provider in region_data.get(mono_type, []):
                    pid   = provider.get("provider_id")
                    pname = PROVIDER_MAP.get(pid, provider.get("provider_name", f"Provider_{pid}"))
                    if pname not in seen:
                        seen.add(pname)
                        offers.append({
                            "platform":     pname,
                            "provider_id":  pid,
                            "monetization": mono_type,
                            "presentation": "hd",
                            "region":       region,
                        })
            if offers:
                return offers

    except Exception as e:
        log.debug(f"TMDB provider error for ID {tmdb_id}: {e}")
    return []


def build_platform_data(titles_df: pd.DataFrame) -> pd.DataFrame:
    """
    Main function: for every scraped title, look up platforms via TMDB.
    Returns long-format DataFrame: one row per (title × platform).
    """
    if not TMDB_API_KEY:
        log.error(
            "\n" + "="*60 +
            "\n  TMDB_API_KEY not set in .env file!" +
            "\n  Get your free key at: https://www.themoviedb.org/settings/api" +
            "\n  Then add this line to your .env file:" +
            "\n  TMDB_API_KEY=your_read_access_token_here" +
            "\n" + "="*60
        )
        # Return a dummy dataframe so pipeline doesn't crash
        dummy = titles_df[["title", "imdb_id", "content_type"]].copy()
        dummy["platform"]       = "Not Available"
        dummy["platform_group"] = "Not Available"
        dummy["monetization"]   = ""
        dummy["presentation"]   = ""
        out_path = os.path.join(RAW_DIR, "justwatch_platforms_raw.csv")
        dummy.to_csv(out_path, index=False)
        return dummy

    log.info(f"Fetching TMDB watch providers for {len(titles_df)} titles (region: IN)...")
    records     = []
    found_count = 0

    for i, row in titles_df.iterrows():
        title        = row["title"]
        year         = int(row["year"]) if pd.notna(row.get("year")) else None
        content_type = "tv" if row.get("content_type") == "tv_show" else "movie"

        # Step 1: find TMDB ID
        tmdb_id = search_tmdb(title, year, content_type)
        if not tmdb_id:
            log.info(f"  [{i}] '{title}' — not found on TMDB")
            records.append({
                "title": title, "imdb_id": row.get("imdb_id", ""),
                "content_type": row.get("content_type", "movie"),
                "platform": "Not Available", "platform_group": "Not Available",
                "monetization": "", "presentation": "", "region": "",
            })
            time.sleep(random.uniform(0.4, 0.8))
            continue

        # Step 2: get watch providers
        offers = get_watch_providers(tmdb_id, content_type)

        if offers:
            found_count += 1
            for offer in offers:
                records.append({
                    "title":        title,
                    "imdb_id":      row.get("imdb_id", ""),
                    "content_type": row.get("content_type", "movie"),
                    "platform":     offer["platform"],
                    "platform_group": offer["platform"],
                    "monetization": offer["monetization"],
                    "presentation": offer["presentation"],
                    "region":       offer.get("region", "IN"),
                })
            platform_names = list({o["platform"] for o in offers})
            log.info(f"  [{i}] '{title}' → {', '.join(platform_names[:4])}")
        else:
            records.append({
                "title": title, "imdb_id": row.get("imdb_id", ""),
                "content_type": row.get("content_type", "movie"),
                "platform": "Not Available", "platform_group": "Not Available",
                "monetization": "", "presentation": "", "region": "",
            })
            log.info(f"  [{i}] '{title}' — no IN/US providers found")

        time.sleep(random.uniform(0.3, 0.7))   # TMDB rate limit is generous (40 req/10s)

    df = pd.DataFrame(records)
    out_path = os.path.join(RAW_DIR, "justwatch_platforms_raw.csv")
    df.to_csv(out_path, index=False)
    log.info(f"\nSaved {len(df)} rows. Found providers for {found_count}/{len(titles_df)} titles.")
    return df


if __name__ == "__main__":
    movies_path = os.path.join(RAW_DIR, "imdb_movies_raw.csv")
    shows_path  = os.path.join(RAW_DIR, "imdb_shows_raw.csv")

    dfs = []
    if os.path.exists(movies_path):
        dfs.append(pd.read_csv(movies_path))
    if os.path.exists(shows_path):
        dfs.append(pd.read_csv(shows_path))

    if not dfs:
        log.error("No IMDb data found. Run scraper_imdb.py first.")
        exit(1)

    combined = pd.concat(dfs, ignore_index=True)
    log.info(f"Loaded {len(combined)} titles.")
    platform_df = build_platform_data(combined)
    print(f"\nPlatform distribution:\n{platform_df['platform'].value_counts().head(10)}")
