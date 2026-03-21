"""
scraper_imdb.py
---------------
Scrapes IMDb Top 250 Movies and Top 50 TV Shows.
Saves raw data to data/raw/imdb_movies_raw.csv and imdb_shows_raw.csv

Run: python scrapers/scraper_imdb.py
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import os
import json
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

BASE_URL = "https://www.imdb.com"
RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)


def get_soup(url: str, retries: int = 3) -> BeautifulSoup | None:
    """Fetch a page and return BeautifulSoup object with retry logic."""
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=15)
            response.raise_for_status()
            time.sleep(random.uniform(1.5, 3.0))   # polite delay
            return BeautifulSoup(response.text, "lxml")
        except requests.RequestException as e:
            log.warning(f"Attempt {attempt} failed for {url}: {e}")
            if attempt < retries:
                time.sleep(5 * attempt)
    log.error(f"All {retries} attempts failed for {url}")
    return None


def scrape_imdb_top250_movies() -> pd.DataFrame:
    """
    Scrapes the IMDb Top 250 movies list.
    Returns a DataFrame with: rank, title, year, imdb_rating, votes, runtime_min,
                               certificate, genres, imdb_id, plot
    """
    log.info("Scraping IMDb Top 250 Movies...")
    url = f"{BASE_URL}/chart/top/"
    soup = get_soup(url)
    if not soup:
        log.error("Failed to fetch Top 250 page.")
        return pd.DataFrame()

    # IMDb Top 250 embeds data in a JSON-LD script tag — more reliable than HTML parsing
    script_tag = soup.find("script", {"id": "\\__NEXT_DATA__"}) or \
                 soup.find("script", type="application/ld+json")

    records = []

    # Primary method: parse the visible list items
    items = soup.select("li.ipc-metadata-list-summary-item")
    if not items:
        # Fallback selector for older IMDb layout
        items = soup.select("td.titleColumn")

    log.info(f"Found {len(items)} list items on the page.")

    for idx, item in enumerate(items, start=1):
        try:
            # Title and year
            title_tag = item.select_one("h3.ipc-title__text") or item.select_one("a")
            title_raw = title_tag.get_text(strip=True) if title_tag else ""
            # IMDb prepends rank number to title text e.g. "1. The Shawshank Redemption"
            title = title_raw.split(". ", 1)[-1] if ". " in title_raw else title_raw

            # Year
            year_tag = item.select_one("span.cli-title-metadata-item")
            year = year_tag.get_text(strip=True) if year_tag else ""

            # Rating
            rating_tag = item.select_one("span.ipc-rating-star--imdb") or \
                         item.select_one('[data-testid="ratingGroup--imdb-rating"] span')
            rating_text = rating_tag.get_text(strip=True) if rating_tag else "0"
            # Rating text may look like "9.3(2.8M)" 
            rating = float(rating_text.split("(")[0]) if rating_text.split() else 0.0

            # Votes — often in aria-label
            votes_label = item.select_one('[data-testid="ratingGroup--imdb-rating"]')
            votes_text = votes_label.get("aria-label", "") if votes_label else ""
            votes = parse_votes(votes_text)

            # IMDb ID from link href
            link_tag = item.select_one("a.ipc-title-link-wrapper") or item.select_one("a")
            href = link_tag["href"] if link_tag and link_tag.get("href") else ""
            imdb_id = href.split("/")[2] if "/title/" in href else ""

            records.append({
                "rank": idx,
                "imdb_id": imdb_id,
                "title": title,
                "year": clean_year(year),
                "imdb_rating": rating,
                "votes": votes,
                "content_type": "movie",
            })
        except Exception as e:
            log.warning(f"Row {idx} parse error: {e}")
            continue

    df = pd.DataFrame(records)
    log.info(f"Scraped {len(df)} movies from Top 250 list.")

    # Enrich top 100 with detail page data (runtime, genres, certificate, plot)
    df = enrich_with_detail_pages(df, sample=100)

    out_path = os.path.join(RAW_DIR, "imdb_movies_raw.csv")
    df.to_csv(out_path, index=False)
    log.info(f"Saved to {out_path}")
    return df


def scrape_imdb_top_tv() -> pd.DataFrame:
    """
    Scrapes IMDb Top Rated TV Shows (top 50).
    Returns a DataFrame with same schema as movies.
    """
    log.info("Scraping IMDb Top Rated TV Shows...")
    url = f"{BASE_URL}/chart/toptv/"
    soup = get_soup(url)
    if not soup:
        log.error("Failed to fetch Top TV page.")
        return pd.DataFrame()

    items = soup.select("li.ipc-metadata-list-summary-item")
    records = []

    for idx, item in enumerate(items[:50], start=1):
        try:
            title_tag = item.select_one("h3.ipc-title__text")
            title_raw = title_tag.get_text(strip=True) if title_tag else ""
            title = title_raw.split(". ", 1)[-1] if ". " in title_raw else title_raw

            year_spans = item.select("span.cli-title-metadata-item")
            year = year_spans[0].get_text(strip=True) if year_spans else ""

            rating_tag = item.select_one("span.ipc-rating-star--imdb")
            rating_text = rating_tag.get_text(strip=True) if rating_tag else "0"
            rating = float(rating_text.split("(")[0]) if rating_text.split() else 0.0

            votes_label = item.select_one('[data-testid="ratingGroup--imdb-rating"]')
            votes_text = votes_label.get("aria-label", "") if votes_label else ""
            votes = parse_votes(votes_text)

            link_tag = item.select_one("a.ipc-title-link-wrapper")
            href = link_tag["href"] if link_tag and link_tag.get("href") else ""
            imdb_id = href.split("/")[2] if "/title/" in href else ""

            records.append({
                "rank": idx,
                "imdb_id": imdb_id,
                "title": title,
                "year": clean_year(year),
                "imdb_rating": rating,
                "votes": votes,
                "content_type": "tv_show",
            })
        except Exception as e:
            log.warning(f"TV Row {idx} parse error: {e}")
            continue

    df = pd.DataFrame(records)
    log.info(f"Scraped {len(df)} TV shows.")
    df = enrich_with_detail_pages(df, sample=50)

    out_path = os.path.join(RAW_DIR, "imdb_shows_raw.csv")
    df.to_csv(out_path, index=False)
    log.info(f"Saved to {out_path}")
    return df


def enrich_with_detail_pages(df: pd.DataFrame, sample: int = 50) -> pd.DataFrame:
    """
    Visit each title's detail page to extract:
    runtime_min, genres (pipe-separated), certificate (U/UA/A/R), plot
    Only processes up to `sample` rows to stay within polite scraping limits.
    """
    log.info(f"Enriching {min(sample, len(df))} titles with detail page data...")

    runtime_list, genres_list, cert_list, plot_list = [], [], [], []

    for i, row in df.head(sample).iterrows():
        if not row.get("imdb_id"):
            runtime_list.append(None)
            genres_list.append(None)
            cert_list.append(None)
            plot_list.append(None)
            continue

        detail_url = f"{BASE_URL}/title/{row['imdb_id']}/"
        soup = get_soup(detail_url)

        if not soup:
            runtime_list.append(None)
            genres_list.append(None)
            cert_list.append(None)
            plot_list.append(None)
            continue

        # Runtime
        runtime = extract_runtime(soup)
        runtime_list.append(runtime)

        # Genres — IMDb 2024/2025 layout uses multiple selector patterns
        genres = extract_genres(soup)
        genres_list.append(genres)

        # Certificate — try multiple selector patterns
        cert = extract_certificate(soup)
        cert_list.append(cert)

        # Plot — try multiple selector patterns
        plot = extract_plot(soup)
        plot_list.append(plot)

        log.info(f"  [{i+1}/{min(sample, len(df))}] {row['title']} — runtime: {runtime}m, genres: {genres}")

    # Pad lists to match full df length
    pad = len(df) - sample
    runtime_list += [None] * pad
    genres_list  += [None] * pad
    cert_list    += [None] * pad
    plot_list    += [None] * pad

    df["runtime_min"] = runtime_list
    df["genres"]      = genres_list
    df["certificate"] = cert_list
    df["plot"]        = plot_list

    return df


def extract_genres(soup: BeautifulSoup) -> str | None:
    """
    Extract pipe-separated genre string from an IMDb detail page.
    Priority: JSON-LD (most reliable) → genre link URLs → chip selectors.
    Filters out IMDb's "Atmospheric", "Cinematography" mood/keyword tags —
    we only want standard genre labels like "Drama", "Crime", "Action".
    """
    # Canonical IMDb genre list — anything outside this is a mood/keyword tag
    VALID_GENRES = {
        "Action", "Adventure", "Animation", "Biography", "Comedy", "Crime",
        "Documentary", "Drama", "Family", "Fantasy", "Film-Noir", "History",
        "Horror", "Music", "Musical", "Mystery", "News", "Reality-TV",
        "Romance", "Sci-Fi", "Short", "Sport", "Talk-Show", "Thriller",
        "War", "Western", "Animation", "Game-Show"
    }

    def filter_genres(raw_list: list[str]) -> list[str]:
        """Keep only recognised IMDb genres, deduplicated."""
        seen, result = set(), []
        for g in raw_list:
            g = g.strip()
            # Accept exact match OR case-insensitive match
            matched = next((v for v in VALID_GENRES if v.lower() == g.lower()), None)
            if matched and matched not in seen:
                seen.add(matched)
                result.append(matched)
        return result

    # ── Method 1: JSON-LD <script> tag — most reliable, IMDb injects this themselves
    import json
    script = soup.find("script", {"type": "application/ld+json"})
    if script and script.string:
        try:
            data = json.loads(script.string)
            genre_val = data.get("genre", [])
            raw = genre_val if isinstance(genre_val, list) else [genre_val]
            genres = filter_genres([g for g in raw if g])
            if genres:
                return "|".join(genres)
        except Exception:
            pass

    # ── Method 2: genre links in the URL pattern /search/title/?genres=
    tags = soup.select('a[href*="/search/title/?genres="]')
    if tags:
        raw = [t.get_text(strip=True) for t in tags]
        genres = filter_genres(raw)
        if genres:
            return "|".join(genres)

    # ── Method 3: data-testid="genres" chips (may include mood tags — filter them)
    tags = soup.select('[data-testid="genres"] span.ipc-chip__text')
    if tags:
        raw = [t.get_text(strip=True) for t in tags]
        genres = filter_genres(raw)
        if genres:
            return "|".join(genres)
        # If none passed filter, return raw first 5 (better than None)
        return "|".join(raw[:5])

    return None


def extract_certificate(soup: BeautifulSoup) -> str | None:
    """Extract content rating certificate from IMDb detail page."""
    # Pattern 1: data-testid attribute
    tag = soup.select_one('[data-testid="certificate"] span') or \
          soup.select_one('[data-testid="certificate"]')
    if tag:
        return tag.get_text(strip=True)

    # Pattern 2: JSON-LD
    import json
    script = soup.find("script", {"type": "application/ld+json"})
    if script:
        try:
            data = json.loads(script.string)
            rating = data.get("contentRating")
            if rating:
                return str(rating)
        except Exception:
            pass

    # Pattern 3: tech specs list
    for li in soup.select("li.ipc-inline-list__item"):
        text = li.get_text(strip=True)
        if text in {"G", "PG", "PG-13", "R", "NC-17", "U", "UA", "A",
                    "U/A", "TV-MA", "TV-14", "TV-PG", "TV-G", "Not Rated"}:
            return text

    return None


def extract_plot(soup: BeautifulSoup) -> str | None:
    """Extract plot summary from IMDb detail page."""
    # Pattern 1: data-testid="plot"
    tag = soup.select_one('[data-testid="plot"] span[data-testid="plot-xl"]') or \
          soup.select_one('[data-testid="plot"] span[role="presentation"]') or \
          soup.select_one('[data-testid="plot"] p span') or \
          soup.select_one('[data-testid="plot"]')
    if tag:
        text = tag.get_text(strip=True)
        if len(text) > 20:
            return text

    # Pattern 2: JSON-LD description
    import json
    script = soup.find("script", {"type": "application/ld+json"})
    if script:
        try:
            data = json.loads(script.string)
            desc = data.get("description", "")
            if desc and len(desc) > 20:
                return desc
        except Exception:
            pass

    # Pattern 3: og:description meta tag
    meta = soup.find("meta", {"property": "og:description"}) or \
           soup.find("meta", {"name": "description"})
    if meta and meta.get("content"):
        return meta["content"].strip()

    return None


def extract_runtime(soup: BeautifulSoup) -> int | None:
    """Extract runtime in minutes from a detail page soup."""
    # Modern IMDb layout uses a <ul> of tech specs
    tech_items = soup.select("ul.ipc-inline-list li.ipc-inline-list__item")
    for item in tech_items:
        text = item.get_text(strip=True)
        if "h" in text and "m" in text:
            return parse_runtime_text(text)
        if text.endswith("m") and text[:-1].isdigit():
            return int(text[:-1])
    return None


def parse_runtime_text(text: str) -> int | None:
    """Convert '2h 22m' or '142m' to integer minutes."""
    try:
        hours = 0
        minutes = 0
        if "h" in text:
            parts = text.split("h")
            hours = int(parts[0].strip())
            m_part = parts[1].replace("m", "").strip()
            minutes = int(m_part) if m_part else 0
        elif "m" in text:
            minutes = int(text.replace("m", "").strip())
        return hours * 60 + minutes
    except (ValueError, IndexError):
        return None


def parse_votes(text: str) -> int:
    """Parse vote strings like '2.8M', '450K', or '(1,234,567)' into integers."""
    if not text:
        return 0
    # Extract numeric part
    import re
    match = re.search(r"([\d,.]+)\s*([KkMm]?)", text.replace(",", ""))
    if not match:
        return 0
    num_str, suffix = match.groups()
    try:
        num = float(num_str)
        if suffix.upper() == "M":
            return int(num * 1_000_000)
        elif suffix.upper() == "K":
            return int(num * 1_000)
        return int(num)
    except ValueError:
        return 0


def clean_year(year_text: str) -> int | None:
    """Extract 4-digit year from strings like '1994', '1994–1996', '2022– '."""
    import re
    match = re.search(r"\b(19|20)\d{2}\b", year_text)
    return int(match.group()) if match else None


if __name__ == "__main__":
    movies_df = scrape_imdb_top250_movies()
    shows_df  = scrape_imdb_top_tv()
    print(f"\nMovies scraped: {len(movies_df)}")
    print(f"TV Shows scraped: {len(shows_df)}")
    print("\nSample movie row:")
    if not movies_df.empty:
        print(movies_df.head(2).to_string())
