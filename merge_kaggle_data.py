

import pandas as pd
import numpy as np
import os
import mysql.connector
import logging
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

CLEANED_DIR = os.path.join(os.path.dirname(__file__), "data", "cleaned")
RAW_DIR     = os.path.join(os.path.dirname(__file__), "data", "raw")

DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     int(os.getenv("DB_PORT", "3306")),
    "user":     os.getenv("DB_USER",     "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME",     "ott_pulse"),
}

SUBGENRE_MAP = {
    "Action Epic":"Action","Gun Fu":"Action","Martial Arts":"Action",
    "Superhero":"Action","Sword & Sorcery":"Action","Samurai":"Action",
    "Adventure Epic":"Adventure","Quest":"Adventure","Urban Adventure":"Adventure",
    "Hand-Drawn Animation":"Animation","Adult Animation":"Animation","Anime":"Animation",
    "Docudrama":"Biography","True Crime":"Crime",
    "Docuseries":"Documentary","Nature Documentary":"Documentary",
    "History Documentary":"Documentary","Sports Documentary":"Documentary",
    "Science & Technology Documentary":"Documentary","Military Documentary":"Documentary",
    "Dark Comedy":"Comedy","Parody":"Comedy","Satire":"Comedy","Sitcom":"Comedy",
    "Gangster":"Crime","Cop Drama":"Crime","Drug Crime":"Crime",
    "Police Procedural":"Crime","Whodunnit":"Mystery","Serial Killer":"Thriller",
    "Legal Drama":"Drama","Medical Drama":"Drama","Psychological Drama":"Drama",
    "Period Drama":"Drama","Prison Drama":"Drama","Coming-of-Age":"Drama",
    "Epic":"Drama","Historical Epic":"History","Romantic Epic":"Romance",
    "Dark Fantasy":"Fantasy","Fantasy Epic":"Fantasy","Supernatural Fantasy":"Fantasy",
    "Psychological Horror":"Horror","Supernatural Horror":"Horror","Body Horror":"Horror",
    "Tragic Romance":"Romance","Feel-Good Romance":"Romance",
    "Sci-Fi Epic":"Sci-Fi","Space Sci-Fi":"Sci-Fi","Dystopian Sci-Fi":"Sci-Fi",
    "Cyberpunk":"Sci-Fi","Time Travel":"Sci-Fi","Artificial Intelligence":"Sci-Fi",
    "Psychological Thriller":"Thriller","Disaster":"Thriller",
    "War Epic":"War","Spaghetti Western":"Western","Western Epic":"Western",
    "Holiday Family":"Family","Animal Adventure":"Family",
    "Musical":"Music","Basketball":"Documentary",
}

STANDARD_GENRES = {
    "Action","Adventure","Animation","Biography","Comedy","Crime",
    "Documentary","Drama","Family","Fantasy","Film-Noir","History",
    "Horror","Music","Musical","Mystery","Romance","Sci-Fi",
    "Sport","Thriller","War","Western",
}

PLATFORM_KEYWORDS = {
    "netflix":   "Netflix",
    "prime":     "Amazon Prime Video",
    "amazon":    "Amazon Prime Video",
    "disney":    "Disney+ Hotstar",
    "hotstar":   "Disney+ Hotstar",
    "apple":     "Apple TV+",
    "hulu":      "Hulu",
    "hbo":       "HBO Max",
    "peacock":   "Peacock",
    "paramount": "Paramount+",
}


def map_genre(g: str) -> str | None:
    g = g.strip()
    if g in STANDARD_GENRES:
        return g
    return SUBGENRE_MAP.get(g)


def clean_genres(raw: str) -> str:
    if not raw or pd.isna(raw):
        return "Unknown"
    parts = [p.strip() for p in str(raw).replace(",", "|").split("|") if p.strip()]
    result, seen = [], set()
    for p in parts:
        mapped = map_genre(p)
        if mapped and mapped not in seen:
            seen.add(mapped)
            result.append(mapped)
    return "|".join(result) if result else "Unknown"


def detect_platform(filename: str) -> str:
    fn = filename.lower()
    for key, label in PLATFORM_KEYWORDS.items():
        if key in fn:
            return label
    return "Unknown"


def load_kaggle_dataset(path: str, platform_hint: str = "Netflix") -> pd.DataFrame:
    
    df = pd.read_csv(path, low_memory=False)
    log.info(f"Loaded Kaggle file: {path} — {len(df)} rows, columns: {list(df.columns)[:8]}")

    cols = [c.lower().strip() for c in df.columns]
    df.columns = cols

    records = []

    # ── Format A: Victor Soeiro (has imdb_score) ──────────────
    if "imdb_score" in cols:
        df = df[df["imdb_score"].notna()].copy()
        df = df[df["imdb_score"] >= 5.0]       # filter out very low-rated filler
        # Sample up to 500 to keep dataset manageable
        df = df.sample(min(500, len(df)), random_state=42)

        for _, row in df.iterrows():
            genres_raw = str(row.get("genres", "")).replace("'", "").replace("[", "").replace("]", "")
            genres_clean = clean_genres(genres_raw)
            content_type = "tv_show" if str(row.get("type", "")).upper() in ("SHOW", "TV SHOW") else "movie"
            runtime = row.get("runtime")
            records.append({
                "imdb_id":      f"KGL_{row.get('id', row.name)}",
                "title":        str(row.get("title", "Unknown")).strip(),
                "content_type": content_type,
                "year":         int(row["release_year"]) if pd.notna(row.get("release_year")) else None,
                "imdb_rating":  float(row["imdb_score"]),
                "votes":        int(row["imdb_votes"]) if pd.notna(row.get("imdb_votes")) else 10000,
                "runtime_min":  int(runtime) if pd.notna(runtime) else None,
                "genres":       genres_clean,
                "genre_primary":genres_clean.split("|")[0] if genres_clean != "Unknown" else "Unknown",
                "certificate":  str(row.get("age_certification", "")).strip() or "Unknown",
                "platform":     platform_hint,
                "source":       "kaggle",
            })

    # ── Format B: Shivam Bansal (no IMDb score — use tmdb_score or skip) ─
    elif "listed_in" in cols:
        df = df.sample(min(300, len(df)), random_state=42)
        for _, row in df.iterrows():
            genres_raw = str(row.get("listed_in", ""))
            genres_clean = clean_genres(genres_raw)
            content_type = "tv_show" if "TV" in str(row.get("type", "")).upper() else "movie"
            # No rating column — assign median placeholder (7.0)
            records.append({
                "imdb_id":      f"KGL_{row.get('show_id', row.name)}",
                "title":        str(row.get("title", "Unknown")).strip(),
                "content_type": content_type,
                "year":         int(row["release_year"]) if pd.notna(row.get("release_year")) else None,
                "imdb_rating":  7.0,
                "votes":        10000,
                "runtime_min":  None,
                "genres":       genres_clean,
                "genre_primary":genres_clean.split("|")[0] if genres_clean != "Unknown" else "Unknown",
                "certificate":  str(row.get("rating", "")).strip() or "Unknown",
                "platform":     platform_hint,
                "source":       "kaggle",
            })
    else:
        log.warning(f"Unrecognised Kaggle format — columns: {cols}")
        return pd.DataFrame()

    result = pd.DataFrame(records)
    log.info(f"Parsed {len(result)} usable rows from Kaggle dataset.")
    return result


def enrich_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add all derived columns so schema matches titles table."""
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["imdb_rating"] = pd.to_numeric(df["imdb_rating"], errors="coerce").fillna(7.0)
    df["votes"] = pd.to_numeric(df["votes"], errors="coerce").fillna(10000).astype(int)
    df["runtime_min"] = pd.to_numeric(df["runtime_min"], errors="coerce")

    # Decade
    df["decade"] = ((df["year"] // 10) * 10).astype("Int64").astype(str) + "s"
    df.loc[df["year"].isna(), "decade"] = "Unknown"

    # Era
    def era(y):
        if pd.isna(y): return "Unknown"
        if y >= 2020:  return "2020s"
        elif y >= 2015: return "2015–2019"
        elif y >= 2010: return "2010–2014"
        elif y >= 2000: return "2000s"
        elif y >= 1990: return "1990s"
        else:           return "Pre-1990"
    df["era"] = df["year"].apply(era)
    df["is_recent"] = (df["year"] >= 2018).astype(int)

    # Vote-weighted score
    df["vote_weighted_score"] = (
        df["imdb_rating"] * np.log10(df["votes"] + 1)
    ).round(3)

    # Popularity tier
    v33 = df["votes"].quantile(0.33)
    v66 = df["votes"].quantile(0.66)
    df["popularity_tier"] = df["votes"].apply(
        lambda v: "High" if v >= v66 else ("Medium" if v >= v33 else "Low")
    )

    # Runtime category
    def rcat(r):
        if pd.isna(r):   return "Unknown"
        if r < 60:       return "Short (<1h)"
        elif r <= 90:    return "Standard (1–1.5h)"
        elif r <= 120:   return "Long (1.5–2h)"
        else:            return "Epic (2h+)"
    df["runtime_category"] = df["runtime_min"].apply(rcat)
    df["rank"] = None
    df["plot"] = ""

    return df


def load_to_mysql(titles_df: pd.DataFrame, platform_col: str = "platform"):
    """Insert merged titles and platform rows into MySQL."""
    conn = mysql.connector.connect(**DB_CONFIG)
    cur  = conn.cursor()

    title_cols = [
        "imdb_id","title","content_type","year","decade","era","is_recent",
        "imdb_rating","votes","vote_weighted_score","popularity_tier",
        "runtime_min","runtime_category","genres","genre_primary",
        "certificate","plot","rank"
    ]

    title_sql = f"""
        INSERT IGNORE INTO titles ({', '.join(f'`{c}`' for c in title_cols)})
        VALUES ({', '.join(['%s'] * len(title_cols))})
    """
    plat_sql = """
        INSERT IGNORE INTO platforms
          (imdb_id, title, content_type, platform, platform_group,
           monetization, imdb_rating, year, genre_primary, vote_weighted_score, is_recent)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    genre_sql = """
        INSERT INTO genre_map (imdb_id, title, genre, imdb_rating, year, content_type)
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    title_rows, plat_rows, genre_rows = [], [], []

    for _, row in titles_df.iterrows():
        def v(col):
            val = row.get(col)
            if not isinstance(val, str) and pd.isna(val):
               return None 
            return val

        title_rows.append(tuple(v(c) for c in title_cols))


        # Platform row
        plat = row.get("platform", "Unknown")
        if plat and plat != "Unknown":
            plat_rows.append((
                v("imdb_id"), v("title"), v("content_type"),
                plat, plat, "flatrate",
                v("imdb_rating"), v("year"), v("genre_primary"),
                v("vote_weighted_score"), v("is_recent"),
            ))

        # Genre rows
        genres_str = str(row.get("genres", ""))
        if genres_str and genres_str != "Unknown":
            for g in genres_str.split("|"):
                g = g.strip()
                if g:
                    genre_rows.append((
                        v("imdb_id"), v("title"), g,
                        v("imdb_rating"), v("year"), v("content_type"),
                    ))

    cur.executemany(title_sql, title_rows)
    log.info(f"  Inserted {cur.rowcount} title rows")

    cur.executemany(plat_sql, plat_rows)
    log.info(f"  Inserted {cur.rowcount} platform rows")

    if genre_rows:
        cur.executemany(genre_sql, genre_rows)
        log.info(f"  Inserted {cur.rowcount} genre rows")

    conn.commit()
    cur.close()
    conn.close()


def main():
    kaggle_path = os.path.join(RAW_DIR, "kaggle_netflix_titles.csv")

    if not os.path.exists(kaggle_path):
        print("\n" + "="*60)
        print("  Kaggle dataset not found.")
        print("  1. Go to: kaggle.com/datasets/victorsoeiro/netflix-tv-shows-and-movies")
        print("  2. Download titles.csv")
        print(f"  3. Save it as: {kaggle_path}")
        print("  Then re-run this script.")
        print("="*60 + "\n")
        return

    # Detect platform from filename
    platform = detect_platform(os.path.basename(kaggle_path))

    # Load and parse
    kaggle_df = load_kaggle_dataset(kaggle_path, platform_hint=platform)
    if kaggle_df.empty:
        log.error("Failed to parse Kaggle dataset.")
        return

    # Enrich with derived columns
    kaggle_df = enrich_columns(kaggle_df)

    # Load existing scraped titles to avoid duplicates
    existing_path = os.path.join(CLEANED_DIR, "titles_clean.csv")
    if os.path.exists(existing_path):
        existing = pd.read_csv(existing_path)
        existing_titles = set(existing["title"].str.lower().str.strip())
        before = len(kaggle_df)
        kaggle_df = kaggle_df[~kaggle_df["title"].str.lower().str.strip().isin(existing_titles)]
        log.info(f"Removed {before - len(kaggle_df)} duplicates already in your dataset.")

    log.info(f"Final merge: adding {len(kaggle_df)} new titles to MySQL.")

    # Save merged CSV
    all_df = pd.concat([
        pd.read_csv(existing_path) if os.path.exists(existing_path) else pd.DataFrame(),
        kaggle_df
    ], ignore_index=True)
    all_df.to_csv(os.path.join(CLEANED_DIR, "titles_clean.csv"), index=False)
    log.info(f"Saved merged titles_clean.csv: {len(all_df)} total rows.")

    # Load to MySQL
    load_to_mysql(kaggle_df)

    print("\n" + "="*60)
    print(f"  DONE — {len(all_df)} total titles now in ott_pulse database")
    print(f"  Your scraped IMDb top 50: still there")
    print(f"  Kaggle titles added:      {len(kaggle_df)}")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
