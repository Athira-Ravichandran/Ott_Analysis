

import pandas as pd
import numpy as np
import os
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

RAW_DIR     = os.path.join(os.path.dirname(__file__), "data", "raw")
CLEANED_DIR = os.path.join(os.path.dirname(__file__), "data", "cleaned")
os.makedirs(CLEANED_DIR, exist_ok=True)


# ─── 1. LOAD ────────────────────────────────────────────────────────────────

def load_raw_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    movies_path   = os.path.join(RAW_DIR, "imdb_movies_raw.csv")
    shows_path    = os.path.join(RAW_DIR, "imdb_shows_raw.csv")
    platform_path = os.path.join(RAW_DIR, "justwatch_platforms_raw.csv")

    movies_df   = pd.read_csv(movies_path)   if os.path.exists(movies_path)   else pd.DataFrame()
    shows_df    = pd.read_csv(shows_path)    if os.path.exists(shows_path)    else pd.DataFrame()
    platform_df = pd.read_csv(platform_path) if os.path.exists(platform_path) else pd.DataFrame()

    log.info(f"Loaded: {len(movies_df)} movies, {len(shows_df)} TV shows, {len(platform_df)} platform rows")
    return movies_df, shows_df, platform_df


# ─── 2. CLEAN TITLES ─────────────────────────────────────────────────────────

def clean_titles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and enriches the titles dataframe (movies + shows combined).
    """
    log.info("Cleaning titles dataframe...")

    # ── 2a. Drop exact duplicates
    before = len(df)
    df = df.drop_duplicates(subset=["title", "year", "content_type"])
    log.info(f"  Removed {before - len(df)} duplicate rows.")

    # ── 2b. Handle missing values
    df["imdb_rating"]  = pd.to_numeric(df["imdb_rating"], errors="coerce")
    df["votes"]        = pd.to_numeric(df["votes"],       errors="coerce").fillna(0).astype(int)
    df["runtime_min"]  = pd.to_numeric(df["runtime_min"], errors="coerce")
    df["year"]         = pd.to_numeric(df["year"],        errors="coerce")

    # Fill missing ratings with column median
    median_rating = df["imdb_rating"].median()
    df["imdb_rating"] = df["imdb_rating"].fillna(median_rating)

    # Fill missing runtime with median per content_type
    df["runtime_min"] = df.groupby("content_type")["runtime_min"].transform(
        lambda x: x.fillna(x.median())
    )

    # Clean certificate — strip whitespace, uppercase, map variants
    cert_map = {
        "PG-13": "PG-13", "PG": "PG", "G": "G", "R": "R", "NC-17": "NC-17",
        "U": "U", "UA": "UA", "A": "A", "U/A": "UA",
        "TV-MA": "TV-MA", "TV-14": "TV-14", "TV-PG": "TV-PG", "TV-G": "TV-G",
        "NOT RATED": "Unrated", "NR": "Unrated", "UNRATED": "Unrated",
    }
    df["certificate"] = (
        df["certificate"]
        .fillna("Unknown")
        .str.strip()
        .str.upper()
        .map(lambda x: cert_map.get(x, x))
    )

    # ── 2c. Derived columns

    # Decade bucket
    df["decade"] = ((df["year"] // 10) * 10).astype("Int64").astype(str) + "s"
    df.loc[df["year"].isna(), "decade"] = "Unknown"

    # Era label
    def era(year):
        if pd.isna(year):       return "Unknown"
        if year >= 2020:        return "2020s"
        elif year >= 2015:      return "2015–2019"
        elif year >= 2010:      return "2010–2014"
        elif year >= 2000:      return "2000s"
        elif year >= 1990:      return "1990s"
        else:                   return "Pre-1990"
    df["era"] = df["year"].apply(era)

    # Is recent flag
    df["is_recent"] = (df["year"] >= 2018).astype(int)

    # Vote-weighted score — rewards popular AND well-rated titles
    # Formula: rating × log10(votes + 1)   (+ 1 to handle zero votes)
    df["vote_weighted_score"] = (
        df["imdb_rating"] * np.log10(df["votes"] + 1)
    ).round(3)

    # Popularity tier based on votes
    vote_33 = df["votes"].quantile(0.33)
    vote_66 = df["votes"].quantile(0.66)
    def vote_tier(v):
        if v >= vote_66: return "High"
        elif v >= vote_33: return "Medium"
        else: return "Low"
    df["popularity_tier"] = df["votes"].apply(vote_tier)

    # Runtime category
    def runtime_cat(r):
        if pd.isna(r):    return "Unknown"
        if r < 60:        return "Short (<1h)"
        elif r <= 90:     return "Standard (1–1.5h)"
        elif r <= 120:    return "Long (1.5–2h)"
        else:             return "Epic (2h+)"
    df["runtime_category"] = df["runtime_min"].apply(runtime_cat)

    # ── 2d. Genre explosion
    # Genres stored as "Action|Drama|Thriller" — we keep the pipe-separated column
    # AND create a genre_primary column (first listed genre)
    df["genre_primary"] = df["genres"].fillna("Unknown").apply(
        lambda x: x.split("|")[0].strip() if pd.notna(x) and x else "Unknown"
    )

    # ── 2e. Clean text fields
    df["title"] = df["title"].str.strip()
    df["plot"]  = df["plot"].fillna("").str.strip()

    # ── 2f. Final column selection and ordering
    col_order = [
        "imdb_id", "title", "content_type", "year", "decade", "era", "is_recent",
        "imdb_rating", "votes", "vote_weighted_score", "popularity_tier",
        "runtime_min", "runtime_category", "genres", "genre_primary",
        "certificate", "plot", "rank",
    ]
    existing_cols = [c for c in col_order if c in df.columns]
    df = df[existing_cols]

    log.info(f"Clean titles shape: {df.shape}")
    return df


# ─── 3. CLEAN PLATFORMS ──────────────────────────────────────────────────────

def clean_platforms(platform_df: pd.DataFrame, titles_df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans platform mapping table and adds IMDb metadata for analysis.
    """
    if platform_df.empty:
        log.warning("Platform data is empty — skipping platform cleaning.")
        return platform_df

    log.info("Cleaning platform dataframe...")

    # Standardize platform names
    platform_df["platform"] = platform_df["platform"].str.strip()

    # Drop rows where platform is truly unknown
    platform_df = platform_df[platform_df["platform"].notna()]

    # Keep only major streaming platforms for analysis
    target_platforms = [
        "Netflix", "Amazon Prime Video", "Disney+ Hotstar",
        "Apple TV+", "Hotstar", "Not Available"
    ]
    platform_df["platform_group"] = platform_df["platform"].apply(
        lambda x: x if x in target_platforms else "Other"
    )

    # Merge with cleaned titles to get rating/year for platform analysis
    merge_cols = ["imdb_id", "title", "imdb_rating", "year", "genre_primary",
                  "vote_weighted_score", "content_type", "is_recent"]
    available = [c for c in merge_cols if c in titles_df.columns]

    if "imdb_id" in platform_df.columns and "imdb_id" in titles_df.columns:
        platform_df = platform_df.merge(
            titles_df[available],
            on="imdb_id",
            how="left",
            suffixes=("", "_titles")
        )
        # Prefer title from platform_df if both exist
        if "title_titles" in platform_df.columns:
            platform_df["title"] = platform_df["title"].fillna(platform_df["title_titles"])
            platform_df.drop(columns=["title_titles"], inplace=True, errors="ignore")
    else:
        # Fallback: merge on title string
        platform_df = platform_df.merge(
            titles_df[available],
            on="title",
            how="left",
        )

    log.info(f"Clean platform shape: {platform_df.shape}")
    return platform_df


# ─── 4. BUILD GENRE EXPLODED TABLE ───────────────────────────────────────────

def build_genre_table(titles_df: pd.DataFrame) -> pd.DataFrame:
    """
    Explodes the pipe-separated genres column into one row per (title, genre).
    Useful for genre-level analysis in SQL and Power BI.
    """
    log.info("Building genre exploded table...")

    genre_rows = []
    for _, row in titles_df.iterrows():
        genres_raw = row.get("genres", "")
        if pd.isna(genres_raw) or not genres_raw:
            genre_rows.append({
                "imdb_id":      row.get("imdb_id", ""),
                "title":        row["title"],
                "genre":        "Unknown",
                "imdb_rating":  row.get("imdb_rating"),
                "year":         row.get("year"),
                "content_type": row.get("content_type"),
            })
            continue

        for genre in str(genres_raw).split("|"):
            genre = genre.strip()
            if genre:
                genre_rows.append({
                    "imdb_id":      row.get("imdb_id", ""),
                    "title":        row["title"],
                    "genre":        genre,
                    "imdb_rating":  row.get("imdb_rating"),
                    "year":         row.get("year"),
                    "content_type": row.get("content_type"),
                })

    df = pd.DataFrame(genre_rows)
    log.info(f"Genre exploded table: {len(df)} rows, {df['genre'].nunique()} unique genres")
    return df


# ─── 5. SUMMARY STATS ────────────────────────────────────────────────────────

def print_data_quality_report(titles_df: pd.DataFrame, platform_df: pd.DataFrame):
    """Print a quick data quality summary for the README / notebook."""
    print("\n" + "="*60)
    print("  DATA QUALITY REPORT")
    print("="*60)
    print(f"  Total titles:           {len(titles_df)}")
    print(f"  Movies:                 {(titles_df['content_type']=='movie').sum()}")
    print(f"  TV Shows:               {(titles_df['content_type']=='tv_show').sum()}")
    print(f"  Missing ratings:        {titles_df['imdb_rating'].isna().sum()}")
    print(f"  Missing runtime:        {titles_df['runtime_min'].isna().sum()}")
    print(f"  Missing genres:         {titles_df['genres'].isna().sum()}")
    print(f"  Year range:             {int(titles_df['year'].min())} – {int(titles_df['year'].max())}")
    print(f"  Avg IMDb rating:        {titles_df['imdb_rating'].mean():.2f}")
    print(f"  Platform rows:          {len(platform_df)}")
    if not platform_df.empty and "platform_group" in platform_df.columns:
        print(f"\n  Platform distribution:")
        for plat, cnt in platform_df["platform_group"].value_counts().items():
            print(f"    {plat:<25} {cnt}")
    print("="*60 + "\n")


# ─── 6. SAVE ─────────────────────────────────────────────────────────────────

def save_cleaned_data(titles_df, platform_df, genre_df):
    titles_path   = os.path.join(CLEANED_DIR, "titles_clean.csv")
    platform_path = os.path.join(CLEANED_DIR, "platform_clean.csv")
    genre_path    = os.path.join(CLEANED_DIR, "genre_exploded.csv")

    titles_df.to_csv(titles_path,   index=False)
    log.info(f"Saved: {titles_path}")

    if not platform_df.empty:
        platform_df.to_csv(platform_path, index=False)
        log.info(f"Saved: {platform_path}")

    genre_df.to_csv(genre_path, index=False)
    log.info(f"Saved: {genre_path}")


# ─── MAIN ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    movies_df, shows_df, platform_df = load_raw_data()

    # Combine movies and shows
    all_titles = pd.concat([movies_df, shows_df], ignore_index=True)

    # Clean
    titles_clean   = clean_titles(all_titles)
    platform_clean = clean_platforms(platform_df, titles_clean)
    genre_exploded = build_genre_table(titles_clean)

    # Report
    print_data_quality_report(titles_clean, platform_clean)

    # Save
    save_cleaned_data(titles_clean, platform_clean, genre_exploded)
    log.info("Pipeline complete. Cleaned data ready in data/cleaned/")
